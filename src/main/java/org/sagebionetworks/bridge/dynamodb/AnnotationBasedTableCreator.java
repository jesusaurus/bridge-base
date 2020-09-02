package org.sagebionetworks.bridge.dynamodb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import org.joda.time.LocalDate;

/**
 * Scans a package for types that are annotated as DynamoDBTable and maps
 * them to TableDescription objects.
 */
public class AnnotationBasedTableCreator {

    public static final long DEFAULT_READ_CAPACITY = 10;
    public static final long DEFAULT_WRITE_CAPACITY = 10;

    private final DynamoNamingHelper namingHelper;

    /**
     * @param namingHelper  helper for naming dynamo tables
     */
    public AnnotationBasedTableCreator(DynamoNamingHelper namingHelper) {
        this.namingHelper = namingHelper;
    }

    /**
     * @param tablePackage The package to scan for DynamoDBTable annotated types.
     */
    public List<TableDescription> getTables(final String tablePackage) {
        checkNotNull(tablePackage);
        return getAnnotatedTables(loadDynamoTableClasses(tablePackage));
    }

    /**
     * @param tables The list of DynamoDBTable annotated types.
     */
    public List<TableDescription> getTables(final Class<?>... tables) {
        checkNotNull(tables);
        return getAnnotatedTables(Arrays.asList(tables));
    }

    /**
     * Uses reflection to get all the annotated DynamoDBTable.
     */
    List<Class<?>> loadDynamoTableClasses(final String tablePackage) {
        final List<Class<?>> classes = new ArrayList<>();
        final ClassLoader classLoader = getClass().getClassLoader();
        try {
            final ImmutableSet<ClassInfo> classSet = ClassPath.from(classLoader).getTopLevelClasses(tablePackage);
            for (ClassInfo classInfo : classSet) {
                Class<?> clazz = classInfo.load();
                if (clazz.isAnnotationPresent(DynamoDBTable.class)) {
                    classes.add(clazz);
                }
            }
            return classes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<TableDescription> getAnnotatedTables(final List<Class<?>> classes) {
        final Map<String, TableDescription> tables = new HashMap<>();
        for (final Class<?> clazz : classes) {
            final List<KeySchemaElement> keySchema = new ArrayList<>();
            final Map<String, AttributeDefinition> attributes = new HashMap<>();
            final List<GlobalSecondaryIndexDescription> globalIndices = new ArrayList<>();
            final List<LocalSecondaryIndexDescription> localIndices = new ArrayList<>();
            Method[] methods = clazz.getMethods();
            KeySchemaElement hashKey = null;
            for (Method method : methods) {
                if (method.isAnnotationPresent(DynamoDBHashKey.class)) {
                    // Hash key
                    DynamoDBHashKey hashKeyAttr = method.getAnnotation(DynamoDBHashKey.class);
                    String attrName = hashKeyAttr.attributeName();
                    if (attrName == null || attrName.isEmpty()) {
                        attrName = getAttributeName(method);
                    }
                    hashKey = new KeySchemaElement(attrName, KeyType.HASH);
                    keySchema.add(0, hashKey);
                    ScalarAttributeType attrType = getAttributeType(method);
                    AttributeDefinition attribute = new AttributeDefinition(attrName, attrType);
                    attributes.put(attrName, attribute);
                } else if (method.isAnnotationPresent(DynamoDBRangeKey.class)) {
                    // Range key
                    DynamoDBRangeKey rangeKeyAttr = method.getAnnotation(DynamoDBRangeKey.class);
                    String attrName = rangeKeyAttr.attributeName();
                    if (attrName == null || attrName.isEmpty()) {
                        attrName = getAttributeName(method);
                    }
                    KeySchemaElement rangeKey = new KeySchemaElement(attrName, KeyType.RANGE);
                    keySchema.add(rangeKey);
                    ScalarAttributeType attrType = getAttributeType(method);
                    AttributeDefinition attribute = new AttributeDefinition(attrName, attrType);
                    attributes.put(attrName, attribute);
                }
            }
            if (hashKey == null) {
                throw new RuntimeException("Missing hash key for DynamoDBTable " + clazz);
            }
            // This supports local indices, and global indices with a hash only or a hash and range.
            for (Method method : methods) {
                String attrName = null;
                if (method.isAnnotationPresent(DynamoDBIndexHashKey.class)) {
                    // There is no localSecondaryIndexName attribute, so this is by definition a global index
                    // with a hash and range key. Find the range annotation to complete this description
                    DynamoDBIndexHashKey indexKey = method.getAnnotation(DynamoDBIndexHashKey.class);
                    attrName = indexKey.attributeName();
                    if (isBlank(attrName)) {
                        attrName = getAttributeName(method);
                    }

                    // Index name can be in globalSecondaryIndexName() or globalSecondaryIndexNames(). (Note the plural.)
                    // Add them all to a set, then loop over the set to build all indices.
                    Set<String> indexNameSet = new HashSet<>();
                    if (isNotBlank(indexKey.globalSecondaryIndexName())) {
                        indexNameSet.add(indexKey.globalSecondaryIndexName());
                    }
                    if (indexKey.globalSecondaryIndexNames() != null) {
                        for (String oneIndexName : indexKey.globalSecondaryIndexNames()) {
                            if (isNotBlank(oneIndexName)) {
                                indexNameSet.add(oneIndexName);
                            }
                        }
                    }

                    for (String oneIndexName : indexNameSet) {
                        String rangeAttrName = findGlobalIndexRangeAttrName(clazz, oneIndexName);
                        GlobalSecondaryIndexDescription descr = createGlobalIndexDescr(
                                oneIndexName, attrName, rangeAttrName);
                        addProjectionIfAnnotated(method, oneIndexName, descr);
                        globalIndices.add(descr);
                    }
                } else if (method.isAnnotationPresent(DynamoDBIndexRangeKey.class)) {
                    DynamoDBIndexRangeKey indexKey = method.getAnnotation(DynamoDBIndexRangeKey.class);
                    attrName = indexKey.attributeName();
                    if (isBlank(attrName)) {
                        attrName = getAttributeName(method);
                    }
                    // Local index, range only, there are none of these in our code base (except for tests)
                    String indexName = indexKey.localSecondaryIndexName();
                    if (isNotBlank(indexName)) {
                        // For a local index, the hash key is always the same hash key, it's only the range key tha
                        // varies.
                        LocalSecondaryIndexDescription descr = createLocalIndexDescr(indexName,
                                        hashKey.getAttributeName(), attrName);
                        localIndices.add(descr);
                    }

                    // Global indexes are already handled in the branch above. No need to handle them here.
                    // Note that in previous versions of this class, we supported global indices where the hash key is
                    // the same as the table hash key. This feature is not in use, and doesn't really make sense, since
                    // you should just use a secondary index.
                }
                if (attrName != null) {
                    ScalarAttributeType attrType = getAttributeType(method);
                    AttributeDefinition attribute = new AttributeDefinition(attrName, attrType);
                    attributes.put(attrName, attribute);
                }
            }
            final String tableName = namingHelper.getFullyQualifiedTableName(clazz);
            // Create the table description
            final TableDescription table = new TableDescription()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributes.values())
                .withGlobalSecondaryIndexes(globalIndices)
                .withLocalSecondaryIndexes(localIndices);
            tables.put(tableName, table);
        }
        return new ArrayList<>(tables.values());
    }

    /**
     * If the method is also annotated with a projection annotation, use the projection it indicates. For hash/range
     * global indices, this annotation needs to be on the same method as @DynamoDBIndexHashKey. This is a
     * Bridge-specific annotation, it's not in the AWS SDK.
     */
    private void addProjectionIfAnnotated(Method method, String indexName, GlobalSecondaryIndexDescription descr) {
        DynamoProjection projection = method.getAnnotation(DynamoProjection.class);
        if (projection != null && indexName.equals(projection.globalSecondaryIndexName())) {
            descr.setProjection(new Projection().withProjectionType(projection.projectionType()));
        }
    }

    private GlobalSecondaryIndexDescription createGlobalIndexDescr(String indexName, String hashAttrName,
                    String rangeAttrName) {
        checkArgument(isNotBlank(indexName));

        List<KeySchemaElement> keys = Lists.newArrayList();
        if (hashAttrName != null) {
            keys.add(new KeySchemaElement(hashAttrName, KeyType.HASH));
        }
        if (rangeAttrName != null) {
            keys.add(new KeySchemaElement(rangeAttrName, KeyType.RANGE));
        }
        GlobalSecondaryIndexDescription globalIndex = new GlobalSecondaryIndexDescription()
            .withIndexName(indexName)
            .withKeySchema(keys)
            .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY));
        return globalIndex;
    }

    private LocalSecondaryIndexDescription createLocalIndexDescr(final String indexName, final String hashAttrName,
                    final String rangeAttrName) {
        final List<KeySchemaElement> keys = Lists.newArrayList();
        if (hashAttrName != null) {
            keys.add(new KeySchemaElement(hashAttrName, KeyType.HASH));
        }
        if (rangeAttrName != null) {
            keys.add(new KeySchemaElement(rangeAttrName, KeyType.RANGE));
        }
        final LocalSecondaryIndexDescription localIndex = new LocalSecondaryIndexDescription().withIndexName(indexName)
                        .withKeySchema(keys).withProjection(new Projection().withProjectionType(ProjectionType.ALL));
        return localIndex;
    }

    private String findAttrName(final Class<?> clazz, final Function<Method, String> func) {
        final Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            final String attrName = func.apply(method);
            if (attrName != null) {
                return attrName;
            }
        }
        return null;
    }

    private String findGlobalIndexRangeAttrName(final Class<?> clazz, final String indexName) {
        if (isBlank(indexName)) {
            return null;
        }
        return findAttrName(clazz, method -> {
            if (method.isAnnotationPresent(DynamoDBIndexRangeKey.class)) {
                DynamoDBIndexRangeKey indexRangeKey = method.getAnnotation(DynamoDBIndexRangeKey.class);

                // Index name can be in globalSecondaryIndexName() or globalSecondaryIndexNames(). (Note the plural.)
                // Add them all to a set, then check the set contains the name we're looking for.
                Set<String> rangeKeyIndexNameSet = new HashSet<>();
                if (isNotBlank(indexRangeKey.globalSecondaryIndexName())) {
                    rangeKeyIndexNameSet.add(indexRangeKey.globalSecondaryIndexName());
                }
                if (indexRangeKey.globalSecondaryIndexNames() != null) {
                    for (String oneIndexName : indexRangeKey.globalSecondaryIndexNames()) {
                        if (isNotBlank(oneIndexName)) {
                            rangeKeyIndexNameSet.add(oneIndexName);
                        }
                    }
                }

                if (rangeKeyIndexNameSet.contains(indexName)) {
                    return indexRangeKey.attributeName();
                }
            }
            return null;
        });
    }

    String getAttributeName(final Method method) {
        String attrName = method.getName();
        if (attrName.startsWith("get")) {
            attrName = attrName.substring("get".length());
            if (attrName.length() > 0) {
                // Make sure the first letter is lower case
                char[] chars = attrName.toCharArray();
                chars[0] = Character.toLowerCase(chars[0]);
                attrName = new String(chars);
            }
        }
        return attrName;
    }

    ScalarAttributeType getAttributeType(final Method method) {
        final Class<?> returnType = method.getReturnType();
        if (returnType == String.class ||
                returnType == JsonNode.class ||
                returnType == LocalDate.class) {
            return ScalarAttributeType.S;
        } else if (returnType == Long.class ||
                returnType == long.class ||
                returnType == Integer.class ||
                returnType == int.class) {
            return ScalarAttributeType.N;
        } else if (returnType == Boolean.class ||
                returnType == boolean.class) {
            return ScalarAttributeType.B;
        }
        throw new RuntimeException("Unsupported return type " + returnType
                + " of method " + method.getName());
    }
}
