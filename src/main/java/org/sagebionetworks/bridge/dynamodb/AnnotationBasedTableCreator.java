package org.sagebionetworks.bridge.dynamodb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
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
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
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
            final ProvisionedThroughput throughput = getThroughput(clazz);
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
                    String indexName = indexKey.globalSecondaryIndexName();
                    String rangeAttrName = findIndexRangeAttrName(clazz, true, indexName);
                    GlobalSecondaryIndexDescription descr = createGlobalIndexDescr(
                            indexName, attrName, rangeAttrName, throughput);
                    addProjectionIfAnnotated(method, indexName, descr);
                    globalIndices.add(descr);
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
                        // String hashAttrName = findIndexHashAttrName(clazz, indexName);
                        LocalSecondaryIndexDescription descr = createLocalIndexDescr(indexName,
                                        hashKey.getAttributeName(), attrName);
                        localIndices.add(descr);
                    }
                    // If this is a range key but has no index, it hasn't been added yet, and needs to be added now.
                    // So here we search for the accompanying hash annotation and if it exists, we skip adding this.
                    indexName = indexKey.globalSecondaryIndexName();
                    if (isNotBlank(indexName)) {
                        String hashAttrName = findIndexHashAttrName(clazz, indexName);
                        if (hashAttrName == null) {
                            GlobalSecondaryIndexDescription descr = createGlobalIndexDescr(
                                    indexName, null, attrName, throughput);
                            addProjectionIfAnnotated(method, indexName, descr);
                            globalIndices.add(descr);
                        }
                    }
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
                .withLocalSecondaryIndexes(localIndices)
                .withProvisionedThroughput(
                    new ProvisionedThroughputDescription()
                        .withReadCapacityUnits(throughput.getReadCapacityUnits())
                        .withWriteCapacityUnits(throughput.getWriteCapacityUnits()));
            tables.put(tableName, table);
        }
        return new ArrayList<>(tables.values());
    }

    private ProvisionedThroughput getThroughput(final Class<?> clazz) {
        if (clazz.isAnnotationPresent(DynamoThroughput.class)) {
            DynamoThroughput throughput = clazz.getAnnotation(DynamoThroughput.class);
            return new ProvisionedThroughput(throughput.readCapacity(), throughput.writeCapacity());
        }
        return new ProvisionedThroughput(DEFAULT_READ_CAPACITY, DEFAULT_WRITE_CAPACITY);
    }

    /**
     * If the method is also annotated with a projection annotation, use the projection it indicates. For hash/range
     * global indices, this annotation needs to be on the same method as @DynamoDBIndexHashKey. This is a
     * Bridge-specific annotation, it's not in the AWS SDK.
     * 
     * @param method
     * @param indexName
     * @param descr
     */
    private void addProjectionIfAnnotated(Method method, String indexName, GlobalSecondaryIndexDescription descr) {
        DynamoProjection projection = method.getAnnotation(DynamoProjection.class);
        if (projection != null && indexName.equals(projection.globalSecondaryIndexName())) {
            descr.setProjection(new Projection().withProjectionType(projection.projectionType()));
        }
    }

    private GlobalSecondaryIndexDescription createGlobalIndexDescr(String indexName, String hashAttrName,
                    String rangeAttrName, ProvisionedThroughput throughput) {
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
            .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
            .withProvisionedThroughput(
                new ProvisionedThroughputDescription()
                    .withWriteCapacityUnits(throughput.getWriteCapacityUnits())
                    .withReadCapacityUnits(throughput.getReadCapacityUnits()));
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

    private String findIndexHashAttrName(final Class<?> clazz, final String indexName) {
        if (isBlank(indexName)) {
            return null;
        }
        return findAttrName(clazz, new Function<Method, String>() {
            public String apply(Method method) {
                if (method.isAnnotationPresent(DynamoDBIndexHashKey.class)) {
                    DynamoDBIndexHashKey indexKey = method.getAnnotation(DynamoDBIndexHashKey.class);
                    String thisIndexName = indexKey.globalSecondaryIndexName();
                    if (indexName.equals(thisIndexName)) {
                        return indexKey.attributeName();
                    }
                }
                return null;
            }
        });
    }

    private String findIndexRangeAttrName(final Class<?> clazz, final boolean isGlobal, final String indexName) {
        if (isBlank(indexName)) {
            return null;
        }
        return findAttrName(clazz, new Function<Method, String>() {
            public String apply(Method method) {
                if (method.isAnnotationPresent(DynamoDBIndexRangeKey.class)) {
                    DynamoDBIndexRangeKey indexKey = method.getAnnotation(DynamoDBIndexRangeKey.class);
                    String thisIndexName = isGlobal ? indexKey.globalSecondaryIndexName() : indexKey
                                    .localSecondaryIndexName();
                    if (indexName.equals(thisIndexName)) {
                        return indexKey.attributeName();
                    }
                }
                return null;
            }
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
