package org.sagebionetworks.bridge.dynamodb;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

public class DynamoUtils {

    private static final String TABLE_NAME_DELIMITER = "-";

    private final DynamoNamingHelper namingHelper;
    private final AmazonDynamoDB dynamoClient;

    public DynamoUtils(DynamoNamingHelper namingHelper, AmazonDynamoDB dynamoClient) {
        checkNotNull(namingHelper);
        checkNotNull(dynamoClient);

        this.namingHelper = namingHelper;
        this.dynamoClient = dynamoClient;
    }

    /**
     * Gets the mapper with UPDATE behavior for saves and CONSISTENT reads.
     */
    public DynamoDBMapper getMapper(final Class<?> dynamoTable) {
        checkNotNull(dynamoTable);

        final DynamoDBMapperConfig mapperConfig = new DynamoDBMapperConfig.Builder()
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE)
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .withTableNameOverride(namingHelper.getTableNameOverride(dynamoTable))
                .build();
        return new DynamoDBMapper(dynamoClient, mapperConfig);
    }

    /**
     * Gets the mapper with UPDATE behavior for saves and EVENTUALLY consistent reads.
     */
    public DynamoDBMapper getMapperEventually(final Class<?> dynamoTable) {
        checkNotNull(dynamoTable);

        final DynamoDBMapperConfig mapperConfig = new DynamoDBMapperConfig.Builder()
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE)
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.EVENTUAL)
                .withTableNameOverride(namingHelper.getTableNameOverride(dynamoTable))
                .build();
        return new DynamoDBMapper(dynamoClient, mapperConfig);
    }

    public CreateTableRequest getCreateTableRequest(final TableDescription table) {
        checkNotNull(table);

        final CreateTableRequest request = new CreateTableRequest()
                .withTableName(table.getTableName())
                .withKeySchema(table.getKeySchema())
                .withAttributeDefinitions(table.getAttributeDefinitions());

        // ProvisionedThroughputDescription -> ProvisionedThroughput
        final ProvisionedThroughput throughput = new ProvisionedThroughput(
                table.getProvisionedThroughput().getReadCapacityUnits(),
                table.getProvisionedThroughput().getWriteCapacityUnits()
        );
        request.setProvisionedThroughput(throughput);

        // GlobalSecondaryIndexDescription -> GlobalSecondaryIndex
        final List<GlobalSecondaryIndex> globalIndices = new ArrayList<>();
        for (GlobalSecondaryIndexDescription globalIndexDesc : table.getGlobalSecondaryIndexes()) {
            final GlobalSecondaryIndex globalIndex = new GlobalSecondaryIndex()
                    .withIndexName(globalIndexDesc.getIndexName())
                    .withKeySchema(globalIndexDesc.getKeySchema())
                    .withProjection(globalIndexDesc.getProjection())
                    .withProvisionedThroughput(new ProvisionedThroughput(
                            globalIndexDesc.getProvisionedThroughput().getReadCapacityUnits(),
                            globalIndexDesc.getProvisionedThroughput().getWriteCapacityUnits()
                    ));
            globalIndices.add(globalIndex);
        }
        if (globalIndices.size() > 0) {
            request.setGlobalSecondaryIndexes(globalIndices);
        }

        // LocalSecondaryIndexDescription -> LocalSecondaryIndex
        final List<LocalSecondaryIndex> localIndices = new ArrayList<>();
        for (LocalSecondaryIndexDescription localIndexDesc : table.getLocalSecondaryIndexes()) {
            final LocalSecondaryIndex localIndex = new LocalSecondaryIndex()
                    .withIndexName(localIndexDesc.getIndexName())
                    .withKeySchema(localIndexDesc.getKeySchema())
                    .withProjection(localIndexDesc.getProjection());
            localIndices.add(localIndex);
        }
        if (localIndices.size() > 0) {
            request.setLocalSecondaryIndexes(localIndices);
        }

        return request;
    }

    /**
     * Compares if the two tables are of the same name. Also compares hash key, range key of the two tables.
     * Throws an exception if there is difference.
     */
    public void compareSchema(final TableDescription table1, final TableDescription table2) {
        if (table1.getTableName().equals(table2.getTableName())) {
            compareKeySchema(table1, table2);
        }
    }

    /**
     * Compares hash key, range key of the two tables. Throws an exception if there is difference.
     */
    public void compareKeySchema(final TableDescription table1, final TableDescription table2) {
        List<KeySchemaElement> keySchema1 = table1.getKeySchema();
        List<KeySchemaElement> keySchema2 = table2.getKeySchema();
        compareKeySchema(keySchema1, keySchema2);
    }

    private void compareKeySchema(final List<KeySchemaElement> keySchema1,
                                         final List<KeySchemaElement> keySchema2) {
        if (keySchema1.size() != keySchema2.size()) {
            throw new KeySchemaMismatchException("Key schemas have different number of key elements.");
        }
        final Map<String, KeySchemaElement> keySchemaMap1 = new HashMap<>();
        for (KeySchemaElement ele1 : keySchema1) {
            keySchemaMap1.put(ele1.getAttributeName(), ele1);
        }
        for (KeySchemaElement ele2 : keySchema2) {
            KeySchemaElement ele1 = keySchemaMap1.get(ele2.getAttributeName());
            if (ele1 == null) {
                throw new KeySchemaMismatchException("Missing key " + ele2.getAttributeName() + " in schema 1.");
            }
            if (!ele1.equals(ele2)) {
                throw new KeySchemaMismatchException("Different key schema for key " + ele2.getAttributeName());
            }
        }
    }

    // TODO: Add timeout
    public void waitForActive(final String tableName) {
        checkNotNull(tableName);
        final DescribeTableRequest request = new DescribeTableRequest(tableName);
        TableDescription table = dynamoClient.describeTable(request).getTable();
        while (!TableStatus.ACTIVE.name().equalsIgnoreCase(table.getTableStatus())) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                throw new RuntimeException("Shouldn't be interrupted.", e);
            }
            table = dynamoClient.describeTable(request).getTable();
        }
    }

    // TODO: Add timeout
    public void waitForDelete(final String tableName) {
        checkNotNull(tableName);
        final DescribeTableRequest request = new DescribeTableRequest(tableName);
        while (true) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                throw new RuntimeException("Shouldn't be interrupted.", e);
            }
            try {
                dynamoClient.describeTable(request);
            } catch (ResourceNotFoundException e) {
                return;
            }
        }
    }

    /**
     * Gets all the tables, keyed by the fully qualified table name.
     */
    public Map<String, TableDescription> getAllExistingTables() {
        final Map<String, TableDescription> existingTables = new HashMap<>();
        String lastTableName = null;
        ListTablesResult listTablesResult = dynamoClient.listTables();
        do {
            for (final String tableName : listTablesResult.getTableNames()) {
                DescribeTableResult describeResult = dynamoClient.describeTable(tableName);
                TableDescription table = describeResult.getTable();
                existingTables.put(tableName, table);
            }
            lastTableName = listTablesResult.getLastEvaluatedTableName();
            if (lastTableName != null) {
                listTablesResult = dynamoClient.listTables(lastTableName);
            }
        } while (lastTableName != null);
        return existingTables;
    }

    /**
     * Gets the tables, keyed by the fully qualified table name, for the current environment and user.
     */
    public Map<String, TableDescription> getExistingTables() {
        final Map<String, TableDescription> tables = getAllExistingTables();
        final String prefix = namingHelper.getTableNamePrefix();
        final Map<String, TableDescription> filteredTables = new HashMap<>();
        for (final Map.Entry<String, TableDescription> table : tables.entrySet()) {
            if (table.getKey().startsWith(prefix)) {
                filteredTables.put(table.getKey(), table.getValue());
            }
        }
        return filteredTables;
    }
}
