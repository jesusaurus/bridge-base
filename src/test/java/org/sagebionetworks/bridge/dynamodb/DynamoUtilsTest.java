package org.sagebionetworks.bridge.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;
import org.sagebionetworks.bridge.dynamodb.test.TestHealthDataRecord;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class DynamoUtilsTest {

    @Test
    public void testTableName() {
        final Config config = mock(Config.class);
        when(config.getUser()).thenReturn("testTableName");
        when(config.getEnvironment()).thenReturn(Environment.LOCAL);
        final String fqTableName = DynamoUtils.getFullyQualifiedTableName(TestHealthDataRecord.class, config);
        assertEquals("local-testTableName-TestHealthDataRecord", fqTableName);
        final String simpleTableName = DynamoUtils.getSimpleTableName(fqTableName, config);
        assertEquals("TestHealthDataRecord", simpleTableName);
        assertEquals(fqTableName, DynamoUtils.getFullyQualifiedTableName(simpleTableName, config));
    }

    @Test
    public void testGetCreateTableRequest() {

        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables();
        TableDescription table = DynamoTestUtils.getTableByName(tables, "TestHealthDataRecord");
        CreateTableRequest request = DynamoUtils.getCreateTableRequest(table);
        assertNotNull(request);

        // KeySchema
        List<KeySchemaElement> keySchema = request.getKeySchema();
        assertNotNull(keySchema);
        assertEquals(2, keySchema.size());
        Map<String, KeySchemaElement> keyElements = new HashMap<String, KeySchemaElement>();
        for (KeySchemaElement ele : keySchema) {
            keyElements.put(ele.getAttributeName(), ele);
        }
        assertEquals("HASH", keyElements.get("key").getKeyType());
        assertEquals("RANGE", keyElements.get("recordId").getKeyType());
        assertEquals("HASH", keySchema.get(0).getKeyType()); // The first key must be the hashkey

        // Local indices
        List<LocalSecondaryIndex> localIndices = request.getLocalSecondaryIndexes();
        assertNotNull(localIndices);
        assertEquals(2, localIndices.size());
        Map<String, LocalSecondaryIndex> localIndexMap = new HashMap<String, LocalSecondaryIndex>();
        for (LocalSecondaryIndex idx : localIndices) {
            localIndexMap.put(idx.getIndexName(), idx);
        }
        assertNotNull(localIndexMap.get("startDate-index"));
        assertNotNull(localIndexMap.get("endDate-index"));

        // global indices
        List<GlobalSecondaryIndex> globalIndexList = request.getGlobalSecondaryIndexes();
        assertEquals(1, globalIndexList.size());
        assertEquals("secondary-index", globalIndexList.get(0).getIndexName());

        // Attributes
        List<AttributeDefinition> attributes = request.getAttributeDefinitions();
        assertNotNull(attributes);
        assertEquals(5, attributes.size());

        // Throughput
        assertEquals(30L, request.getProvisionedThroughput().getReadCapacityUnits().longValue());
        assertEquals(50L, request.getProvisionedThroughput().getWriteCapacityUnits().longValue());
    }

    @Test
    public void testCompareSchema() {
        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables();
        TableDescription table1 = tables.get(0);
        TableDescription table2 = copyTableDescription(table1);
        // No exception
        DynamoUtils.compareSchema(table1, table2);
    }

    @Test(expected = KeySchemaMismatchException.class)
    public void testCompareSchemaDifferentKeys() {
        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables();
        TableDescription table1 = tables.get(0);
        TableDescription table2 = copyTableDescription(table1);
        table2.getKeySchema().get(0).setAttributeName("some fake attr name");
        DynamoUtils.compareSchema(table1, table2);
    }

    // Copies the relevant attributes from a table (name, keys, global and local secondary indices)
    private static TableDescription copyTableDescription(TableDescription table1) {
        TableDescription table2 = new TableDescription();
        table2.setTableName(table1.getTableName());

        // key schema
        table2.setKeySchema(new ArrayList<KeySchemaElement>());
        for (KeySchemaElement ele : table1.getKeySchema()) {
            table2.getKeySchema().add(new KeySchemaElement(ele.getAttributeName(), ele.getKeyType()));
        }

        // global indices
        table2.setGlobalSecondaryIndexes(new ArrayList<GlobalSecondaryIndexDescription>());
        for (GlobalSecondaryIndexDescription index : table1.getGlobalSecondaryIndexes()) {
            table2.getGlobalSecondaryIndexes().add(
                    new GlobalSecondaryIndexDescription()
                            .withIndexName(index.getIndexName())
                            .withKeySchema(index.getKeySchema())
                            .withProjection(index.getProjection())
                            .withProvisionedThroughput(index.getProvisionedThroughput()));
        }

        // local indices
        table2.setLocalSecondaryIndexes(new ArrayList<LocalSecondaryIndexDescription>());
        for (LocalSecondaryIndexDescription index : table1.getLocalSecondaryIndexes()) {
            table2.getLocalSecondaryIndexes().add(
                    new LocalSecondaryIndexDescription()
                            .withIndexName(index.getIndexName())
                            .withKeySchema(index.getKeySchema())
                            .withProjection(index.getProjection()));
        }

        return table2;
    }
}
