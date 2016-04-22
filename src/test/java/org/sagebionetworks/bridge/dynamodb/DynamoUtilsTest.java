package org.sagebionetworks.bridge.dynamodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DynamoUtilsTest {

    private DynamoUtils dynamoUtils;

    private DynamoNamingHelper namingHelper;
    private AmazonDynamoDB dynamoClient;

    @BeforeMethod
    public void setup() {
        namingHelper = mock(DynamoNamingHelper.class);
        dynamoClient = mock(AmazonDynamoDB.class);

        dynamoUtils = new DynamoUtils(namingHelper, dynamoClient);
    }



    @Test
    public void testGetCreateTableRequest() {

        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables(DynamoTestUtils.PACKAGE);
        TableDescription table = DynamoTestUtils.getTableByName(tables, "TestHealthDataRecord");
        CreateTableRequest request = dynamoUtils.getCreateTableRequest(table);
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
    public void testGetExistingTables() {

        // Mock
        final String table0Name = "local-test-table0";
        final TableDescription table0 = mock(TableDescription.class);
        when(table0.getTableName()).thenReturn(table0Name);
        final String table1Name = "dev-test-table1";
        final TableDescription table1 = mock(TableDescription.class);
        when(table1.getTableName()).thenReturn(table1Name);
        final String table2Name = "local-test-table2";
        final TableDescription table2 = mock(TableDescription.class);
        when(table2.getTableName()).thenReturn(table2Name);

        DescribeTableResult tableResult = mock(DescribeTableResult.class);
        when(tableResult.getTable()).thenReturn(table0);
        when(dynamoClient.describeTable(table0Name)).thenReturn(tableResult);
        tableResult = mock(DescribeTableResult.class);
        when(tableResult.getTable()).thenReturn(table1);
        when(dynamoClient.describeTable(table1Name)).thenReturn(tableResult);
        tableResult = mock(DescribeTableResult.class);
        when(tableResult.getTable()).thenReturn(table2);
        when(dynamoClient.describeTable(table2Name)).thenReturn(tableResult);

        ListTablesResult listResult = mock(ListTablesResult.class);
        when(listResult.getTableNames()).thenReturn(Arrays.asList(table0Name, table1Name));
        when(listResult.getLastEvaluatedTableName()).thenReturn(table1Name);
        when(dynamoClient.listTables()).thenReturn(listResult);
        listResult = mock(ListTablesResult.class);
        when(listResult.getTableNames()).thenReturn(Arrays.asList(table2Name));
        when(dynamoClient.listTables(table1Name)).thenReturn(listResult);

        // Test
        Map<String, TableDescription> tables = dynamoUtils.getAllExistingTables();
        assertNotNull(tables);
        assertEquals(3, tables.size());
        assertSame(table0, tables.get(table0Name));
        assertSame(table1, tables.get(table1Name));
        assertSame(table2, tables.get(table2Name));

        when(namingHelper.getTableNamePrefix()).thenReturn("local-test");
        tables = dynamoUtils.getExistingTables();

        assertNotNull(tables);
        assertEquals(2, tables.size());
        assertSame(table0, tables.get(table0Name));
        assertSame(table2, tables.get(table2Name));
    }

    @Test
    public void testCompareSchema() {
        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables(DynamoTestUtils.PACKAGE);
        TableDescription table1 = tables.get(0);
        TableDescription table2 = copyTableDescription(table1);
        // No exception
        dynamoUtils.compareSchema(table1, table2);
    }

    @Test(expectedExceptions = KeySchemaMismatchException.class)
    public void testCompareSchemaDifferentKeys() {
        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables(DynamoTestUtils.PACKAGE);
        TableDescription table1 = tables.get(0);
        TableDescription table2 = copyTableDescription(table1);
        table2.getKeySchema().get(0).setAttributeName("some fake attr name");
        dynamoUtils.compareSchema(table1, table2);
    }

    // Copies the relevant attributes from a table (name, keys, global and local secondary indices)
    private TableDescription copyTableDescription(TableDescription table1) {
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
            table2
                    .getGlobalSecondaryIndexes()
                    .add(new GlobalSecondaryIndexDescription()
                            .withIndexName(index.getIndexName())
                            .withKeySchema(index.getKeySchema())
                            .withProjection(index.getProjection())
                            .withProvisionedThroughput(index.getProvisionedThroughput()));
        }

        // local indices
        table2.setLocalSecondaryIndexes(new ArrayList<LocalSecondaryIndexDescription>());
        for (LocalSecondaryIndexDescription index : table1.getLocalSecondaryIndexes()) {
            table2
                    .getLocalSecondaryIndexes()
                    .add(new LocalSecondaryIndexDescription()
                            .withIndexName(index.getIndexName())
                            .withKeySchema(index.getKeySchema())
                            .withProjection(index.getProjection()));
        }

        return table2;
    }
}
