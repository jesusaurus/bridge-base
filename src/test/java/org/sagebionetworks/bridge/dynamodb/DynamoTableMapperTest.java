package org.sagebionetworks.bridge.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.sagebionetworks.bridge.config.Environment;
import org.sagebionetworks.bridge.dynamodb.test.TestHealthDataRecord;

import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class DynamoTableMapperTest {

    @Test
    public void testGetAnnotatedTables() {
        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables();
        assertNotNull(tables);
        assertEquals(2, tables.size());
        Map<String, TableDescription> tableMap = new HashMap<String, TableDescription>();
        for (TableDescription table : tables) {
            tableMap.put(table.getTableName(), table);
        }
        String tableName = Environment.LOCAL.name().toLowerCase() + "-"
                + DynamoTestUtils.class.getSimpleName() + "-TestHealthDataRecord";
        TableDescription table = tableMap.get(tableName);
        assertNotNull(table);
        assertEquals(2, table.getKeySchema().size());
        assertEquals(5, table.getAttributeDefinitions().size());
        assertEquals(2, table.getLocalSecondaryIndexes().size());
        assertEquals(1, table.getGlobalSecondaryIndexes().size());
        assertEquals(30L, table.getProvisionedThroughput().getReadCapacityUnits().longValue());
        assertEquals(50L, table.getProvisionedThroughput().getWriteCapacityUnits().longValue());
    }

    @Test
    public void testLoadDynamoTableClasses() {
        List<Class<?>> classes = DynamoTestUtils.MAPPER.loadDynamoTableClasses();
        assertNotNull(classes);
        assertEquals(2, classes.size());
        Set<String> classSet = new HashSet<String>();
        for (Class<?> clazz : classes) {
            classSet.add(clazz.getName());
        }
        assertTrue(classSet.contains("org.sagebionetworks.bridge.dynamodb.test.TestHealthDataRecord"));
    }

    @Test
    public void testGetAttributeName() throws Exception {
        AnnotationBasedTableCreator mapper = DynamoTestUtils.MAPPER;
        Method method = TestHealthDataRecord.class.getMethod("getStartDate");
        assertEquals("startDate", mapper.getAttributeName(method));
        method = TestHealthDataRecord.class.getMethod("hashCode");
        assertEquals("hashCode", mapper.getAttributeName(method));
    }

    @Test
    public void testGetAttributeType() throws Exception {
        AnnotationBasedTableCreator mapper = DynamoTestUtils.MAPPER;
        Method method = TestHealthDataRecord.class.getMethod("getStartDate");
        assertEquals(ScalarAttributeType.N, mapper.getAttributeType(method));
        method = TestHealthDataRecord.class.getMethod("getData");
        assertEquals(ScalarAttributeType.S, mapper.getAttributeType(method));
    }

    @Test
    public void createsGlobalIndices() {
        List<TableDescription> tables = DynamoTestUtils.MAPPER.getTables();
        TableDescription table = DynamoTestUtils.getTableByName(tables, "TestTask");
        assertEquals(3, table.getGlobalSecondaryIndexes().size());

        GlobalSecondaryIndexDescription index = findIndex(table.getGlobalSecondaryIndexes(), "guid-index");
        assertEquals("INCLUDE", index.getProjection().getProjectionType());
        assertEquals("guid", index.getKeySchema().get(0).getAttributeName());
        assertEquals(Long.valueOf(18), index.getProvisionedThroughput().getWriteCapacityUnits());
        assertEquals(Long.valueOf(20), index.getProvisionedThroughput().getReadCapacityUnits());

        index = findIndex(table.getGlobalSecondaryIndexes(), "healthCode-scheduledOn-index");
        assertEquals("ALL", index.getProjection().getProjectionType());
        assertEquals("healthCode", index.getKeySchema().get(0).getAttributeName());
        assertEquals("scheduledOn", index.getKeySchema().get(1).getAttributeName());
        assertEquals(Long.valueOf(18), index.getProvisionedThroughput().getWriteCapacityUnits());
        assertEquals(Long.valueOf(20), index.getProvisionedThroughput().getReadCapacityUnits());

        index = findIndex(table.getGlobalSecondaryIndexes(), "healthCode-expiresOn-index");
        assertEquals("expiresOn", index.getKeySchema().get(0).getAttributeName());
    }

    private GlobalSecondaryIndexDescription findIndex(List<GlobalSecondaryIndexDescription> list, String name) {
        for (GlobalSecondaryIndexDescription index : list) {
            if (index.getIndexName().equals(name)) {
                return index;
            }
        }
        return null;
    }
}
