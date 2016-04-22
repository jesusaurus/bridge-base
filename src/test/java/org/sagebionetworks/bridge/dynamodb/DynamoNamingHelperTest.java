package org.sagebionetworks.bridge.dynamodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;
import org.sagebionetworks.bridge.dynamodb.test.TestHealthDataRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Created by liujoshua on 4/18/16.
 */
public class DynamoNamingHelperTest {

    private static final String USER = "testUser";
    private static final Environment ENV = Environment.LOCAL;
    private static final String EXPECTED_PREFIX = "local-testUser-";

    private Config config;
    private DynamoNamingHelper namingHelper;

    @BeforeMethod
    public void setup() {
        config = mock(Config.class);
        namingHelper = new DynamoNamingHelper(config);

        when(config.getUser()).thenReturn(USER);
        when(config.getEnvironment()).thenReturn(ENV);
    }

    @Test
    public void testGetSimpleTableName() {
        assertEquals("TableName", namingHelper.getSimpleTableName(EXPECTED_PREFIX + "TableName"));
    }

    @Test
    public void testGetFullyQualifiedTableNameFromClass() {
        assertEquals(EXPECTED_PREFIX + "TestHealthDataRecord", namingHelper.getFullyQualifiedTableName(TestHealthDataRecord.class));
    }

    @Test
    public void testGetFullyQualifiedTableNameFromSimpleName() {
        assertEquals(EXPECTED_PREFIX + "TestHealthDataRecord", namingHelper.getFullyQualifiedTableName("TestHealthDataRecord"));
    }
}
