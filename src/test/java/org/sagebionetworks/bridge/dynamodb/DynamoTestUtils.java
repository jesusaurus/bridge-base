package org.sagebionetworks.bridge.dynamodb;

import java.util.List;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;

import com.amazonaws.services.dynamodbv2.model.TableDescription;

public final class DynamoTestUtils {

    static final String PACKAGE = "org.sagebionetworks.bridge.dynamodb.test";

    static final Config CONFIG = new Config() {
        @Override
        public String getUser() {
            return DynamoTestUtils.class.getSimpleName();
        }
        @Override
        public Environment getEnvironment() {
            return Environment.LOCAL;
        }
        @Override
        public String get(String key) {
            return null;
        }
        @Override
        public int getInt(String key) {
            return 0;
        }
        @Override
        public List<String> getList(String key) {
            return null;
        }
    };

    static final AnnotationBasedTableCreator MAPPER = new AnnotationBasedTableCreator(DynamoTestUtils.PACKAGE, DynamoTestUtils.CONFIG);

    /**
     * Finds the first table that matches the supplied partial name.
     */
    static TableDescription getTableByName(final List<TableDescription> tables, final String partialTableName) {
        for (TableDescription table : tables) {
            if (table.getTableName().indexOf(partialTableName) > -1) {
                return table;
            }
        }
        return null;
    }

    private DynamoTestUtils() {}
}
