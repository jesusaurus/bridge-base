package org.sagebionetworks.bridge.dynamodb;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;

/**
 * Created by liujoshua on 4/18/16.
 */
public class DynamoNamingHelper {

    private static final String TABLE_NAME_DELIMITER = "-";

    private final Config config;

    public DynamoNamingHelper(Config config) {
        checkNotNull(config);

        this.config = config;
    }

    /**
     * Given a class annotated as DynamoDBTable, gets the name override responsible
     * for generating the fully qualified table name.
     */
    public DynamoDBMapperConfig.TableNameOverride getTableNameOverride(final Class<?> dynamoTable) {
        checkNotNull(dynamoTable);

        final DynamoDBTable table = dynamoTable.getAnnotation(DynamoDBTable.class);
        if (table == null) {
            throw new IllegalArgumentException("Missing DynamoDBTable annotation for " + dynamoTable.getName());
        }
        return new DynamoDBMapperConfig.TableNameOverride(getTableNamePrefix() + table.tableName());
    }

    /**
     * Given a class annotated as DynamoDBTable, gets the fully qualified table name.
     */
    public String getFullyQualifiedTableName(final Class<?> dynamoTable) {
        return getTableNameOverride(dynamoTable).getTableName();
    }


    /**
     * Given a class annotated as DynamoDBTable, gets the fully qualified table name.
     */
    public String getFullyQualifiedTableName(final String simpleTableName) {
        checkNotNull(simpleTableName);

        return getTableNamePrefix() + simpleTableName;
    }

    /**
     * Given a fully-qualified table name, gets the simple table name.
     */
    public String getSimpleTableName(final String fullyQualifiedTableName) {
        checkNotNull(fullyQualifiedTableName);

        final String prefix = getTableNamePrefix();
        if (!fullyQualifiedTableName.startsWith(prefix)) {
            throw new IllegalArgumentException(fullyQualifiedTableName + " is not a fully qualified table name.");
        }
        return fullyQualifiedTableName.substring(prefix.length());
    }


    public String getTableNamePrefix() {
        final Environment env = config.getEnvironment();
        return env.name().toLowerCase() + TABLE_NAME_DELIMITER + config.getUser() + TABLE_NAME_DELIMITER;
    }

}
