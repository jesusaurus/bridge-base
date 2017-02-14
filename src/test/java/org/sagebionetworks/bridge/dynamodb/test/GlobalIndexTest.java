package org.sagebionetworks.bridge.dynamodb.test;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

// Contrived test class for use with AnnotationBasedTableCreatorTest. We only care about method annotations, so the
// methods all return null.
@DynamoDBTable(tableName = "GlobalIndexTest")
public class GlobalIndexTest {
    @DynamoDBHashKey
    public String getTableHashKey() {
        return null;
    }

    @DynamoDBIndexHashKey(attributeName = "hashWithoutRangeHashKey",
            globalSecondaryIndexName = "hash-without-range-index")
    public String getHashWithoutRangeHashKey() {
        return null;
    }

    @DynamoDBIndexHashKey(attributeName = "simpleIndexHashKey", globalSecondaryIndexName = "simple-index")
    public String getSimpleIndexHashKey() {
        return null;
    }

    @DynamoDBIndexRangeKey(attributeName = "simpleIndexRangeKey", globalSecondaryIndexName = "simple-index")
    public String getSimpleIndexRangeKey() {
        return null;
    }

    @DynamoDBIndexHashKey(attributeName = "listIndexHashKeyA",
            globalSecondaryIndexNames = { "ax-list-index", "ay-list-index", "a-only-list-index" })
    public String getListIndexHashKeyA() {
        return null;
    }

    @DynamoDBIndexHashKey(attributeName = "listIndexHashKeyB",
            globalSecondaryIndexNames = { "bx-list-index", "by-list-index" })
    public String getListIndexHashKeyB() {
        return null;
    }

    @DynamoDBIndexRangeKey(attributeName = "listIndexRangeKeyX",
            globalSecondaryIndexNames = { "ax-list-index", "bx-list-index" })
    public String getListIndexRangeKeyX() {
        return null;
    }

    @DynamoDBIndexRangeKey(attributeName = "listIndexRangeKeyY",
            globalSecondaryIndexNames = { "ay-list-index", "by-list-index" })
    public String getListIndexRangeKeyY() {
        return null;
    }
}
