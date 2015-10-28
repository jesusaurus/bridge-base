package org.sagebionetworks.bridge.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.api.QueryApi;

/**
 * In the Dynamo Document API, Table.query() and Index.query() return an ItemCollection. While ItemCollection
 * implements Iterable, it overrides iterator() to return an IteratorSupport, which is not publicly exposed. This makes
 * query() nearly impossible to mock. So we abstract it away into a methods that we can mock.
 */
public class DynamoQueryHelper {
    /** Query a DDB object (table or index) on the hash key only. */
    public Iterable<Item> query(QueryApi queryable, String hashKeyName, Object hashKeyValue) {
        return queryable.query(hashKeyName, hashKeyValue);
    }

    /** Query a DDB object (table or index) on both hash key and range key. */
    public Iterable<Item> query(QueryApi queryable, String hashKeyName, Object hashKeyValue,
            RangeKeyCondition rangeKeyCondition) {
        return queryable.query(hashKeyName, hashKeyValue, rangeKeyCondition);
    }
}
