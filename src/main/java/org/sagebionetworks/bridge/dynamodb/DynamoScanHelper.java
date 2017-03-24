package org.sagebionetworks.bridge.dynamodb;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.api.ScanApi;

public class DynamoScanHelper {
    /** Scan a DDB object. */
    public Iterable<Item> scan(ScanApi scannable, String filterExpression, String projectionExpression, Map<String, String> nameMap, Map<String, Object> valueMap) {
        return scannable.scan(filterExpression, projectionExpression, nameMap, valueMap);
    }
}
