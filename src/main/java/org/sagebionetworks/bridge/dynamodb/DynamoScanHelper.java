package org.sagebionetworks.bridge.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.api.ScanApi;

public class DynamoScanHelper {
    /** Scan a DDB object. */
    public Iterable<Item> scan(ScanApi scannable) {
        return scannable.scan();
    }
}
