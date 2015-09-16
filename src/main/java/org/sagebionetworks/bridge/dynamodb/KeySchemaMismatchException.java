package org.sagebionetworks.bridge.dynamodb;

@SuppressWarnings("serial")
public class KeySchemaMismatchException extends RuntimeException {

    public KeySchemaMismatchException(String message) {
        super(message);
    }
}
