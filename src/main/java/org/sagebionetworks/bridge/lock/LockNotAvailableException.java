package org.sagebionetworks.bridge.lock;

@SuppressWarnings("serial")
public class LockNotAvailableException extends RuntimeException {

    public LockNotAvailableException() {
    }

    public LockNotAvailableException(final String key) {
        super("Lock for " + key + " is not available.");
    }
}
