package org.sagebionetworks.bridge.exceptions;

/** A BridgeSynapseException that is a deterministic failure and should not be retried. */
@SuppressWarnings("serial")
public class BridgeSynapseNonRetryableException extends BridgeSynapseException {
    public BridgeSynapseNonRetryableException() {
    }

    public BridgeSynapseNonRetryableException(String message) {
        super(message);
    }

    public BridgeSynapseNonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeSynapseNonRetryableException(Throwable cause) {
        super(cause);
    }
}
