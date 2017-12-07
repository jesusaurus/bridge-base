package org.sagebionetworks.bridge.exceptions;

/** Exception in Bridge calling Synapse, that's not a SynapseException. */
@SuppressWarnings("serial")
public class BridgeSynapseException extends Exception {
    public BridgeSynapseException() {
    }

    public BridgeSynapseException(String message) {
        super(message);
    }

    public BridgeSynapseException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeSynapseException(Throwable cause) {
        super(cause);
    }
}
