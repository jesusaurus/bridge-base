package org.sagebionetworks.bridge.crypto;

/** Thrown if someone tries to decrypt data with the wrong key. */
@SuppressWarnings("serial")
public class WrongEncryptionKeyException extends Exception {
    public WrongEncryptionKeyException() {
    }

    public WrongEncryptionKeyException(String message) {
        super(message);
    }

    public WrongEncryptionKeyException(String message, Throwable cause) {
        super(message, cause);
    }

    public WrongEncryptionKeyException(Throwable cause) {
        super(cause);
    }
}
