package org.sagebionetworks.bridge.crypto;

public interface Encryptor {

    String encrypt(String text);

    String decrypt(String text);
}
