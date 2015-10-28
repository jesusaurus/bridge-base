package org.sagebionetworks.bridge.crypto;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.security.KeyPair;

import org.testng.annotations.Test;

public class KeyPairFactoryTest {
    @Test
    public void test() {
        KeyPair keyPair = KeyPairFactory.newRsa2048();
        assertNotNull(keyPair);
        assertNotNull(keyPair.getPublic());
        assertNotNull(keyPair.getPrivate());
        assertNotNull(keyPair.getPublic().getEncoded());
        assertNotNull(keyPair.getPrivate().getEncoded());
        assertTrue(keyPair.getPublic().getAlgorithm().toLowerCase().contains("rsa"));
        assertTrue(keyPair.getPrivate().getAlgorithm().toLowerCase().contains("rsa"));
    }
}
