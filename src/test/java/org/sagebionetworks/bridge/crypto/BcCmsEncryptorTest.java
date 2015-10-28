package org.sagebionetworks.bridge.crypto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BcCmsEncryptorTest {
    private CmsEncryptor encryptor;
    private CmsEncryptor decryptor;

    @BeforeMethod
    public void before() throws Exception {
        KeyPair keyPair = KeyPairFactory.newRsa2048();
        CertificateFactory certFactory = new BcCertificateFactory();
        X509Certificate cert = certFactory.newCertificate(keyPair, new CertificateInfo.Builder().build());
        encryptor = new BcCmsEncryptor(cert, keyPair.getPrivate());
        decryptor = new BcCmsEncryptor(cert, keyPair.getPrivate());
    }

    @Test
    public void test() throws Exception {
        String text = "some text";
        byte[] bytes = text.getBytes(StandardCharsets.ISO_8859_1);
        byte[] encrypted = encryptor.encrypt(bytes);
        assertNotNull(encrypted);
        assertFalse(text.equals(new String(encrypted, StandardCharsets.ISO_8859_1)));
        byte[] decrypted = encryptor.decrypt(encrypted);
        assertEquals(text, new String(decrypted, StandardCharsets.ISO_8859_1));
    }

    @Test
    public void testDecryptDeterministic() throws Exception {
        String text = "some more text";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted = encryptor.encrypt(bytes);
        byte[] decrypted = decryptor.decrypt(encrypted);
        assertEquals(text, new String(decrypted, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncryptRandomized() throws Exception {
        String text = "some even more text";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted1 = encryptor.encrypt(bytes);
        assertNotNull(encrypted1);
        byte[] encrypted2 = encryptor.encrypt(bytes);
        assertNotNull(encrypted2);
        assertFalse(Arrays.equals(encrypted1, encrypted2));
    }
}
