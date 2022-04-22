package org.sagebionetworks.bridge.crypto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BcCmsEncryptorTest {
    private static CmsEncryptor encryptorTwoWay;
    private static CmsEncryptor encryptorOnly;

    @BeforeClass
    public static void before() throws Exception {
        KeyPair keyPair = KeyPairFactory.newRsa2048();
        CertificateFactory certFactory = new BcCertificateFactory();
        X509Certificate cert = certFactory.newCertificate(keyPair, new CertificateInfo.Builder().build());
        encryptorTwoWay = new BcCmsEncryptor(cert, keyPair.getPrivate());
        encryptorOnly = new BcCmsEncryptor(cert, null);
    }

    @Test
    public void test() throws Exception {
        String text = "some text";
        byte[] bytes = text.getBytes(StandardCharsets.ISO_8859_1);
        byte[] encrypted = encryptorTwoWay.encrypt(bytes);
        assertNotNull(encrypted);
        assertFalse(text.equals(new String(encrypted, StandardCharsets.ISO_8859_1)));
        byte[] decrypted = encryptorTwoWay.decrypt(encrypted);
        assertEquals(text, new String(decrypted, StandardCharsets.ISO_8859_1));
    }

    @Test
    public void testDecryptDeterministic() throws Exception {
        String text = "some more text";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted = encryptorTwoWay.encrypt(bytes);
        byte[] decrypted = encryptorTwoWay.decrypt(encrypted);
        assertEquals(text, new String(decrypted, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncryptRandomized() throws Exception {
        String text = "some even more text";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted1 = encryptorTwoWay.encrypt(bytes);
        assertNotNull(encrypted1);
        byte[] encrypted2 = encryptorTwoWay.encrypt(bytes);
        assertNotNull(encrypted2);
        assertFalse(Arrays.equals(encrypted1, encrypted2));
    }

    @Test
    public void encryptorOnlyCannotDecrypt() throws Exception {
        // Encryptor only can encrypt.
        String text = "more more text";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted = encryptorOnly.encrypt(bytes);
        assertNotNull(encrypted);

        // Cannot decrypt as bytes.
        try {
            encryptorOnly.decrypt(encrypted);
            fail("expected exception");
        } catch (UnsupportedOperationException ex) {
            // expected exception
        }

        // Cannot decrypt as stream.
        try (ByteArrayInputStream stream = new ByteArrayInputStream(encrypted)) {
            encryptorOnly.decrypt(stream);
            fail("expected exception");
        } catch (UnsupportedOperationException ex) {
            // expected exception
        }
    }

    @Test(expectedExceptions = WrongEncryptionKeyException.class)
    public void decryptWithWrongKey() throws Exception {
        // Make an encryptor with the wrong key.
        KeyPair keyPair = KeyPairFactory.newRsa2048();
        CertificateFactory certFactory = new BcCertificateFactory();
        X509Certificate cert = certFactory.newCertificate(keyPair, new CertificateInfo.Builder().build());
        CmsEncryptor wrongKeyEncryptor = new BcCmsEncryptor(cert, null);

        // Encrypt some data with the wrong key.
        String text = "some text";
        byte[] bytes = text.getBytes(StandardCharsets.ISO_8859_1);
        byte[] encrypted = wrongKeyEncryptor.encrypt(bytes);

        // Decrypt with the correct key. This will throw.
        encryptorTwoWay.decrypt(encrypted);
    }
}
