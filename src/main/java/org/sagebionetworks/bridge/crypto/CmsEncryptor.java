package org.sagebionetworks.bridge.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateEncodingException;

import org.bouncycastle.cms.CMSException;

/**
 * Does CMS encryption.
 */
public interface CmsEncryptor {

    byte[] encrypt(byte[] bytes) throws CMSException, IOException;

    byte[] decrypt(byte[] bytes) throws CMSException, CertificateEncodingException, IOException;

    /**
     * Takes in a stream of encrypted data and outputs a stream of decrypted data. The caller is responsible for
     * closing both streams.
     */
    InputStream decrypt(InputStream encryptedStream) throws CertificateEncodingException, CMSException, IOException;
}
