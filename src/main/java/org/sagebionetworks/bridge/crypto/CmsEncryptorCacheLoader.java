package org.sagebionetworks.bridge.crypto;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

import com.google.common.cache.CacheLoader;

import org.sagebionetworks.bridge.s3.S3Helper;

/**
 * This is the cache loader that supports loading CMS encryptors on demand, keyed by the appId. If the app's
 * encryptor is already in the cache, this returns that encryptor. If it isn't, this app will pull the PEM files for
 * the cert and private key from the configured S3 bucket and construct an encryptor using those encryption materials.
 */
public class CmsEncryptorCacheLoader extends CacheLoader<String, CmsEncryptor> {
    private static final String PEM_FILENAME_FORMAT = "%s.pem";

    private String certBucket;
    private String privateKeyBucket;
    private S3Helper s3Helper;

    /**
     * S3 bucket in which the public encryption certificates live. The certs must have a filename in the format
     * [appId].pem
     */
    public final void setCertBucket(String certBucket) {
        this.certBucket = certBucket;
    }

    /**
     * S3 bucket in which the private encryption keys live. The keys must have a filename in the format [appId].pem.
     */
    public final void setPrivateKeyBucket(String privateKeyBucket) {
        this.privateKeyBucket = privateKeyBucket;
    }

    /** S3 Helper, used to load the public encryption certificate and the private encryption key. */
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Override
    public CmsEncryptor load(String appId) throws CertificateEncodingException, IOException {
        checkNotNull(appId);
        String pemFileName = String.format(PEM_FILENAME_FORMAT, appId);

        // download certificate
        String certPem = s3Helper.readS3FileAsString(certBucket, pemFileName);
        X509Certificate cert = PemUtils.loadCertificateFromPem(certPem);

        // download private key
        String privKeyPem = s3Helper.readS3FileAsString(privateKeyBucket, pemFileName);
        PrivateKey privKey = PemUtils.loadPrivateKeyFromPem(privKeyPem);

        return new BcCmsEncryptor(cert, privKey);
    }
}
