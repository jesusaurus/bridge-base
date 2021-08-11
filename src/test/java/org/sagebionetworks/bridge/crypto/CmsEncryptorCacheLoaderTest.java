package org.sagebionetworks.bridge.crypto;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.nio.file.Files;

import com.google.common.base.Charsets;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.s3.S3Helper;

public class CmsEncryptorCacheLoaderTest {
    private static final String APP_ID = "test-app";
    private static final String CERT_BUCKET = "test-cert-bucket";
    private static final String PRIVATE_KEY_BUCKET = "test-priv-key-bucket";

    @Mock
    private S3Helper mockS3Helper;

    @InjectMocks
    private CmsEncryptorCacheLoader testCacheLoader;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);

        testCacheLoader.setCertBucket(CERT_BUCKET);
        testCacheLoader.setPrivateKeyBucket(PRIVATE_KEY_BUCKET);
    }

    @Test
    public void test() throws Exception {
        // Test strategy is to validate that we can successfully create a CmsEncryptor. S3Helper is mocked to return
        // test materials instead of calling through to S3.

        // set up cert and priv key PEM files as strings
        File certFile =  new ClassPathResource("/cms/rsacert.pem").getFile();
        byte[] certBytes = Files.readAllBytes(certFile.toPath());
        String certString = new String(certBytes, Charsets.UTF_8);
        File privKeyFile =  new ClassPathResource("/cms/rsaprivkey.pem").getFile();
        byte[] privKeyBytes = Files.readAllBytes(privKeyFile.toPath());
        String privKeyString = new String(privKeyBytes, Charsets.UTF_8);

        // mock S3 helper
        when(mockS3Helper.readS3FileAsString(CERT_BUCKET, APP_ID + ".pem"))
                .thenReturn(certString);
        when(mockS3Helper.readS3FileAsString(PRIVATE_KEY_BUCKET, APP_ID + ".pem"))
                .thenReturn(privKeyString);

        // execute and validate
        CmsEncryptor retVal = testCacheLoader.load(APP_ID);
        assertNotNull(retVal);
    }
}
