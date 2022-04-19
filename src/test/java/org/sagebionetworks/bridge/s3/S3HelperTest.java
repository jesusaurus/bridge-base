package org.sagebionetworks.bridge.s3;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class S3HelperTest {
    // Test strategy is that given a mock input stream from a mock S3 object, the S3Helper can still turn that
    // input stream into a byte array or a string.

    @Mock
    AmazonS3 mockS3Client;

    @InjectMocks
    S3Helper s3Helper;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void copyS3File() {
        // Execute.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        s3Helper.copyS3File("source-bucket", "source-key", "destination-bucket",
                "destination-key", metadata);

        // Verify.
        ArgumentCaptor<CopyObjectRequest> requestCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockS3Client).copyObject(requestCaptor.capture());

        CopyObjectRequest request = requestCaptor.getValue();
        assertEquals(request.getSourceBucketName(), "source-bucket");
        assertEquals(request.getSourceKey(), "source-key");
        assertEquals(request.getDestinationBucketName(), "destination-bucket");
        assertEquals(request.getDestinationKey(), "destination-key");
        assertSame(request.getNewObjectMetadata(), metadata);
    }

    @Test
    public void downloadS3File() {
        // execute
        File mockFile = mock(File.class);
        s3Helper.downloadS3File("test-bucket", "test-key", mockFile);

        // The inner S3 client does all the work. Validate that we passed args to it correctly.
        ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockS3Client).getObject(requestCaptor.capture(), same(mockFile));

        GetObjectRequest request = requestCaptor.getValue();
        assertEquals(request.getBucketName(), "test-bucket");
        assertEquals(request.getKey(), "test-key");
    }

    @Test
    public void generatePresignedUrl() throws Exception {
        // mock S3 client
        when(mockS3Client.generatePresignedUrl("test-bucket", "test-key", new DateTime(42).toDate(), HttpMethod.GET))
                .thenReturn(new URL("http://www.example.com/"));

        // execute and validate
        URL retval = s3Helper.generatePresignedUrl("test-bucket", "test-key", new DateTime(42), HttpMethod.GET);
        assertEquals(retval.toString(), "http://www.example.com/");
    }

    @Test
    public void getObjectMetadata() {
        // Mock S3 client.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        when(mockS3Client.getObjectMetadata("test-bucket", "test-key")).thenReturn(objectMetadata);

        // Execute and validate.
        ObjectMetadata retval = s3Helper.getObjectMetadata("test-bucket", "test-key");
        assertSame(retval, objectMetadata);
    }

    @Test
    public void readAsBytes() throws Exception {
        String bucket = "bucket-with-bytes";
        String key = "key-with-bytes";
        String content = "this is the answer in bytes";

        S3Object spyS3Object = spyS3ObjectForRead(content);
        when(mockS3Client.getObject(bucket, key)).thenReturn(spyS3Object);
        byte[] retValBytes = s3Helper.readS3FileAsBytes(bucket, key);
        assertEquals(new String(retValBytes, Charsets.UTF_8), content);

        verify(spyS3Object, atLeastOnce()).close();
    }

    @Test
    public void readAsString() throws Exception {
        String bucket = "test-bucket";
        String key = "test-key";
        String content = "this is the answer";

        S3Object spyS3Object = spyS3ObjectForRead(content);
        when(mockS3Client.getObject(bucket, key)).thenReturn(spyS3Object);
        String retVal = s3Helper.readS3FileAsString(bucket, key);
        assertEquals(retVal, content);

        verify(spyS3Object, atLeastOnce()).close();
    }

    @Test
    public void readAsLines() throws Exception {
        String bucket = "bucket-with-lines";
        String key = "key-with-lines";
        String content = "foo\nbar\nbaz";

        S3Object spyS3Object = spyS3ObjectForRead(content);
        when(mockS3Client.getObject(bucket, key)).thenReturn(spyS3Object);
        List<String> lineList = s3Helper.readS3FileAsLines(bucket, key);
        assertEquals(lineList.size(), 3);
        assertEquals(lineList.get(0), "foo");
        assertEquals(lineList.get(1), "bar");
        assertEquals(lineList.get(2), "baz");

        verify(spyS3Object, atLeastOnce()).close();
    }

    private static S3Object spyS3ObjectForRead(String content) {
        // mock S3 stream
        byte[] contentBytes = content.getBytes(Charsets.UTF_8);
        InputStream contentStream = new ByteArrayInputStream(contentBytes);

        // not sure this is safe, but this is the easiest way to mock an S3 stream
        S3ObjectInputStream mockS3Stream = new S3ObjectInputStream(contentStream, null, false);

        // mock S3 object - This needs to be a spy, so we can verify that it was closed.
        S3Object mockS3Object = spy(new S3Object());
        mockS3Object.setObjectContent(mockS3Stream);
        return mockS3Object;
    }

    @Test
    public void writeBytes() throws Exception {
        // execute and validate
        byte[] content = "test write bytes".getBytes(Charsets.UTF_8);
        s3Helper.writeBytesToS3("write-bucket", "write-bytes-key", content);

        ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
        ArgumentCaptor<ObjectMetadata> metadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Client).putObject(eq("write-bucket"), eq("write-bytes-key"), streamCaptor.capture(),
                metadataCaptor.capture());

        InputStream writtenStream = streamCaptor.getValue();
        byte[] writtenBytes = ByteStreams.toByteArray(writtenStream);
        assertEquals(new String(writtenBytes, Charsets.UTF_8), "test write bytes");

        ObjectMetadata metadata = metadataCaptor.getValue();
        assertEquals(metadata.getContentLength(), content.length);
    }

    @Test
    public void writeFile() {
        // execute and validate
        File mockFile = mock(File.class);
        s3Helper.writeFileToS3("write-bucket", "write-file-key", mockFile);
        verify(mockS3Client).putObject("write-bucket", "write-file-key", mockFile);
    }

    @Test
    public void writeFileWithMetadata() {
        ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        
        // execute and validate
        File mockFile = mock(File.class);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        s3Helper.writeFileToS3("write-bucket", "write-file-key", mockFile, metadata);
        verify(mockS3Client).putObject(putObjectRequestCaptor.capture());
        
        PutObjectRequest request = putObjectRequestCaptor.getValue();
        assertEquals("write-bucket", request.getBucketName());
        assertEquals("write-file-key", request.getKey());
        assertEquals(mockFile, request.getFile());
        assertEquals(metadata, request.getMetadata());
        assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, request.getMetadata().getSSEAlgorithm());
    }
    
    @Test
    public void writeLines() throws Exception {
        // execute and validate
        s3Helper.writeLinesToS3("test-bucket", "test-key", ImmutableList.of("foo", "bar", "baz"));

        ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
        ArgumentCaptor<ObjectMetadata> metadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Client).putObject(eq("test-bucket"), eq("test-key"), streamCaptor.capture(),
                metadataCaptor.capture());

        String expected = "foo\nbar\nbaz";

        InputStream writtenStream = streamCaptor.getValue();
        byte[] writtenBytes = ByteStreams.toByteArray(writtenStream);
        assertEquals(new String(writtenBytes, Charsets.UTF_8), expected);

        ObjectMetadata metadata = metadataCaptor.getValue();
        assertEquals(metadata.getContentLength(), expected.length());
    }
    
    @Test
    public void writeBytesToS3WithMetadata() throws Exception {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        byte[] data = "[1,2,3]".getBytes(Charsets.UTF_8);
        
        s3Helper.writeBytesToS3("test-bucket", "test-key", data, metadata);
        
        ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
        
        verify(mockS3Client).putObject(eq("test-bucket"), eq("test-key"), streamCaptor.capture(),
                same(metadata));
        
        InputStream writtenStream = streamCaptor.getValue();
        byte[] writtenBytes = ByteStreams.toByteArray(writtenStream);
        assertEquals(new String(writtenBytes, Charsets.UTF_8), "[1,2,3]");

        assertEquals(metadata.getContentLength(), data.length);
    }
    
}
