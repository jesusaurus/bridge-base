package org.sagebionetworks.bridge.s3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.jcabi.aspects.RetryOnFailure;
import org.joda.time.DateTime;

/**
 * Helper class that simplifies reading S3 files. This is generally created by Spring. However, we don't use the
 * Component annotation because there are multiple S3 clients, so there may be multiple S3 helpers.
 */
public class S3Helper {
    private static final Joiner LINES_JOINER = Joiner.on('\n').useForNull("");

    private AmazonS3Client s3Client;

    /**
     * S3 Client. This is configured by Spring. We don't use the Autowired annotation because there are multiple S3
     * clients.
     */
    public final void setS3Client(AmazonS3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Downloads a file from S3 directly to the specified file.
     *
     * @param bucket
     *         S3 bucket to download from
     * @param key
     *         S3 key to download from
     * @param destinationFile
     *         file to download to
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = AmazonClientException.class,
            randomize = false)
    public void downloadS3File(String bucket, String key, File destinationFile) {
        GetObjectRequest s3Request = new GetObjectRequest(bucket, key);
        s3Client.getObject(s3Request, destinationFile);
    }

    /**
     * Pass through to S3 generate presigned URL. This exists mainly as a convenience, so we can do all S3 operations
     * through the helper instead of using the S3 client directly for some operations. This also enables us to add
     * retry logic later.
     *
     * @param bucket
     *         bucket containing the file we want to get a pre-signed URL for
     * @param key
     *         key (filename) to get the pre-signed URL for
     * @param expiration
     *         expiration date of the pre-signed URL
     * @param httpMethod
     *         HTTP method to restrict our pre-signed URL
     * @return the generated pre-signed URL
     */
    public URL generatePresignedUrl(String bucket, String key, DateTime expiration, HttpMethod httpMethod) {
        return s3Client.generatePresignedUrl(bucket, key, expiration.toDate(), httpMethod);
    }

    /**
     * Read the given S3 file as a byte array in memory.
     *
     * @param bucket
     *         S3 bucket to read from, must be non-null and non-empty
     * @param key
     *         S3 key (filename), must be non-null and non-empty
     * @return the S3 file contents as an in-memory byte array
     * @throws IOException
     *         if closing the stream fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = AmazonClientException.class,
            randomize = false)
    public byte[] readS3FileAsBytes(String bucket, String key) throws IOException {
        try (S3Object s3File = s3Client.getObject(bucket, key); InputStream s3Stream = s3File.getObjectContent()) {
            return ByteStreams.toByteArray(s3Stream);
        }
    }

    /**
     * Read the given S3 file contents as a list of lines. The encoding is assumed to be UTF-8.
     *
     * @param bucket
     *         S3 bucket to read from, must be non-null and non-empty
     * @param key
     *         S3 key (filename), must be non-null and non-empty
     * @return the S3 file as a list of lines
     * @throws IOException
     *         if reading or closing the stream fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = AmazonClientException.class,
            randomize = false)
    public List<String> readS3FileAsLines(String bucket, String key) throws IOException {
        try (S3Object s3File = s3Client.getObject(bucket, key); BufferedReader recordIdReader = new BufferedReader(
                new InputStreamReader(s3File.getObjectContent(), Charsets.UTF_8))) {
            return CharStreams.readLines(recordIdReader);
        }
    }

    /**
     * Read the given S3 file contents as a string. The encoding is assumed to be UTF-8.
     *
     * @param bucket
     *         S3 bucket to read from, must be non-null and non-empty
     * @param key
     *         S3 key (filename), must be non-null and non-empty
     * @return the S3 file contents as a string
     * @throws IOException
     *         if closing the stream fails
     */
    public String readS3FileAsString(String bucket, String key) throws IOException {
        byte[] bytes = readS3FileAsBytes(bucket, key);
        return new String(bytes, Charsets.UTF_8);
    }

    /**
     * Upload the given bytes as an S3 file to S3.
     *
     * @param bucket
     *         bucket to upload to
     * @param key
     *         key (filename) to upload to
     * @param data
     *         bytes to upload
     * @throws IOException
     *         if uploading the byte stream fails
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = AmazonClientException.class,
            randomize = false)
    public void writeBytesToS3(String bucket, String key, byte[] data) throws IOException {
        try (InputStream dataInputStream = new ByteArrayInputStream(data)) {
            s3Client.putObject(bucket, key, dataInputStream, null);
        }
    }

    /**
     * Pass through to S3 PutObject. This exists mainly as a convenience, so we can do all S3 operations through the
     * helper instead of using the S3 client directly for some operations. This also enables us to add retry logic
     * later.
     *
     * @param bucket
     *         bucket to upload to
     * @param key
     *         key (filename) to upload to
     * @param file
     *         file to upload
     */
    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = AmazonClientException.class,
            randomize = false)
    public void writeFileToS3(String bucket, String key, File file) {
        s3Client.putObject(bucket, key, file);
    }

    /**
     * Upload the given lines as a file to S3. The lines will be joined by a single newline (\n), and then streamed to
     * S3.
     *
     * @param bucket
     *         bucket to upload to
     * @param key
     *         key (filename) to upload to
     * @param lines
     *         lines to upload
     * @throws IOException
     *         if uploading the lines fails
     */
    public void writeLinesToS3(String bucket, String key, Iterable<String> lines) throws IOException {
        // Join the lines to a String, then convert the String to bytes using UTF-8.
        String joinedLines = LINES_JOINER.join(lines);
        byte[] linesData = joinedLines.getBytes(Charsets.UTF_8);
        writeBytesToS3(bucket, key, linesData);
    }
}
