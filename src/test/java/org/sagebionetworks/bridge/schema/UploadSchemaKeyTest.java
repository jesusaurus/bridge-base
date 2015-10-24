package org.sagebionetworks.bridge.schema;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class UploadSchemaKeyTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void nullStudyId() {
        new UploadSchemaKey.Builder().withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void emptyStudyId() {
        new UploadSchemaKey.Builder().withStudyId("").withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void nullSchemaId() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void emptySchemaId() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void noRev() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void negativeRev() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema").withRevision(-1).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void zeroRev() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema").withRevision(0).build();
    }

    @Test
    public void happyCase() {
        UploadSchemaKey schemaKey = new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema")
                .withRevision(42).build();
        assertEquals(schemaKey.getStudyId(), "test-study");
        assertEquals(schemaKey.getSchemaId(), "test-schema");
        assertEquals(schemaKey.getRevision(), 42);
        assertEquals(schemaKey.toString(), "test-study-test-schema-v42");
    }
}
