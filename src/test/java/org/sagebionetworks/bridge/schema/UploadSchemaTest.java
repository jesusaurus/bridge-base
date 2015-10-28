package org.sagebionetworks.bridge.schema;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.testng.annotations.Test;

public class UploadSchemaTest {
    private static final String DUMMY_FIELD_DEF_LIST_JSON = "[\n" +
            "   {\n" +
            "       \"name\":\"foo-field\",\n" +
            "       \"type\":\"STRING\"\n" +
            "   },\n" +
            "   {\n" +
            "       \"name\":\"bar-field\",\n" +
            "       \"type\":\"INT\"\n" +
            "   }\n" +
            "]";

    @Test
    public void happyCase() {
        UploadSchemaKey schemaKey = new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema")
                .withRevision(7).build();
        UploadSchema schema = new UploadSchema.Builder().withKey(schemaKey).addField("foo", "STRING")
                .addField("bar", "INT").build();
        assertEquals(schema.getKey().toString(), "test-study-test-schema-v7");

        List<String> fieldNameList = schema.getFieldNameList();
        assertEquals(fieldNameList.size(), 2);
        assertEquals(fieldNameList.get(0), "foo");
        assertEquals(fieldNameList.get(1), "bar");

        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        assertEquals(fieldTypeMap.size(), 2);
        assertEquals(fieldTypeMap.get("foo"), "STRING");
        assertEquals(fieldTypeMap.get("bar"), "INT");
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*key.*")
    public void nullKey() {
        new UploadSchema.Builder().addField("foo", "STRING").addField("bar", "INT").build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void emptyFieldDefList() {
        UploadSchemaKey schemaKey = new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema")
                .withRevision(7).build();
        new UploadSchema.Builder().withKey(schemaKey).build();
    }

    @Test
    public void fromDdbItem() throws Exception {
        Item ddbItem = new Item().withString("studyId", "test-study").withString("key", "test-study:ddb-schema")
                .withInt("revision", 13).withString("fieldDefinitions", DUMMY_FIELD_DEF_LIST_JSON);
        UploadSchema schema = UploadSchema.fromDdbItem(ddbItem);
        assertEquals(schema.getKey().toString(), "test-study-ddb-schema-v13");

        List<String> fieldNameList = schema.getFieldNameList();
        assertEquals(fieldNameList.size(), 2);
        assertEquals(fieldNameList.get(0), "foo-field");
        assertEquals(fieldNameList.get(1), "bar-field");

        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        assertEquals(fieldTypeMap.size(), 2);
        assertEquals(fieldTypeMap.get("foo-field"), "STRING");
        assertEquals(fieldTypeMap.get("bar-field"), "INT");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void fromDdbItemMalformedKey() throws Exception {
        Item ddbItem = new Item().withString("studyId", "test-study").withString("key", "required colon missing")
                .withInt("revision", 2).withString("fieldDefinitions", DUMMY_FIELD_DEF_LIST_JSON);
        UploadSchema.fromDdbItem(ddbItem);
    }


    @Test
    public void fromDdbItemSchemaIdWithColon() throws Exception {
        Item ddbItem = new Item().withString("studyId", "test-study").withString("key", "test-study:has:colons")
                .withInt("revision", 42).withString("fieldDefinitions", DUMMY_FIELD_DEF_LIST_JSON);
        UploadSchema schema = UploadSchema.fromDdbItem(ddbItem);
        assertEquals(schema.getKey().toString(), "test-study-has:colons-v42");

        // Don't bother testing the actual fields. They've already been tested in a previous test case.
    }
}
