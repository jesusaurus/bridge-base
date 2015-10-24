package org.sagebionetworks.bridge.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

/** Represents an upload schema, including key, field names, and field types. */
public class UploadSchema {
    /** Set of field types that are considered attachments. */
    public static final Set<String> ATTACHMENT_TYPE_SET = ImmutableSet.of("ATTACHMENT_BLOB", "ATTACHMENT_CSV",
            "ATTACHMENT_JSON_BLOB", "ATTACHMENT_JSON_TABLE");

    private final UploadSchemaKey key;
    private final List<String> fieldNameList;
    private final Map<String, String> fieldTypeMap;
    private final String name;

    /** Upload schema constructor. To construct, go through builder or static factory methods. */
    private UploadSchema(UploadSchemaKey key, List<String> fieldNameList, Map<String, String> fieldTypeMap,
            String name) {
        this.key = key;
        this.fieldNameList = ImmutableList.copyOf(fieldNameList);
        this.fieldTypeMap = ImmutableMap.copyOf(fieldTypeMap);
        this.name = name;
    }

    /**
     * Factory method for converting a DDB item into an UploadSchema. This assumes the schema came from the Upload
     * Schemas table.
     *
     * @param ddbItem
     *         DDB item from the Upload Schemas table
     * @return the converted upload schema
     * @throws IOException
     *         if parsing the DDB item fails
     */
    public static UploadSchema fromDdbItem(Item ddbItem) throws IOException {
        UploadSchema.Builder schemaBuilder = new UploadSchema.Builder();

        // parse key
        String ddbKey = ddbItem.getString("key");
        String[] ddbKeyTokenArray = ddbKey.split(":", 2);
        if (ddbKeyTokenArray.length != 2) {
            throw new IllegalArgumentException("Malformed key " + ddbKey);
        }

        String studyId = ddbKeyTokenArray[0];
        String schemaId = ddbKeyTokenArray[1];
        int rev = ddbItem.getInt("revision");
        UploadSchemaKey key = new UploadSchemaKey.Builder().withStudyId(studyId).withSchemaId(schemaId)
                .withRevision(rev).build();
        schemaBuilder.withKey(key);

        // add fields
        JsonNode fieldDefList = DefaultObjectMapper.INSTANCE.readTree(ddbItem.getString("fieldDefinitions"));
        for (JsonNode oneFieldDef : fieldDefList) {
            String name = oneFieldDef.get("name").textValue();
            String bridgeType = oneFieldDef.get("type").textValue();
            schemaBuilder.addField(name, bridgeType);
        }

        return schemaBuilder.build();
    }

    /** Schema key (study, schema, revision). */
    public UploadSchemaKey getKey() {
        return key;
    }

    /** List of field names. This preserves the order of fields, such as for table columns. */
    public List<String> getFieldNameList() {
        return fieldNameList;
    }

    /** Map from field name to field type. */
    public Map<String, String> getFieldTypeMap() {
        return fieldTypeMap;
    }

    /** Schema builder. */
    public static class Builder {
        private UploadSchemaKey key;
        private final List<String> fieldNameList = new ArrayList<>();
        private final Map<String, String> fieldTypeMap = new HashMap<>();
        private String name;

        /** @see UploadSchema#getKey */
        public Builder withKey(UploadSchemaKey key) {
            this.key = key;
            return this;
        }

        /** Adds a single field to the Builder's field name list and field type map. */
        public Builder addField(String fieldName, String fieldType) {
            fieldNameList.add(fieldName);
            fieldTypeMap.put(fieldName, fieldType);
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        /** Builds an upload schema, validating that the key is specified and that there is at least one field. */
        public UploadSchema build() {
            if (key == null) {
                throw new IllegalStateException("key must be specified");
            }

            if (fieldNameList.isEmpty()) {
                throw new IllegalStateException("there must be at least one field");
            }

            // TODO doc
            // Some apps don't use the schema name. For back-compat, don't enforce name.

            return new UploadSchema(key, fieldNameList, fieldTypeMap, name);
        }
    }
}
