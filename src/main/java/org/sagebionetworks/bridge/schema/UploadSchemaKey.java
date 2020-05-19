package org.sagebionetworks.bridge.schema;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;

/** This class represents an upload schema key, with a app ID, schema ID, and revision. */
@JsonDeserialize(builder = UploadSchemaKey.Builder.class)
public final class UploadSchemaKey {
    private final String appId;
    private final String schemaId;
    private final int revision;

    /** Private constructor. To construct, use Builder. */
    private UploadSchemaKey(String appId, String schemaId, int revision) {
        this.appId = appId;
        this.schemaId = schemaId;
        this.revision = revision;
    }

    /** ID of the app this schema lives in. */
    public String getAppId() {
        return appId;
    }
    
    /** ID of the schema. */
    public String getSchemaId() {
        return schemaId;
    }

    /** Revision number of the schema. */
    public int getRevision() {
        return revision;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UploadSchemaKey schemaKey = (UploadSchemaKey) o;
        return Objects.equal(revision, schemaKey.revision) &&
                Objects.equal(appId, schemaKey.appId) &&
                Objects.equal(schemaId, schemaKey.schemaId);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hashCode(appId, schemaId, revision);
    }

    /**
     * Returns the string representation of the schema, which is "[appId]-[schemaId]-v[revision]". For example:
     * parkinson-Voice Activity-v1
     */
    @Override
    public String toString() {
        return appId + "-" + schemaId + "-v" + revision;
    }

    /** Builder for an UploadSchemaKey. */
    public static class Builder {
        private String appId;
        private String schemaId;
        private Integer revision;

        /** @see UploadSchemaKey#getAppId */
        @JsonAlias("studyId")
        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }
        
        /** @see UploadSchemaKey#getSchemaId */
        public Builder withSchemaId(String schemaId) {
            this.schemaId = schemaId;
            return this;
        }

        /** @see UploadSchemaKey#getRevision */
        public Builder withRevision(Integer revision) {
            this.revision = revision;
            return this;
        }

        /** Builds an UploadSchemaKey and validate that all fields are specified and that revision is positive. */
        public UploadSchemaKey build() {
            if (StringUtils.isBlank(appId)) {
                throw new IllegalStateException("appId must be specified");
            }

            if (StringUtils.isBlank(schemaId)) {
                throw new IllegalStateException("schemaId must be specified");
            }

            // Zero rev is only meaningful when we are creating a new schema for the first time. Since that never
            // happens here, we reject zero rev.
            if (revision == null || revision <= 0) {
                throw new IllegalStateException("revision must be specified and positive");
            }

            return new UploadSchemaKey(appId, schemaId, revision);
        }
    }
}
