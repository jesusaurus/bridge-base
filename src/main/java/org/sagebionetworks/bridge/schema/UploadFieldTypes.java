package org.sagebionetworks.bridge.schema;

/** Utility class that contains constants for Upload field types. */
// TODO: Make this into an enum and consolidate it with the corresponding BridgePF class.
@SuppressWarnings("unused")
public class UploadFieldTypes {
    public static final String ATTACHMENT_BLOB = "ATTACHMENT_BLOB";
    public static final String ATTACHMENT_CSV = "ATTACHMENT_CSV";
    public static final String ATTACHMENT_JSON_BLOB = "ATTACHMENT_JSON_BLOB";
    public static final String ATTACHMENT_JSON_TABLE = "ATTACHMENT_JSON_TABLE";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String CALENDAR_DATE = "CALENDAR_DATE";
    public static final String FLOAT = "FLOAT";
    public static final String INLINE_JSON_BLOB = "INLINE_JSON_BLOB";
    public static final String INT = "INT";
    public static final String STRING = "STRING";
    public static final String TIMESTAMP = "TIMESTAMP";
}
