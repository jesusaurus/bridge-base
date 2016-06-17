package org.sagebionetworks.bridge.schema;

import java.util.regex.Pattern;

import com.google.common.base.Strings;

/** Static utility class for Upload Schemas. */
public class SchemaUtils {
    private static final Pattern FIELD_NAME_DOT_REPLACEMENT_PATTERN = Pattern.compile("\\.{2,}");
    private static final Pattern FIELD_NAME_END_REPLACEMENT_PATTERN = Pattern.compile("[\\s\\.]$");
    private static final Pattern FIELD_NAME_INVALID_CHAR_REPLACEMENT_PATTERN = Pattern.compile("[^\\w\\-\\. ]");
    private static final Pattern FIELD_NAME_START_REPLACEMENT_PATTERN = Pattern.compile("^[\\s\\-\\.]");

    /**
     * <p>
     * Synapse column names have a few notable restrictions:
     * 1. Can't start with dash (-).
     * 2. Can't start or end with period (.), nor can you have two periods in a row.
     * 3. Field names are always trimmed.
     * 4. Only a specific set of characters are allowed. For simplicity, only accept alphanumeric chars, dashes (-),
     * periods (.), underscores (_), and spaces.
     * </p>
     * <p>
     * This method takes in a raw field name and runs it throgh the above rules and returns the sanitized field name.
     * This can be used for schema field names as well as multiple-choice values (which will be used for Synapse
     * column names, and hence the above rules apply).
     * </p>
     *
     * @param name
     *         field name to be sanitized
     * @return sanitized field name, or null if the input was null
     */
    public static String sanitizeFieldName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return name;
        }

        // If the first char is an invalid char (space, dash, dot), replace it with a _
        name = FIELD_NAME_START_REPLACEMENT_PATTERN.matcher(name).replaceFirst("_");

        // Similarly, if the last char is an in invalid char (space, dot, but not dash).
        name = FIELD_NAME_END_REPLACEMENT_PATTERN.matcher(name).replaceFirst("_");

        // Replace consecutive dots with a single dot.
        name = FIELD_NAME_DOT_REPLACEMENT_PATTERN.matcher(name).replaceAll(".");

        // Replace invalid chars with _
        name = FIELD_NAME_INVALID_CHAR_REPLACEMENT_PATTERN.matcher(name).replaceAll("_");

        return name;
    }
}
