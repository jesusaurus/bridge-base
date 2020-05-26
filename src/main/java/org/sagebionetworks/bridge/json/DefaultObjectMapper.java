package org.sagebionetworks.bridge.json;

import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The only thing this class provides is a singleton instance of the Jackson JSON Object Mapper. We use a constant
 * (public static final) field in a utility class because Spring is too heavy-weight for a Jackson object mapper, and
 * creating a new one for each class or project is overkill.
 */
public class DefaultObjectMapper {
    /** Jackson Object Mapper with default settings. */
    public static final ObjectMapper INSTANCE = new ObjectMapper();
    static {
        INSTANCE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /** Jackson type reference to deserialize a JSON struct as a raw map. */
    public static final TypeReference<Map<String, Object>> TYPE_REF_RAW_MAP =
            new TypeReference<Map<String, Object>>(){};
}
