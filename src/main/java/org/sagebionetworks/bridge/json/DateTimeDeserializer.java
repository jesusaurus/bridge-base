package org.sagebionetworks.bridge.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.joda.time.DateTime;

/**
 * Custom deserializer for Joda DateTime. The deserializer in jackson-datatype-joda doesn't work, and the Joda Module
 * ignores time zone.
 */
public class DateTimeDeserializer extends JsonDeserializer<DateTime> {
    /** {@inheritDoc} */
    @Override
    public DateTime deserialize(JsonParser jp, DeserializationContext dc) throws IOException {
        String date = jp.getText();
        return DateTime.parse(date);
    }
}
