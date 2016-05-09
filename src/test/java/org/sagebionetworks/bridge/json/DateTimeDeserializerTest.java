package org.sagebionetworks.bridge.json;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonParser;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

public class DateTimeDeserializerTest {
    private static final String DATE_TIME_STRING = "2016-05-09T15:33:09.376-0700";
    private static final DateTime DATE_TIME = DateTime.parse(DATE_TIME_STRING);

    @Test
    public void test() throws Exception {
        // mock JsonParser
        JsonParser mockJP = mock(JsonParser.class);
        when(mockJP.getText()).thenReturn(DATE_TIME_STRING);

        // execute and validate
        DateTime result = new DateTimeDeserializer().deserialize(mockJP, null);
        assertEquals(result, DATE_TIME);
    }
}
