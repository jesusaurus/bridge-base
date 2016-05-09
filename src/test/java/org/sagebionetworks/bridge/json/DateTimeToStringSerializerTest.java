package org.sagebionetworks.bridge.json;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonGenerator;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class DateTimeToStringSerializerTest {
    private static final DateTime DATE_TIME = DateTime.parse("2016-05-09T15:33:09.376-0700");

    @Test
    public void test() throws Exception {
        // mock JsonGenerator
        JsonGenerator mockJGen = mock(JsonGenerator.class);

        // execute and validate
        new DateTimeToStringSerializer().serialize(DATE_TIME, mockJGen, null);
        ArgumentCaptor<String> arg = ArgumentCaptor.forClass(String.class);
        verify(mockJGen).writeString(arg.capture());
        assertEquals(DateTime.parse(arg.getValue()), DATE_TIME);
    }
}
