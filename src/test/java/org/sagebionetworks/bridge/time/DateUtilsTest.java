package org.sagebionetworks.bridge.time;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

@SuppressWarnings("ConstantConditions")
public class DateUtilsTest {

    // The test date is: either "2014-08-11T16:01:23.817Z" or 1407772883817 in milliseconds
    private static final long MILLIS = 1407772883817L;

    private static final String ISO_DATE_TIME = "2014-08-11T16:01:23.817Z";
    private static final long SIX_DAYS_IN_MILLIS = 6*24*60*60*1000;

    private DateTime getDateTime() {
        return new DateTime(MILLIS, DateTimeZone.UTC);
    }

    @Test
    public void getDurationFromMillis() {
        String duration = DateUtils.convertToDuration(SIX_DAYS_IN_MILLIS);
        assertEquals(duration, "PT144H", "Comes out as 144 hrs");
    }

    @Test
    public void getMillisFromDuration() {
        long millis = DateUtils.convertToMillisFromDuration("PT144H");
        assertEquals(millis, SIX_DAYS_IN_MILLIS, "Comes out as six days");
    }

    @Test
    public void getCalendarDateString() {
        LocalDate date = new LocalDate(2014, 2, 16);
        assertEquals(DateUtils.getCalendarDateString(date), "2014-02-16");
    }

    @Test
    public void parseCalendarDate() {
        LocalDate date = DateUtils.parseCalendarDate("2014-02-16");
        assertEquals(date.getYear(), 2014);
        assertEquals(date.getMonthOfYear(), 2);
        assertEquals(date.getDayOfMonth(), 16);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void parseCalendarDateMalformattedString() {
        DateUtils.parseCalendarDate("February 16, 2014");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void parseCalendarDateShortInt() {
        // Make sure we don't do something dumb, like parse this as year 42.
        DateUtils.parseCalendarDate("42");
    }

    @Test
    public void getISODateTime() {
        String dateString = DateUtils.getISODateTime(getDateTime());
        assertEquals(dateString, ISO_DATE_TIME, "Datetime is correctly formatted");
    }

    @Test
    public void parseISODateTime() {
        // { expected, input }
        Object[][] testCaseArray = {
                { new DateTime(2014, 2, 17, 23, 0, DateTimeZone.UTC), "2014-02-17T23:00Z" },
                { new DateTime(2014, 2, 17, 23, 0, DateTimeZone.forOffsetHours(-8)), "2014-02-17T23:00-0800" },
                { new DateTime(2014, 2, 17, 23, 0, DateTimeZone.forOffsetHours(+9)), "2014-02-17T23:00+0900" },
        };

        for (Object[] oneTestCase : testCaseArray) {
            String input = (String) oneTestCase[1];
            DateTime actual = DateUtils.parseISODateTime(input);
            assertEquals(actual, oneTestCase[0], "Datetime is correctly formatted");
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void parseISODateTimeMalformattedString() {
        DateUtils.parseISODateTime("February 17, 2014 at 11:00:00pm");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void parseISODateTimeShortInt() {
        // Make sure we don't do something dumb, like parse this as year 42.
        DateUtils.parseISODateTime("42");
    }

    @Test
    public void convertToMillisFromDate() {
        long expectedMillis = new DateTime(2014, 2, 17, 0, 0, DateTimeZone.UTC).getMillis();

        long millis = DateUtils.convertToMillisFromEpoch("2014-02-17");
        assertEquals(millis, expectedMillis);
    }

    @Test
    public void convertToMillisFromDateTime() {
        // Arbitrarily 2014-02-17T23:00Z.
        long expectedMillis = new DateTime(2014, 2, 17, 23, 0, DateTimeZone.UTC).getMillis();

        long millis = DateUtils.convertToMillisFromEpoch("2014-02-17T23:00Z");
        assertEquals(millis, expectedMillis);
    }

    @Test
    public void convertToMillisFromDateTimeNonUtc() {
        // Arbitrarily 2014-02-17T23:00-0800. We want to use a timezone other than UTC to make sure we can handle
        // non-UTC timezones.
        long expectedMillis = new DateTime(2014, 2, 17, 23, 0, DateUtils.LOCAL_TIME_ZONE)
                .getMillis();

        long millis = DateUtils.convertToMillisFromEpoch("2014-02-17T23:00-0800");
        assertEquals(millis, expectedMillis);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void convertToMillisFromMalformattedDate() {
        // We detect dates as being 10 chars long.
        DateUtils.convertToMillisFromEpoch("1234567890");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void convertToMillisFromMalformattedString() {
        DateUtils.convertToMillisFromEpoch("February 17, 2014 at 11:00:00pm");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void convertToMillisFromShortInt() {
        // Make sure we don't do something dumb, like parse this as year 42.
        DateUtils.convertToMillisFromEpoch("42");
    }

    @Test
    public void canParseTimeISOZoneString() {
        DateTimeZone compareTo = DateTimeZone.forOffsetHours(-7);
        DateTimeZone zone = DateUtils.parseZoneFromOffsetString("-07:00");

        assertEquals(zone, compareTo);
    }

    @Test
    public void canParseHoursOnlyAbbreviation() {
        DateTimeZone compareTo = DateTimeZone.forOffsetHours(7);
        DateTimeZone zone = DateUtils.parseZoneFromOffsetString("7");
        assertEquals(zone, compareTo);

        zone = DateUtils.parseZoneFromOffsetString("+7");
        assertEquals(zone, compareTo);
    }

    @Test
    public void canParseSpecialValueUTC() {
        // When you parse an offset of 0 and get a time zone, that time zone expressed as a string
        // will be "UTC", not "+00:00". The parser should correctly handle this special case.
        DateTimeZone zone = DateUtils.parseZoneFromOffsetString("UTC");
        assertEquals(zone, DateTimeZone.UTC);

        zone = DateUtils.parseZoneFromOffsetString("+00:00");
        assertEquals(zone, DateTimeZone.UTC);
    }

    @Test
    public void emptyOrNullReturnNull() {
        DateTimeZone zone = DateUtils.parseZoneFromOffsetString(null);
        assertNull(zone);

        zone = DateUtils.parseZoneFromOffsetString("");
        assertNull(zone);

        zone = DateUtils.parseZoneFromOffsetString(" ");
        assertNull(zone);
    }

    @Test
    public void anyDeviationThrowsCorrectException() {
        try {
            DateUtils.parseZoneFromOffsetString("Z");
            fail("Should have thrown exception");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Cannot not parse timezone offset 'Z' (use format ±HH:MM)");
        }
    }

    @Test
    public void canParsePositiveHourValue() {
        DateTimeZone zone = DateUtils.parseZoneFromOffsetString("+00:00");
        assertEquals(zone, DateTimeZone.UTC);
    }

    @Test
    public void getDateTimeOrDefaultReturnsValidValue() {
        String dateTimeString = "2016-07-15T10:10:10.000Z";

        DateTime parsedValue = DateUtils.getDateTimeOrDefault(dateTimeString, null);
        assertEquals(parsedValue, DateTime.parse(dateTimeString));
    }

    @Test
    public void canParseHalfOffsets() {
        DateTimeZone timeZone = DateUtils.parseZoneFromOffsetString("-07:30");
        assertEquals(timeZone.toString(), "-07:30");

        timeZone = DateUtils.parseZoneFromOffsetString("+05:45");
        assertEquals(timeZone.toString(), "+05:45");
    }

    @Test
    public void getDateTimeOrDefaultReturnsDefaultOnNull() {
        DateTime timestamp = DateTime.now();

        DateTime parsedValue = DateUtils.getDateTimeOrDefault(null, timestamp);
        assertEquals(parsedValue, timestamp);
    }

    @Test
    public void getDateTimeOrDefaultReturnsDefaultOnEmptyString() {
        DateTime timestamp = DateTime.now();

        DateTime parsedValue = DateUtils.getDateTimeOrDefault("", timestamp);
        assertEquals(parsedValue, timestamp);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void getDateTimeOrDefaultThrowsException() {
        DateUtils.getDateTimeOrDefault("6/7/2016", null);
    }

    @Test
    public void testTimeZoneToOffsetString() {
        assertEquals(DateUtils.timeZoneToOffsetString(DateTimeZone.forOffsetHours(0)), "+00:00");
        assertEquals(DateUtils.timeZoneToOffsetString(DateTimeZone.UTC), "+00:00");
        assertEquals(DateUtils.timeZoneToOffsetString(DateTimeZone.forOffsetHours(7)), "+07:00");
        assertEquals(DateUtils.timeZoneToOffsetString(DateTimeZone.forOffsetHours(-5)), "-05:00");
        assertEquals(DateUtils.timeZoneToOffsetString(DateTimeZone.forOffsetHoursMinutes(-5, 30)), "-05:30");
        assertEquals(DateUtils.timeZoneToOffsetString(DateTimeZone.forOffsetHoursMinutes(2, 30)), "+02:30");
    }

    // This method is dealing with local time, but we're casting to UTC so that 
    // the one time tasks that call this are forced to a common day (this day 
    // or maybe the day before... this won't matter for one-time task or persistent 
    // tasks unless they expire, but we prevent a short expiration window). Then if 
    // the user moves across the datetime boundary, they won't get another task.
    @Test
    public void eventToPriorUTCMidnight() {
        DateTime dateTime = DateTime.parse("2016-11-06T16:32.123-07:00");
        DateTime midnight = DateUtils.dateTimeToMidnightUTC(dateTime);
        assertEquals(midnight.toString(), "2016-11-06T00:00:00.000Z");

        dateTime = DateTime.parse("2016-11-07T02:32.123+03:00");
        midnight = DateUtils.dateTimeToMidnightUTC(dateTime);
        assertEquals(midnight.toString(), "2016-11-06T00:00:00.000Z");
    }
}
