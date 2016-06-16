package org.sagebionetworks.bridge.schema;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchemaUtilsTest {
    @DataProvider(name = "sanitizeFieldNameDataProvider")
    public Object[][] sanitizeFieldNameDataProvider() {
        return new Object[][] {
                { "Passthrough1", "Passthrough1" },
                { "__lots__of__underscores__", "__lots__of__underscores__" },
                { "--lots--of--dashes--", "_-lots--of--dashes--" },
                { "..lots..of..dots..", "_.lots.of.dots._" },
                { "  lots  of  spaces  ", "_ lots  of  spaces _" },
                { "- .mix- .of- .chars- .", "_ .mix- .of- .chars- _" },
                { "!@replaces#$invalid%^chars&*", "__replaces__invalid__chars__" },
                { "replace\tnewlines\nand\rtabs", "replace_newlines_and_tabs" },
        };
    }

    @Test(dataProvider = "sanitizeFieldNameDataProvider")
    public void testSanitizeFieldName(String input, String expected) {
        String output = SchemaUtils.sanitizeFieldName(input);
        assertEquals(output, expected);
    }
}
