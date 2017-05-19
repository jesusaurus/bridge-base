package org.sagebionetworks.bridge.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PropertiesConfigTest {

    private Path configFile;
    private static final String TEST_CONF_FILE = "conf/bridge.conf";

    @BeforeMethod
    public void before() throws URISyntaxException {
        configFile = Paths.get(getClass().getClassLoader().getResource("conf/bridge.conf").toURI());
    }

    @AfterMethod
    public void after() {
        System.clearProperty(PropertiesConfig.USER_KEY);
        System.clearProperty(PropertiesConfig.ENV_KEY);
    }

    @Test
    public void testGetUser() throws IOException {
        System.setProperty(PropertiesConfig.USER_KEY, "testGetUser");
        Config config = new PropertiesConfig(configFile);
        assertEquals("testGetUser", config.getUser());
    }

    @Test
    public void testGetDefaultUser() throws IOException {
        Config config = new PropertiesConfig(configFile);
        assertEquals(PropertiesConfig.DEFAULT_USER, config.getUser());
    }

    @Test
    public void testGetEnvironment() throws IOException {
        System.setProperty(PropertiesConfig.ENV_KEY, "dev");
        Config config = new PropertiesConfig(configFile);
        assertEquals(Environment.DEV, config.getEnvironment());
    }

    @Test
    public void testGetDefaultEnvironment() throws IOException {
        Config config = new PropertiesConfig(configFile);
        assertEquals(PropertiesConfig.DEFAULT_ENV, config.getEnvironment());
    }

    @Test(expectedExceptions=InvalidEnvironmentException.class)
    public void testGetInvalidEnvironment() throws IOException {
        System.setProperty(PropertiesConfig.ENV_KEY, "InvalidEnvironment");
        new PropertiesConfig(configFile);
    }

    @Test
    public void testGet() throws IOException {
        Config config = new PropertiesConfig(configFile);
        assertEquals("example.value", config.get("example.property"));
    }

    @Test
    public void testGetEnvSpecific() throws IOException {
        System.setProperty(PropertiesConfig.ENV_KEY, "dev");
        Config config = new PropertiesConfig(configFile);
        assertEquals("example.value.for.dev", config.get("example.property"));
    }

    @Test
    public void testGetNull() throws IOException {
        Config config = new PropertiesConfig(configFile);
        assertNull(config.get("someFakePropertyThatDoesNotExist"));
    }

    @Test
    public void testGetInt() throws IOException {
        Config config = new PropertiesConfig(configFile, configFile);
        assertEquals(2000, config.getInt("example.timeout"));
    }

    @Test
    public void testGetUserFromString() throws IOException {
        System.setProperty(PropertiesConfig.USER_KEY, "testGetUser");
        Config config = new PropertiesConfig(TEST_CONF_FILE);
        assertEquals("testGetUser", config.getUser());
    }

    @Test
    public void testGetDefaultUserFromString() throws IOException {
        Config config = new PropertiesConfig(TEST_CONF_FILE);
        assertEquals(PropertiesConfig.DEFAULT_USER, config.getUser());
    }

    @Test
    public void testGetEnvironmentFromString() throws IOException {
        System.setProperty(PropertiesConfig.ENV_KEY, "dev");
        Config config = new PropertiesConfig(TEST_CONF_FILE);
        assertEquals(Environment.DEV, config.getEnvironment());
    }

    @Test
    public void testGetDefaultEnvironmentFromString() throws IOException {
        Config config = new PropertiesConfig(TEST_CONF_FILE);
        assertEquals(PropertiesConfig.DEFAULT_ENV, config.getEnvironment());
    }

    @Test(expectedExceptions=InvalidEnvironmentException.class)
    public void testGetInvalidEnvironmentFromString() throws IOException {
        System.setProperty(PropertiesConfig.ENV_KEY, "InvalidEnvironment");
        new PropertiesConfig(TEST_CONF_FILE);
    }

    @Test
    public void testGetFromString() throws IOException {
        Config config = new PropertiesConfig(TEST_CONF_FILE);
        assertEquals("example.value", config.get("example.property"));
    }

    @Test
    public void testGetEnvSpecificFromString() throws IOException {
        System.setProperty(PropertiesConfig.ENV_KEY, "dev");
        Config config = new PropertiesConfig(TEST_CONF_FILE);
        assertEquals("example.value.for.dev", config.get("example.property"));
    }

    @Test
    public void testGetNullFromString() throws IOException {
        Config config = new PropertiesConfig(TEST_CONF_FILE);
        assertNull(config.get("someFakePropertyThatDoesNotExist"));
    }

    @Test
    public void testGetIntFromStringPath() throws IOException {
        Config config = new PropertiesConfig(TEST_CONF_FILE, configFile);
        assertEquals(2000, config.getInt("example.timeout"));
    }

    @Test
    public void testGetList() throws IOException {
        Config config = new PropertiesConfig(configFile, configFile);
        List<String> list = config.getList("example.property");
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals("example.value", list.get(0));
        list = config.getList("example.list");
        assertNotNull(list);
        assertEquals(4, list.size());
        assertEquals("a", list.get(0));
        assertEquals("bc", list.get(1));
        assertEquals("d", list.get(2));
        assertEquals("e", list.get(3));
    }

    @Test
    public void testGetListFromStringPath() throws IOException {
        Config config = new PropertiesConfig(TEST_CONF_FILE, configFile);
        List<String> list = config.getList("example.property");
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals("example.value", list.get(0));
        list = config.getList("example.list");
        assertNotNull(list);
        assertEquals(4, list.size());
        assertEquals("a", list.get(0));
        assertEquals("bc", list.get(1));
        assertEquals("d", list.get(2));
        assertEquals("e", list.get(3));
    }

    @Test
    public void testOverwriteFromString() throws IOException, URISyntaxException {
        Path localPath = Paths.get(getClass().getClassLoader().getResource("conf/local.conf").toURI());
        System.setProperty(PropertiesConfig.ENV_KEY, "dev");
        Config config = new PropertiesConfig(TEST_CONF_FILE, localPath);
        assertEquals("local.value.for.dev", config.get("example.property"));
    }
}
