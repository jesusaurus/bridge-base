package org.sagebionetworks.bridge.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PropertiesConfigTest {

    private Path configFile;

    @Before
    public void before() throws URISyntaxException {
        configFile = Paths.get(getClass().getClassLoader().getResource("conf/bridge.conf").getPath());
    }

    @After
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

    @Test(expected=InvalidEnvironmentException.class)
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
}
