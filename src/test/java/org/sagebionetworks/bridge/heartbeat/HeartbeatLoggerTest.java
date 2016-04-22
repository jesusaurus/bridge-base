package org.sagebionetworks.bridge.heartbeat;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.testng.annotations.Test;

public class HeartbeatLoggerTest {
    @Test
    public void test() {
        HeartbeatLogger heartbeatLogger = spy(new HeartbeatLogger());

        // set interval to 0 for tests
        heartbeatLogger.setIntervalMinutes(0);

        // spy shouldKeepRunning() to run for 3 iterations
        doReturn(true).doReturn(true).doReturn(true).doReturn(false).when(heartbeatLogger).shouldKeepRunning();

        // execute
        heartbeatLogger.run();

        // verify we logged 3 times
        verify(heartbeatLogger, times(3)).logHeartbeat();
    }
}
