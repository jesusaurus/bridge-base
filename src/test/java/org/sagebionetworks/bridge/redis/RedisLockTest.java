package org.sagebionetworks.bridge.redis;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.InOrder;
import org.sagebionetworks.bridge.lock.LockNotAvailableException;

public class RedisLockTest {

    @Test
    public void testAcquireLock() {
        JedisOps ops = mock(JedisOps.class);
        // 1 -- indicates lock acquired
        when(ops.setnx(eq("key"), anyString())).thenReturn(1L);
        RedisLock lock = new RedisLock(ops);
        lock.acquireLock("key", 10);
        InOrder inOrder = inOrder(ops);
        inOrder.verify(ops, times(1)).setnx(eq("key"), anyString());
        inOrder.verify(ops, times(1)).expire("key", 10);
    }

    @Test
    public void testAcquireLockWithLockNotAvailableException() {
        JedisOps ops = mock(JedisOps.class);
        // 0 -- indicates lock acquiring failed
        when(ops.setnx(eq("key"), anyString())).thenReturn(0L);
        when(ops.ttl("key")).thenReturn(30L);
        RedisLock lock = new RedisLock(ops);
        try {
            lock.acquireLock("key", 10);
        } catch (LockNotAvailableException e) {
            assertTrue("LockNotAvailableException expected", true);
        }
        InOrder inOrder = inOrder(ops);
        inOrder.verify(ops, times(1)).setnx(eq("key"), anyString());
        inOrder.verify(ops, times(1)).expire("key", 10);
    }
}
