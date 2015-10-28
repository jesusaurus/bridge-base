package org.sagebionetworks.bridge.redis;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.mockito.InOrder;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.lock.LockNotAvailableException;

public class RedisLockTest {
    @Test
    public void testAcquireLock() {
        JedisOps ops = mock(JedisOps.class);
        // 1 -- indicates lock acquired
        when(ops.setnx(eq("key"), anyString())).thenReturn(1L);
        when(ops.expire("key", 10)).thenReturn(1L);
        RedisLock lock = new RedisLock(ops);
        lock.acquireLock("key", 10);
        InOrder inOrder = inOrder(ops);
        inOrder.verify(ops, times(1)).setnx(eq("key"), anyString());
        inOrder.verify(ops, times(1)).expire("key", 10);
    }

    @Test(expectedExceptions = RedisException.class)
    public void testAcquireLockWithRedisException() {
        JedisOps ops = mock(JedisOps.class);
        // 1 -- indicates lock acquired
        when(ops.setnx(eq("key"), anyString())).thenReturn(1L);
        // 0 -- fails to expire the lock
        when(ops.expire("key", 10)).thenReturn(0L);
        RedisLock lock = new RedisLock(ops);
        lock.acquireLock("key", 10);
    }

    @Test
    public void testAcquireLockWithLockNotAvailableException() {
        JedisOps ops = mock(JedisOps.class);
        // 0 -- indicates lock acquiring failed
        when(ops.setnx(eq("key"), anyString())).thenReturn(0L);
        // -1 -- lock has no associated expire
        when(ops.ttl("key")).thenReturn(-1L);
        when(ops.expire("key", 10)).thenReturn(1L);
        RedisLock lock = new RedisLock(ops);
        LockNotAvailableException expected = null;
        try {
            lock.acquireLock("key", 10);
            fail("LockNotAvailableException expected.");
        } catch (LockNotAvailableException e) {
            expected = e;
        }
        assertNotNull(expected);
        InOrder inOrder = inOrder(ops);
        inOrder.verify(ops, times(1)).setnx(eq("key"), anyString());
        inOrder.verify(ops, times(1)).expire("key", 10);
    }

    @Test
    public void testReleaseLockSucceeded() {
        JedisOps ops = mock(JedisOps.class);
        when(ops.get("key")).thenReturn("lock");
        when(ops.del("key")).thenReturn(1L);
        RedisLock lock = new RedisLock(ops);
        assertTrue(lock.releaseLock("key", "lock"));
        InOrder inOrder = inOrder(ops);
        inOrder.verify(ops, times(1)).get("key");
        inOrder.verify(ops, times(1)).del("key");
    }

    @Test
    public void testReleaseLockFailed() {
        JedisOps ops = mock(JedisOps.class);
        when(ops.get("key")).thenReturn("lock");
        when(ops.del("key")).thenReturn(1L);
        RedisLock lock = new RedisLock(ops);
        assertFalse(lock.releaseLock("key", "lockNotOwnedByMe"));
        verify(ops, never()).del("key");
    }

    @Test(expectedExceptions = RedisException.class)
    public void testReleaseLockWithRedisException() {
        JedisOps ops = mock(JedisOps.class);
        when(ops.get("key")).thenReturn("lock");
        when(ops.del("key")).thenReturn(0L);
        RedisLock lock = new RedisLock(ops);
        lock.releaseLock("key", "lock");
    }
}
