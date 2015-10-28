package org.sagebionetworks.bridge.redis;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisOpsTest {

    private Jedis jedis;
    private JedisOps ops;

    @BeforeMethod
    public void before() {
        jedis = mock(Jedis.class);
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        ops = new JedisOps(pool);
    }

    @Test
    public void testExpire() {
        ops.expire("key", 10);
        verify(jedis, times(1)).expire("key", 10);
    }

    @Test
    public void testSetex() {
        ops.setex("key", 5, "val");
        verify(jedis, times(1)).setex("key", 5, "val");
    }

    @Test
    public void testSetnx() {
        ops.setnx("key", "val");
        verify(jedis, times(1)).setnx("key", "val");
    }

    @Test
    public void testGet() {
        ops.get("key");
        verify(jedis, times(1)).get("key");
    }

    @Test
    public void testDel() {
        ops.del("key1", "key2");
        verify(jedis, times(1)).del("key1", "key2");
    }

    @Test
    public void testTtl() {
        ops.ttl("key");
        verify(jedis, times(1)).ttl("key");
    }

    @Test
    public void testIncr() {
        ops.incr("key");
        verify(jedis, times(1)).incr("key");
    }

    @Test
    public void testDecr() {
        ops.decr("key");
        verify(jedis, times(1)).decr("key");
    }

    @Test
    public void testZadd() {
        ops.zadd("key", 1.1, "member");
        verify(jedis, times(1)).zadd("key", 1.1, "member");
    }

    @Test
    public void testZscore() {
        ops.zscore("key", "member");
        verify(jedis, times(1)).zscore("key", "member");
    }

    @Test
    public void testZrank() {
        ops.zrank("key", "member");
        verify(jedis, times(1)).zrank("key", "member");
    }

    @Test
    public void testZrangeByScore() {
        ops.zrangeByScore("key", 1.1, 1.2);
        verify(jedis, times(1)).zrangeByScore("key", 1.1, 1.2);
    }

    @Test
    public void testZrem() {
        ops.zrem("key", "m1", "m2");
        verify(jedis, times(1)).zrem("key", "m1", "m2");
    }
}
