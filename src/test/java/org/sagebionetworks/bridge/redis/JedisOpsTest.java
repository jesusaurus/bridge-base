package org.sagebionetworks.bridge.redis;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisOpsTest {

    private Jedis jedis;
    private JedisOps ops;

    @Before
    public void before() {
        jedis = mock(Jedis.class);
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        ops = new JedisOps(pool);
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
}
