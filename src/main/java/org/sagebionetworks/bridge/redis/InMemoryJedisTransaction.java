package org.sagebionetworks.bridge.redis;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;

/**
 * In-memory mock implementation of a JedisTransaction. Wraps around an InMemoryJedisOps. Doesn't actually honor
 * transactionality.
 */
public class InMemoryJedisTransaction extends JedisTransaction {
    private final InMemoryJedisOps ops;
    private int numOps = 0;

    public InMemoryJedisTransaction(InMemoryJedisOps ops) {
        // We need to pass in *something*, otherwise this causes a NullPointerException. However, we don't care what it
        // is, so just pass in a Mockito mock.
        super(mock(Jedis.class));
        this.ops = ops;
    }

    @Override
    public JedisTransaction setex(String key, int seconds, String value) {
        ops.setex(key, seconds, value);
        numOps++;
        return this;
    }

    @Override
    public JedisTransaction expire(String key, int seconds) {
        ops.expire(key, seconds);
        numOps++;
        return this;
    }

    @Override public JedisTransaction del(String key) {
        ops.del(key);
        numOps++;
        return this;
    }

    @Override
    public List<Object> exec() {
        // no-op, since we don't support transactionality
        List<Object> resultList = new ArrayList<>();
        for (int i = 0; i < numOps; i++) {
            resultList.add("OK");
        }
        return resultList;
    }

    @Override public String discard() {
        // no-op, since we don't support transactionality
        return "OK";
    }

    @Override public void close() {
        // no-op, since we don't suport transactionality
    }
}
