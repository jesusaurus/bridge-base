package org.sagebionetworks.bridge.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * In-memory mock implementation of Jedis. Uses a map to store everything doesn't actually honor TTL or expiration
 * times.
 */
public class InMemoryJedisOps extends JedisOps {
    private final Map<String, String> map = new HashMap<>();

    public InMemoryJedisOps() {
        // We're mocking out everything, so we don't care about the parent constructor. The super() call is only here
        // because javac requires it.
        super(null);
    }

    @Override
    public Long expire(String key, int seconds) {
        if (seconds <= 0) {
            map.remove(key);
        }
        return 1L;
    }

    @Override
    public String setex(String key, int seconds, String value) {
        // Contrary to what the parent class documentation says, this does NOT return the value set, but rather "OK"
        map.put(key, value);
        return "OK";
    }

    @Override
    public Long setnx(String key, String value) {
        if (map.containsKey(key)) {
            return 0L;
        } else {
            map.put(key, value);
            return 1L;
        }
    }

    @Override
    public String get(String key) {
        return map.get(key);
    }

    @Override
    public Long del(String... keyVarargs) {
        for (String oneKey : keyVarargs) {
            map.remove(oneKey);
        }
        return (long) keyVarargs.length;
    }

    @Override
    public Long ttl(String key) {
        if (map.containsKey(key)) {
            // We don't honor TTLs. Just return 1;
            return 1L;
        } else {
            return 0L;
        }
    }

    private long getAsLong(String key) {
        // The expected behavior is to treat null values as zero.
        String str = map.get(key);
        if (str == null) {
            return 0;
        } else {
            return Long.parseLong(str);
        }
    }

    @Override
    public Long incr(String key) {
        long oldValue = getAsLong(key);
        long newValue = oldValue + 1;
        map.put(key, Long.toString(newValue));
        return newValue;
    }

    @Override
    public Long decr(String key) {
        long oldValue = getAsLong(key);
        long newValue = oldValue - 1;
        map.put(key, Long.toString(newValue));
        return newValue;
    }

    @Override
    public Long zadd(String key, double score, String member) {
        // not used by BridgePF
        throw new UnsupportedOperationException("Unsupported operation zadd()");
    }

    @Override
    public Set<String> zrangeByScore(String key, Double min, Double max) {
        // not used by BridgePF
        throw new UnsupportedOperationException("Unsupported operation zrangeByScore()");
    }

    @Override
    public Long zrank(String key, String member) {
        // not used by BridgePF
        throw new UnsupportedOperationException("Unsupported operation zrank()");
    }

    @Override
    public Double zscore(String key, String member) {
        // not used by BridgePF
        throw new UnsupportedOperationException("Unsupported operation zscore()");
    }

    @Override
    public Long zrem(String key, String... members) {
        // not used by BridgePF
        throw new UnsupportedOperationException("Unsupported operation zrem()");
    }

    @Override
    public JedisTransaction getTransaction(String... keys) {
        return new InMemoryJedisTransaction(this);
    }
}
