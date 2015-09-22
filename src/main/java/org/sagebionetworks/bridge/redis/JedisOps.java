package org.sagebionetworks.bridge.redis;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * A thin wrapper of <code>Jedis</code>. Provides a template
 * that obtains a <code>Jedis</code> instance from a pool,
 * executes a command on the <code>Jedis</code> instance, and
 * returns the <code>Jedis</code> instance to the pool after
 * the command execution.
 */
public class JedisOps {

    private final JedisPool jedisPool;

    public JedisOps(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * The specified key will expire after seconds.
     *
     * @param key
     *            target key.
     * @param seconds
     *            number of seconds until expiration.
     * @return success code
     *          1 if successful, 0 if key doesn't exist or timeout could not be set
     */
    public Long expire(final String key, final int seconds) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.expire(key, seconds);
            }
        }.execute();
    }

    /**
     * Sets the value of the key and makes it expire after the specified
     * seconds.
     *
     * @param key
     *            key of the key-value pair.
     * @param seconds
     *            number of seconds until expiration.
     * @param value
     *            value of the key-value pair.
     */
    public String setex(final String key, final int seconds, final String value) {
        return new AbstractJedisTemplate<String>() {
            @Override
            String execute(Jedis jedis) {
                return jedis.setex(key, seconds, value);
            }
        }.execute();
    }

    /**
     * Sets the value of the key if and only if the key does not already have a
     * value.
     *
     * @param key
     *            key of the key-value pair.
     * @param value
     *            value of the key-value pair.
     * @return success code
     *          1 if the key was set, 0 if not
     */
    public Long setnx(final String key, final String value) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.setnx(key, value);
            }
        }.execute();
    }

    /**
     * Gets the value of the specified key. If the key does not exist null is
     * returned.
     */
    public String get(final String key) {
        return new AbstractJedisTemplate<String>() {
            @Override
            String execute(Jedis jedis) {
                return jedis.get(key);
            }
        }.execute();
    }

    /**
     * Deletes the specified list of keys.
     *
     * @param keys
     *            the list of keys to be deleted.
     * @return number of keys deleted
     */
    public Long del(final String... keys) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.del(keys);
            }
        }.execute();
    }

    /**
     * Determines the time until expiration for a key (time-to-live).
     *
     * @param key
     *            key of the key-value pair.
     * @return ttl
     *      positive value if ttl is set, zero if not, negative if there was an error
     */
    public Long ttl(final String key) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.ttl(key);
            }
        }.execute();
    }

    /**
     * Increments the value by one.
     *
     * @return the new value of the key after incrementing.
     */
    public Long incr(final String key) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.incr(key);
            }
        }.execute();
    }

    /**
     * Decrements the value by one.
     *
     * @return the new value of the key after decrementing.
     */
    public Long decr(final String key) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.decr(key);
            }
        }.execute();
    }

    // Sorted set
    /**
     * Adds a member with a score to the sorted set. Time complexity is O(log(N)).
     */
    public Long zadd(final String key, final double score, final String member) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.zadd(key, score, member);
            }
        }.execute();
    }

    /**
     * Returns all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max). Time
     * complexity is O(log(N)+M) with N being the number of elements in the
     * sorted set and M the number of elements being returned.
     */
    public Set<String> zrangeByScore(final String key, final Double min, final Double max) {
        return new AbstractJedisTemplate<Set<String>>() {
            @Override
            Set<String> execute(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        }.execute();
    }

    /**
     * Returns the rank of member in the sorted set stored at key.
     * The rank (or index) is 0-based. Time complexity is O(log(N)).
     *
     * @return 0-based rank or Double.NaN if the member does not exist.
     */
    public Long zrank(final String key, final String member) {
        return new AbstractJedisTemplate<Long>() {
            @Override
            Long execute(Jedis jedis) {
                return jedis.zrank(key, member);
            }
        }.execute();
    }

    /**
     * Returns the score of the member in the sorted set at key. Time
     * complexity is O(1).
     *
     * @return the score of the member or Double.NaN if the member does not exist.
     */
    public Double zscore(final String key, final String member) {
        return new AbstractJedisTemplate<Double>() {
            @Override
            Double execute(Jedis jedis) {
                return jedis.zscore(key, member);
            }
        }.execute();
    }

    // Transaction
    /**
     * Starts a transaction with the optional list of keys to watch.
     *
     * @param keys
     *            The optional list of keys to watch.
     * @return The transaction object.
     */
    public JedisTransaction getTransaction(final String... keys) {
        final Jedis jedis = jedisPool.getResource();
        if (keys != null && keys.length > 0) {
            jedis.watch(keys);
        }
        return new JedisTransaction(jedis);
    }

    /**
     * Responsible for providing template code such as closing resources.
     */
    private abstract class AbstractJedisTemplate<T> {
        T execute() {
            try (Jedis jedis = jedisPool.getResource()) {
                return execute(jedis);
            }
        }
        abstract T execute(final Jedis jedis);
    }
}
