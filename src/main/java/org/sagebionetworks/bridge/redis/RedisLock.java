package org.sagebionetworks.bridge.redis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.sagebionetworks.bridge.lock.Lock;
import org.sagebionetworks.bridge.lock.LockNotAvailableException;

public class RedisLock implements Lock {

    private final JedisOps ops;

    public RedisLock(JedisOps ops) {
        checkNotNull(ops);
        this.ops = ops;
    }

    @Override
    public String acquireLock(final String key, final int expireInSeconds) {
        checkNotNull(key);
        checkArgument(expireInSeconds > 0);
        final String lock = UUID.randomUUID().toString();
        final Long result = ops.setnx(key, lock);
        if (result != 1L) {
            //
            // Force to expire the lock if not already set to expire.
            // The intention here is to avoid starvation when the lock is
            // not available but has no associated expiration. This can
            // happen when the code crashes after successfully calling setnx()
            // to acquire the lock but before calling expire().
            //
            // According to Redis:
            // Starting with Redis 2.8 the return value in case of error changed:
            //    The command returns -2 if the key does not exist.
            //    The command returns -1 if the key exists but has no associated expire.
            //
            if (ops.ttl(key) == -1L) {
                expire(key, expireInSeconds);
            }
            throw new LockNotAvailableException(key);
        }
        expire(key, expireInSeconds);
        return lock;
    }

    @Override
    public boolean releaseLock(final String key, final String lock) {
        checkNotNull(key);
        checkNotNull(lock);
        final String lockId = ops.get(key);
        if (!lock.equals(lockId)) {
            return false;
        }
        final Long result = ops.del(key);
        if (result == 0L) {
            throw new RedisException("Lock not released for " + key + ".");
        }
        return true;
    }

    private void expire(final String key, final int expireInSeconds) {
        final Long result = ops.expire(key, expireInSeconds);
        if (result != 1L) {
            throw new RedisException("Failed to expire key " + key + ".");
        }
    }
}
