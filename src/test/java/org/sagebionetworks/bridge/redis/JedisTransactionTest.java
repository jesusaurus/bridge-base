package org.sagebionetworks.bridge.redis;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.mockito.InOrder;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class JedisTransactionTest {
    @Test
    public void test() {
        Transaction transaction = mock(Transaction.class);
        InOrder inOrder = inOrder(transaction);
        Jedis jedis = mock(Jedis.class);
        when(jedis.multi()).thenReturn(transaction);
        try (JedisTransaction jt = new JedisTransaction(jedis)) {
            jt.setex("k1", 10, "v1").expire("k1", 15).del("k2").exec();
            inOrder.verify(transaction, times(1)).setex("k1", 10, "v1");
            inOrder.verify(transaction, times(1)).expire("k1", 15);
            inOrder.verify(transaction, times(1)).del("k2");
            inOrder.verify(transaction, times(1)).exec();
        }
    }
}
