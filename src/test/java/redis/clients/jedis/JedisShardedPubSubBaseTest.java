package redis.clients.jedis;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static redis.clients.jedis.Protocol.ResponseKeyword.SMESSAGE;
import static redis.clients.jedis.Protocol.ResponseKeyword.SSUBSCRIBE;
import static redis.clients.jedis.Protocol.ResponseKeyword.SUNSUBSCRIBE;

public class JedisShardedPubSubBaseTest {

    @Test
    public void testProceed_givenThreadInterrupt_exitLoop() throws InterruptedException {
        // setup
        final JedisShardedPubSubBase<String> pubSub = new JedisShardedPubSubBase<String>() {

            @Override
            public void onSMessage(String channel, String message) {
                fail("this should not happen when thread is interrupted");
            }

            @Override
            protected String encode(byte[] raw) {
                return new String(raw);
            }

        };

        final Connection mockConnection = mock(Connection.class);
        final List<Object> mockSubscribe = Arrays.asList(
                SSUBSCRIBE.getRaw(), "channel".getBytes(), 1L
        );
        final List<Object> mockResponse = Arrays.asList(
                SMESSAGE.getRaw(), "channel".getBytes(), "message".getBytes()
        );
        when(mockConnection.getUnflushedObject()).thenReturn(mockSubscribe, mockResponse);


        final CountDownLatch countDownLatch = new CountDownLatch(1);
        // action
        final Thread thread = new Thread(() -> {
            Thread.currentThread().interrupt();
            pubSub.proceed(mockConnection, "channel");

            countDownLatch.countDown();
        });
        thread.start();

        assertTrue(countDownLatch.await(100, TimeUnit.MILLISECONDS));

    }

    @Test
    public void testProceed_givenCallbackThrows_marksConnectionBroken() {
        final JedisShardedPubSubBase<String> pubSub = new JedisShardedPubSubBase<String>() {

            @Override
            public void onSMessage(String channel, String message) {
                throw new IllegalStateException("callback failure");
            }

            @Override
            protected String encode(byte[] raw) {
                return new String(raw);
            }
        };

        final Connection mockConnection = mock(Connection.class);
        when(mockConnection.getUnflushedObject()).thenReturn(
                Arrays.asList(SSUBSCRIBE.getRaw(), "channel".getBytes(), 1L),
                Arrays.asList(SMESSAGE.getRaw(), "channel".getBytes(), "message".getBytes()));

        assertThrows(IllegalStateException.class,
            () -> pubSub.proceed(mockConnection, "channel"));

        verify(mockConnection).setBroken();
    }

    @Test
    public void testProceed_givenServerUnsubscribe_keepsConnectionUsable() {
        final JedisShardedPubSubBase<String> pubSub = new JedisShardedPubSubBase<String>() {

            @Override
            protected String encode(byte[] raw) {
                return new String(raw);
            }
        };

        final Connection mockConnection = mock(Connection.class);
        when(mockConnection.getUnflushedObject()).thenReturn(
                Arrays.asList(SSUBSCRIBE.getRaw(), "channel".getBytes(), 1L),
                Arrays.asList(SUNSUBSCRIBE.getRaw(), "channel".getBytes(), 0L));

        pubSub.proceed(mockConnection, "channel");

        verify(mockConnection, never()).setBroken();
    }
}
