package org.cassandramq.engine;

import org.junit.jupiter.api.Test;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;
import org.cassandramq.store.QueueMessageStore;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryManagerTest {
    @Test
    void schedulesRetryWithinMaxAttempts() {
        CapturingStore store = new CapturingStore();
        RetryManager retryManager = new RetryManager(EngineTestSupport.properties(), store);

        Message message = message(0);
        boolean scheduled = retryManager.scheduleRetry(message);

        assertTrue(scheduled);
        assertEquals(1, store.scheduledCount);
        assertEquals(1, store.lastRetryCount);
        assertEquals(0, store.lastBucket.getEpochSecond() % 10);
    }

    @Test
    void doesNotScheduleWhenAttemptsExhausted() {
        CapturingStore store = new CapturingStore();
        RetryManager retryManager = new RetryManager(EngineTestSupport.properties(), store);

        boolean scheduled = retryManager.scheduleRetry(message(2));

        assertFalse(scheduled);
        assertEquals(0, store.scheduledCount);
    }

    private static Message message(int retryCount) {
        return new Message(
                UUID.randomUUID(),
                "default",
                1,
                Instant.now(),
                "payload".getBytes(),
                MessageStatus.RUNNING,
                Instant.now().plusSeconds(10),
                "worker-1",
                retryCount,
                Instant.now()
        );
    }

    private static final class CapturingStore implements QueueMessageStore {
        int scheduledCount;
        Instant lastBucket;
        int lastRetryCount;

        @Override
        public void scheduleRetry(Message message, Instant retryBucket, int retryCount) {
            scheduledCount++;
            lastBucket = retryBucket;
            lastRetryCount = retryCount;
        }

        @Override
        public void insert(Message message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Message> pollReady(String queueName, int shardId, Instant bucketTime, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Message> pollBucket(String queueName, int shardId, Instant bucketTime, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryClaim(Message message, String workerId, Instant leaseExpiry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markCompleted(Message message, String workerId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markFailed(Message message, String workerId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean requeueExpiredRunning(Message message) {
            throw new UnsupportedOperationException();
        }
    }
}
