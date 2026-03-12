package org.cassandramq.engine;

import org.junit.jupiter.api.Test;
import org.cassandramq.api.MessageHandler;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;
import org.cassandramq.store.QueueMessageStore;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskClaimEngineTest {
    @Test
    void incrementsClaimFailureWhenCasFails() {
        QueueMetrics metrics = new QueueMetrics();
        CapturingStore store = new CapturingStore();
        store.claimResult = false;
        ExecutionPool executionPool = new ExecutionPool(EngineTestSupport.properties(), store, new RetryManager(EngineTestSupport.properties(), store), metrics);
        TaskClaimEngine claimEngine = new TaskClaimEngine(EngineTestSupport.properties(), store, executionPool, metrics);

        claimEngine.tryClaimAndExecute(message(), noop());

        executionPool.shutdown();
        assertEquals(1, metrics.claimFailures());
        assertEquals(0, metrics.claimed());
    }

    @Test
    void dispatchesToExecutionOnSuccessfulClaim() throws InterruptedException {
        QueueMetrics metrics = new QueueMetrics();
        CapturingStore store = new CapturingStore();
        store.claimResult = true;
        ExecutionPool executionPool = new ExecutionPool(EngineTestSupport.properties(), store, new RetryManager(EngineTestSupport.properties(), store), metrics);
        TaskClaimEngine claimEngine = new TaskClaimEngine(EngineTestSupport.properties(), store, executionPool, metrics);

        claimEngine.tryClaimAndExecute(message(), message -> store.handlerLatch.countDown());

        assertTrue(store.handlerLatch.await(2, TimeUnit.SECONDS));
        executionPool.shutdown();
        assertEquals(1, metrics.claimed());
        assertEquals(1, metrics.completed());
    }

    private static MessageHandler noop() {
        return message -> { };
    }

    private static Message message() {
        return new Message(
                UUID.randomUUID(),
                "default",
                1,
                Instant.now(),
                "payload".getBytes(),
                MessageStatus.READY,
                null,
                null,
                0,
                Instant.now()
        );
    }

    private static final class CapturingStore implements QueueMessageStore {
        private final CountDownLatch handlerLatch = new CountDownLatch(1);
        private boolean claimResult;

        @Override
        public boolean tryClaim(Message message, String workerId, Instant leaseExpiry) {
            return claimResult;
        }

        @Override
        public void markCompleted(Message message, String workerId) {
        }

        @Override
        public void markFailed(Message message, String workerId) {
        }

        @Override
        public void scheduleRetry(Message message, Instant retryBucket, int retryCount) {
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
        public boolean requeueExpiredRunning(Message message) {
            throw new UnsupportedOperationException();
        }
    }
}
