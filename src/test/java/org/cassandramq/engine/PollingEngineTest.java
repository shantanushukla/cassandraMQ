package org.cassandramq.engine;

import org.junit.jupiter.api.Test;
import org.cassandramq.api.MessageHandler;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;
import org.cassandramq.store.QueueMessageStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PollingEngineTest {
    @Test
    void respectsClaimBudgetPerCycle() {
        QueueMetrics metrics = new QueueMetrics();
        CapturingClaimDispatcher claimDispatcher = new CapturingClaimDispatcher();
        StaticMessageStore store = new StaticMessageStore(messages(5));

        PollingEngine engine = new PollingEngine(
                EngineTestSupport.properties(),
                () -> Set.of(1),
                store,
                claimDispatcher,
                metrics
        );

        engine.runCycle(noop());

        assertEquals(3, claimDispatcher.claimed);
        assertEquals(3, metrics.polledMessages());
    }

    @Test
    void skipsPollingWhenInflightThresholdExceeded() {
        QueueMetrics metrics = new QueueMetrics();
        metrics.setInflightTasks(10);

        CapturingClaimDispatcher claimDispatcher = new CapturingClaimDispatcher();
        StaticMessageStore store = new StaticMessageStore(messages(3));

        PollingEngine engine = new PollingEngine(
                EngineTestSupport.properties(),
                () -> Set.of(1),
                store,
                claimDispatcher,
                metrics
        );

        engine.runCycle(noop());

        assertEquals(0, claimDispatcher.claimed);
        assertEquals(0, metrics.polledMessages());
    }

    private static MessageHandler noop() {
        return message -> { };
    }

    private static List<Message> messages(int count) {
        List<Message> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(new Message(
                    UUID.randomUUID(),
                    "default",
                    1,
                    Instant.now(),
                    ("payload-" + i).getBytes(),
                    MessageStatus.READY,
                    null,
                    null,
                    0,
                    Instant.now()
            ));
        }
        return out;
    }

    private static final class StaticMessageStore implements QueueMessageStore {
        private final List<Message> messages;

        private StaticMessageStore(List<Message> messages) {
            this.messages = messages;
        }

        @Override
        public List<Message> pollReady(String queueName, int shardId, Instant bucketTime, int limit) {
            return messages;
        }

        @Override
        public void insert(Message message) {
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
        public void scheduleRetry(Message message, Instant retryBucket, int retryCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean requeueExpiredRunning(Message message) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CapturingClaimDispatcher implements ClaimDispatcher {
        int claimed;

        @Override
        public void tryClaimAndExecute(Message message, MessageHandler handler) {
            claimed++;
        }
    }
}
