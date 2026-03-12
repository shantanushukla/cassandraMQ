package org.cassandramq.engine;

import org.junit.jupiter.api.Test;
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

class LeaseRecoveryEngineTest {
    @Test
    void recoversOnlyExpiredRunningMessages() {
        QueueMetrics metrics = new QueueMetrics();
        RecoveringStore store = new RecoveringStore(List.of(
                message(MessageStatus.RUNNING, Instant.now().minusSeconds(5)),
                message(MessageStatus.RUNNING, Instant.now().plusSeconds(5)),
                message(MessageStatus.READY, null)
        ));

        LeaseRecoveryEngine engine = new LeaseRecoveryEngine(
                EngineTestSupport.properties(),
                () -> Set.of(1),
                store,
                metrics
        );

        engine.runRecoveryCycle();

        assertEquals(1, store.requeued);
        assertEquals(1, metrics.recovered());
    }

    private static Message message(MessageStatus status, Instant leaseExpiry) {
        return new Message(
                UUID.randomUUID(),
                "default",
                1,
                Instant.now(),
                "payload".getBytes(),
                status,
                leaseExpiry,
                "worker-1",
                0,
                Instant.now()
        );
    }

    private static final class RecoveringStore implements QueueMessageStore {
        private final List<Message> messages;
        int requeued;

        private RecoveringStore(List<Message> messages) {
            this.messages = new ArrayList<>(messages);
        }

        @Override
        public List<Message> pollBucket(String queueName, int shardId, Instant bucketTime, int limit) {
            return new ArrayList<>(messages);
        }

        @Override
        public boolean requeueExpiredRunning(Message message) {
            requeued++;
            messages.removeIf(m -> m.messageId().equals(message.messageId()));
            return true;
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
    }
}
