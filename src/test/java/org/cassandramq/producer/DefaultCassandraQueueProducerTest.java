package org.cassandramq.producer;

import org.junit.jupiter.api.Test;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;
import org.cassandramq.store.QueueMessageStore;
import org.cassandramq.util.BucketTimeUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultCassandraQueueProducerTest {
    @Test
    void sendsReadyMessageWithShardAndBucket() {
        QueueProperties properties = properties();
        CapturingStore store = new CapturingStore();
        DefaultCassandraQueueProducer producer = new DefaultCassandraQueueProducer(properties, store);

        UUID id = producer.send("orders", "p1".getBytes());

        assertNotNull(id);
        assertEquals(1, store.inserted.size());
        Message message = store.inserted.get(0);
        assertEquals(MessageStatus.READY, message.status());
        assertEquals("orders", message.queueName());
        assertTrue(message.shardId() >= 0 && message.shardId() < properties.queue().totalShards());
    }

    @Test
    void sendsDelayedMessageIntoFutureBucket() {
        QueueProperties properties = properties();
        CapturingStore store = new CapturingStore();
        DefaultCassandraQueueProducer producer = new DefaultCassandraQueueProducer(properties, store);

        producer.sendDelayed("orders", "p2".getBytes(), Duration.ofSeconds(25));

        Message message = store.inserted.get(0);
        Instant expectedBucket = BucketTimeUtil.bucketStart(message.createdTime().plusSeconds(25), properties.queue().bucketSizeSeconds());
        assertEquals(expectedBucket, message.bucketTime());
    }

    @Test
    void sendsBatchMessages() {
        QueueProperties properties = properties();
        CapturingStore store = new CapturingStore();
        DefaultCassandraQueueProducer producer = new DefaultCassandraQueueProducer(properties, store);

        List<UUID> ids = producer.sendBatch("orders", List.of("a".getBytes(), "b".getBytes(), "c".getBytes()));

        assertEquals(3, ids.size());
        assertEquals(3, store.inserted.size());
    }

    private static QueueProperties properties() {
        Properties p = new Properties();
        p.setProperty("cassandramq.cassandra.contact-points", "127.0.0.1");
        p.setProperty("cassandramq.cassandra.port", "9042");
        p.setProperty("cassandramq.cassandra.local-datacenter", "datacenter1");
        p.setProperty("cassandramq.cassandra.keyspace", "cassandra_mq");
        p.setProperty("cassandramq.cassandra.username", "");
        p.setProperty("cassandramq.cassandra.password", "");
        p.setProperty("cassandramq.cassandra.connect-timeout-ms", "5000");
        p.setProperty("cassandramq.cassandra.request-timeout-ms", "3000");
        p.setProperty("cassandramq.cassandra.max-requests-per-connection", "1024");
        p.setProperty("cassandramq.cassandra.ssl-enabled", "false");

        p.setProperty("cassandramq.queue.total-shards", "64");
        p.setProperty("cassandramq.queue.bucket-size-seconds", "10");
        p.setProperty("cassandramq.queue.default-queue", "default");
        p.setProperty("cassandramq.queue.worker-id", "worker-1");
        p.setProperty("cassandramq.queue.max-owned-shards", "64");
        p.setProperty("cassandramq.queue.target-owned-shards", "16");
        p.setProperty("cassandramq.queue.shard-lease-duration-seconds", "30");
        p.setProperty("cassandramq.queue.shard-lease-renew-interval-seconds", "10");
        p.setProperty("cassandramq.queue.shard-rebalance-interval-seconds", "30");

        p.setProperty("cassandramq.poll.interval-ms", "100");
        p.setProperty("cassandramq.poll.batch-size", "200");
        p.setProperty("cassandramq.poll.max-buckets-per-shard", "2");
        p.setProperty("cassandramq.poll.pause-inflight-threshold-percent", "90");

        p.setProperty("cassandramq.claim.task-lease-duration-seconds", "30");
        p.setProperty("cassandramq.claim.max-claim-attempts-per-cycle", "3");

        p.setProperty("cassandramq.execution.thread-pool-size", "1");
        p.setProperty("cassandramq.execution.max-inflight-tasks", "10");
        p.setProperty("cassandramq.execution.shutdown-wait-seconds", "2");

        p.setProperty("cassandramq.retry.max-attempts", "2");
        p.setProperty("cassandramq.retry.backoff-sequence", "PT1S,PT5S");
        p.setProperty("cassandramq.retry.jitter-percent", "0");

        p.setProperty("cassandramq.metrics.enabled", "false");
        p.setProperty("cassandramq.metrics.prometheus-enabled", "false");
        p.setProperty("cassandramq.metrics.namespace", "cassandramq");
        p.setProperty("cassandramq.metrics.lag-scan-interval-seconds", "15");

        p.setProperty("cassandramq.runtime.startup-acquire-timeout-seconds", "20");
        p.setProperty("cassandramq.runtime.health-check-interval-seconds", "10");
        p.setProperty("cassandramq.runtime.enable-lease-recovery", "true");
        p.setProperty("cassandramq.runtime.recovery-lookback-buckets", "3");
        return QueueProperties.from(p);
    }

    private static final class CapturingStore implements QueueMessageStore {
        private final List<Message> inserted = new ArrayList<>();

        @Override
        public void insert(Message message) {
            inserted.add(message);
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
        public void scheduleRetry(Message message, Instant retryBucket, int retryCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean requeueExpiredRunning(Message message) {
            throw new UnsupportedOperationException();
        }
    }
}
