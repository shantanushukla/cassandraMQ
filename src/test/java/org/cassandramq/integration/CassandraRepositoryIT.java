package org.cassandramq.integration;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Assumptions;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;
import org.cassandramq.store.CassandraSessionFactory;
import org.cassandramq.store.PreparedStatementRegistry;
import org.cassandramq.store.QueueMessageRepository;
import org.cassandramq.store.SchemaValidator;
import org.cassandramq.store.ShardOwnershipRepository;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraRepositoryIT {
    private CqlSession session;
    private QueueMessageRepository messageRepository;
    private ShardOwnershipRepository ownershipRepository;
    private QueueProperties properties;

    @BeforeAll
    void setup() {
        Assumptions.assumeTrue(Boolean.getBoolean("cassandramq.it.enabled"),
                "Set -Dcassandramq.it.enabled=true and run with -Pit to execute Cassandra integration tests");

        properties = QueueProperties.from(baseProperties());
        session = CassandraSessionFactory.create(properties);
        SchemaValidator.validateRequiredTables(session);
        PreparedStatementRegistry statements = new PreparedStatementRegistry(session);
        messageRepository = new QueueMessageRepository(session, statements);
        ownershipRepository = new ShardOwnershipRepository(session, statements);
    }

    @AfterAll
    void tearDown() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    void claimAndCompleteMessageRoundTrip() {
        Instant bucket = Instant.ofEpochSecond((Instant.now().getEpochSecond() / 10) * 10);
        Message message = new Message(
                UUID.randomUUID(),
                properties.queue().defaultQueue(),
                3,
                bucket,
                "it-payload".getBytes(),
                MessageStatus.READY,
                null,
                null,
                0,
                Instant.now()
        );
        messageRepository.insert(message);

        List<Message> polled = messageRepository.pollReady(message.queueName(), message.shardId(), message.bucketTime(), 50);
        assertTrue(polled.stream().anyMatch(m -> m.messageId().equals(message.messageId())));

        boolean claimed = messageRepository.tryClaim(message, "it-worker", Instant.now().plusSeconds(20));
        assertTrue(claimed);

        messageRepository.markCompleted(message, "it-worker");
        List<Message> all = messageRepository.pollBucket(message.queueName(), message.shardId(), message.bucketTime(), 200);
        Message completed = all.stream().filter(m -> m.messageId().equals(message.messageId())).findFirst().orElseThrow();
        assertEquals(MessageStatus.COMPLETED, completed.status());
    }

    @Test
    void acquiresAndReleasesShardLease() {
        String queue = properties.queue().defaultQueue();
        Instant expiry = Instant.now().plusSeconds(20);
        boolean acquired = ownershipRepository.tryAcquire(queue, 8, "it-worker", expiry);
        assertTrue(acquired);

        int activeWorkers = ownershipRepository.countActiveWorkers(queue, Instant.now());
        assertTrue(activeWorkers >= 1);

        boolean renewed = ownershipRepository.renew(queue, 8, "it-worker", Instant.now().plusSeconds(30));
        assertTrue(renewed);

        boolean released = ownershipRepository.release(queue, 8, "it-worker");
        assertTrue(released);
    }

    private static Properties baseProperties() {
        Properties p = new Properties();
        p.setProperty("cassandramq.cassandra.contact-points", System.getProperty("cassandramq.cassandra.contact-points", "127.0.0.1"));
        p.setProperty("cassandramq.cassandra.port", System.getProperty("cassandramq.cassandra.port", "9042"));
        p.setProperty("cassandramq.cassandra.local-datacenter", System.getProperty("cassandramq.cassandra.local-datacenter", "datacenter1"));
        p.setProperty("cassandramq.cassandra.keyspace", System.getProperty("cassandramq.cassandra.keyspace", "cassandra_mq"));
        p.setProperty("cassandramq.cassandra.username", System.getProperty("cassandramq.cassandra.username", ""));
        p.setProperty("cassandramq.cassandra.password", System.getProperty("cassandramq.cassandra.password", ""));
        p.setProperty("cassandramq.cassandra.connect-timeout-ms", "5000");
        p.setProperty("cassandramq.cassandra.request-timeout-ms", "3000");
        p.setProperty("cassandramq.cassandra.max-requests-per-connection", "1024");
        p.setProperty("cassandramq.cassandra.ssl-enabled", "false");

        p.setProperty("cassandramq.queue.total-shards", "64");
        p.setProperty("cassandramq.queue.bucket-size-seconds", "10");
        p.setProperty("cassandramq.queue.default-queue", System.getProperty("cassandramq.queue.default-queue", "default"));
        p.setProperty("cassandramq.queue.worker-id", "it-worker");
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
        p.setProperty("cassandramq.claim.max-claim-attempts-per-cycle", "10");

        p.setProperty("cassandramq.execution.thread-pool-size", "2");
        p.setProperty("cassandramq.execution.max-inflight-tasks", "100");
        p.setProperty("cassandramq.execution.shutdown-wait-seconds", "2");

        p.setProperty("cassandramq.retry.max-attempts", "3");
        p.setProperty("cassandramq.retry.backoff-sequence", "PT1S,PT5S,PT30S");
        p.setProperty("cassandramq.retry.jitter-percent", "0");

        p.setProperty("cassandramq.metrics.enabled", "false");
        p.setProperty("cassandramq.metrics.prometheus-enabled", "false");
        p.setProperty("cassandramq.metrics.namespace", "cassandramq");
        p.setProperty("cassandramq.metrics.lag-scan-interval-seconds", "15");

        p.setProperty("cassandramq.runtime.startup-acquire-timeout-seconds", "20");
        p.setProperty("cassandramq.runtime.health-check-interval-seconds", "10");
        p.setProperty("cassandramq.runtime.enable-lease-recovery", "true");
        p.setProperty("cassandramq.runtime.recovery-lookback-buckets", "3");
        return p;
    }
}
