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

        properties = QueueProperties.loadDefault();
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
}
