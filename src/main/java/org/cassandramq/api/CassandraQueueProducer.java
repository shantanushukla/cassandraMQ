package org.cassandramq.api;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface CassandraQueueProducer {
    UUID send(String queueName, byte[] payload);

    UUID sendDelayed(String queueName, byte[] payload, Duration delay);

    List<UUID> sendBatch(String queueName, List<byte[]> payloads);
}
