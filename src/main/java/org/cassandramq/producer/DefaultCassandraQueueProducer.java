package org.cassandramq.producer;

import org.cassandramq.api.CassandraQueueProducer;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;
import org.cassandramq.store.QueueMessageStore;
import org.cassandramq.util.BucketTimeUtil;
import org.cassandramq.util.ShardRouter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class DefaultCassandraQueueProducer implements CassandraQueueProducer {
    private final QueueProperties properties;
    private final QueueMessageStore messageRepository;

    public DefaultCassandraQueueProducer(QueueProperties properties, QueueMessageStore messageRepository) {
        this.properties = properties;
        this.messageRepository = messageRepository;
    }

    @Override
    public UUID send(String queueName, byte[] payload) {
        return sendDelayed(queueName, payload, Duration.ZERO);
    }

    @Override
    public UUID sendDelayed(String queueName, byte[] payload, Duration delay) {
        UUID messageId = UUID.randomUUID();
        Instant now = Instant.now();
        Instant target = now.plus(delay);
        Instant bucket = BucketTimeUtil.bucketStart(target, properties.queue().bucketSizeSeconds());
        int shardId = ShardRouter.shardFor(queueName, messageId, properties.queue().totalShards());

        Message message = new Message(
                messageId,
                queueName,
                shardId,
                bucket,
                payload,
                MessageStatus.READY,
                null,
                null,
                0,
                now
        );
        messageRepository.insert(message);
        return messageId;
    }

    @Override
    public List<UUID> sendBatch(String queueName, List<byte[]> payloads) {
        List<UUID> ids = new ArrayList<>(payloads.size());
        for (byte[] payload : payloads) {
            ids.add(send(queueName, payload));
        }
        return ids;
    }
}
