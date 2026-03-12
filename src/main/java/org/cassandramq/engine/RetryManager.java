package org.cassandramq.engine;

import org.cassandramq.config.QueueProperties;
import org.cassandramq.model.Message;
import org.cassandramq.store.QueueMessageStore;
import org.cassandramq.util.BucketTimeUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class RetryManager {
    private final QueueProperties properties;
    private final QueueMessageStore repository;

    public RetryManager(QueueProperties properties, QueueMessageStore repository) {
        this.properties = properties;
        this.repository = repository;
    }

    public boolean scheduleRetry(Message message) {
        int nextAttempt = message.retryCount() + 1;
        if (nextAttempt > properties.retry().maxAttempts()) {
            return false;
        }

        List<Duration> backoff = properties.retry().backoffSequence();
        Duration baseDelay = backoff.isEmpty()
                ? Duration.ofSeconds(1)
                : backoff.get(Math.min(nextAttempt - 1, backoff.size() - 1));
        Duration jitter = jitter(baseDelay, properties.retry().jitterPercent());

        Instant target = Instant.now().plus(baseDelay).plus(jitter);
        Instant retryBucket = BucketTimeUtil.bucketStart(target, properties.queue().bucketSizeSeconds());
        repository.scheduleRetry(message, retryBucket, nextAttempt);
        return true;
    }

    private Duration jitter(Duration delay, int jitterPercent) {
        if (jitterPercent == 0) {
            return Duration.ZERO;
        }
        double max = delay.toMillis() * (jitterPercent / 100.0);
        long millis = (long) ThreadLocalRandom.current().nextDouble(-max, max + 1);
        return Duration.ofMillis(millis);
    }
}
