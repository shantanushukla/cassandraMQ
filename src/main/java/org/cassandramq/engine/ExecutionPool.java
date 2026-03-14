package org.cassandramq.engine;

import lombok.extern.slf4j.Slf4j;
import org.cassandramq.api.MessageHandler;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.model.Message;
import org.cassandramq.store.QueueMessageStore;
import org.cassandramq.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class ExecutionPool {
    private final String workerId;
    private final QueueMessageStore repository;
    private final RetryManager retryManager;
    private final QueueMetrics metrics;
    private final ExecutorService executor;
    private final Semaphore inflight;
    private final QueueProperties properties;

    public ExecutionPool(
            QueueProperties properties,
            QueueMessageStore repository,
            RetryManager retryManager,
            QueueMetrics metrics
    ) {
        this.workerId = properties.queue().workerId();
        this.repository = repository;
        this.retryManager = retryManager;
        this.metrics = metrics;
        this.properties = properties;
        this.executor = Executors.newFixedThreadPool(properties.execution().threadPoolSize(), new NamedThreadFactory("mq-exec"));
        this.inflight = new Semaphore(properties.execution().maxInflightTasks());
    }

    public void execute(Message message, MessageHandler handler) {
        metrics.setInflightTasks(properties.execution().maxInflightTasks() - inflight.availablePermits());
        if (!inflight.tryAcquire()) {
            if (retryManager.scheduleRetry(message)) {
                metrics.incrementRetried();
            } else {
                repository.markFailed(message, workerId);
                metrics.incrementFailed();
            }
            return;
        }

        executor.execute(() -> {
            try {
                long started = System.nanoTime();
                try {
                    handler.handle(message);
                } finally {
                    metrics.recordHandlerLatency(java.time.Duration.ofNanos(System.nanoTime() - started));
                }
                repository.markCompleted(message, workerId);
                metrics.incrementCompleted();
            } catch (Exception ex) {
                if (retryManager.scheduleRetry(message)) {
                    metrics.incrementRetried();
                    log.debug("Scheduled retry for message {}", message.messageId(), ex);
                } else {
                    repository.markFailed(message, workerId);
                    metrics.incrementFailed();
                    log.warn("Message {} exceeded retry attempts and marked FAILED", message.messageId(), ex);
                }
            } finally {
                inflight.release();
                metrics.setInflightTasks(properties.execution().maxInflightTasks() - inflight.availablePermits());
            }
        });
    }

    public void shutdown() {
        executor.shutdown();
        try {
            executor.awaitTermination(properties.execution().shutdownWait().toSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
