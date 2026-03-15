package org.cassandramq.perf;

import org.cassandramq.api.CassandraQueueProducer;
import org.cassandramq.config.QueueClientFactory;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.model.Message;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manual end-to-end throughput harness.
 * Run with:
 * mvn -q -DskipTests test-compile exec:java \
 *   -Dexec.mainClass=org.cassandramq.perf.QueueThroughputHarness \
 *   -Dexec.classpathScope=test \
 *   -Dcassandramq.perf.messages=100000 \
 *   -Dcassandramq.perf.producers=4 \
 *   -Dcassandramq.perf.consumers=4
 */
public final class QueueThroughputHarness {
    private QueueThroughputHarness() {
    }

    public static void main(String[] args) throws Exception {
        int messages = Integer.parseInt(System.getProperty("cassandramq.perf.messages", "100000"));
        int producerInstances = Integer.parseInt(System.getProperty("cassandramq.perf.producers", "4"));
        int consumerInstances = Integer.parseInt(System.getProperty("cassandramq.perf.consumers", "4"));
        int printEvery = Integer.parseInt(System.getProperty("cassandramq.perf.print-every", "0"));
        int sendRetries = Integer.parseInt(System.getProperty("cassandramq.perf.send-retries", "2"));
        long sendRetryDelayMs = Long.parseLong(System.getProperty("cassandramq.perf.send-retry-delay-ms", "50"));
        long maxWaitSeconds = Long.parseLong(System.getProperty("cassandramq.perf.max-wait-seconds", "300"));
        int totalShards = Integer.parseInt(System.getProperty("cassandramq.perf.total-shards", "64"));
        String queue = System.getProperty(
                "cassandramq.perf.queue",
                "perf-" + Instant.now().toEpochMilli()
        );

        if (messages <= 0 || producerInstances <= 0 || consumerInstances <= 0) {
            throw new IllegalArgumentException("messages/producers/consumers must all be > 0");
        }

        List<QueueClientFactory.QueueClients> consumers = new ArrayList<>();
        List<QueueClientFactory.QueueClients> producers = new ArrayList<>();
        ExecutorService producerPool = Executors.newFixedThreadPool(producerInstances);
        AtomicLong produced = new AtomicLong();
        AtomicLong processed = new AtomicLong();
        AtomicLong firstSendNanos = new AtomicLong();
        AtomicLong lastSendNanos = new AtomicLong();
        AtomicLong firstProcessNanos = new AtomicLong();
        AtomicLong lastProcessNanos = new AtomicLong();
        AtomicLong sendFailures = new AtomicLong();

        try {
            for (int i = 0; i < consumerInstances; i++) {
                String workerId = "perf-consumer-" + i;
                QueueProperties p = createProperties(queue, workerId, totalShards, consumerInstances);
                QueueClientFactory.QueueClients clients = QueueClientFactory.create(p);
                consumers.add(clients);
                clients.consumer().start(message -> handleMessage(
                        workerId,
                        message,
                        processed,
                        firstProcessNanos,
                        lastProcessNanos,
                        printEvery
                ));
            }

            for (int i = 0; i < producerInstances; i++) {
                String workerId = "perf-producer-" + i;
                producers.add(QueueClientFactory.create(createProperties(queue, workerId, totalShards, consumerInstances)));
            }

            System.out.printf(
                    "Starting perf run: queue=%s, messages=%d, producers=%d, consumers=%d, totalShards=%d%n",
                    queue, messages, producerInstances, consumerInstances, totalShards
            );

            int base = messages / producerInstances;
            int extra = messages % producerInstances;
            int startIndex = 0;
            List<Future<?>> producerFutures = new ArrayList<>();
            for (int i = 0; i < producerInstances; i++) {
                QueueClientFactory.QueueClients producerClient = producers.get(i);
                CassandraQueueProducer producer = producerClient.producer();
                int count = base + (i < extra ? 1 : 0);
                int from = startIndex;
                startIndex += count;
                int producerId = i;

                producerFutures.add(producerPool.submit(() -> {
                    for (int n = 0; n < count; n++) {
                        int globalId = from + n;
                        sendWithRetry(
                                producer,
                                queue,
                                ("perf-" + producerId + "-" + globalId).getBytes(StandardCharsets.UTF_8),
                                sendRetries,
                                sendRetryDelayMs,
                                sendFailures
                        );
                        long now = System.nanoTime();
                        firstSendNanos.compareAndSet(0L, now);
                        produced.incrementAndGet();
                        lastSendNanos.set(now);
                    }
                }));
            }

            for (Future<?> future : producerFutures) {
                try {
                    future.get(maxWaitSeconds, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    throw new IllegalStateException("Producer task failed", e.getCause());
                } catch (TimeoutException e) {
                    throw new IllegalStateException("Timed out waiting for producer task completion", e);
                }
            }
            producerPool.shutdown();
            if (!producerPool.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for producer tasks");
            }

            Instant deadline = Instant.now().plusSeconds(maxWaitSeconds);
            while (processed.get() < messages && Instant.now().isBefore(deadline)) {
                Thread.sleep(200);
            }

            long producedCount = produced.get();
            long processedCount = processed.get();
            boolean complete = processedCount >= messages;

            long sendNanos = Math.max(lastSendNanos.get() - firstSendNanos.get(), 1L);
            long consumeNanos = Math.max(lastProcessNanos.get() - firstProcessNanos.get(), 1L);
            double produceThroughput = (producedCount * 1_000_000_000.0) / sendNanos;
            double consumeThroughput = (processedCount * 1_000_000_000.0) / consumeNanos;

            double completedCounterSum = sumCounter(consumers, "cassandramq.completed.total");
            double claimedCounterSum = sumCounter(consumers, "cassandramq.claimed.total");
            double failedCounterSum = sumCounter(consumers, "cassandramq.failed.total");
            double retriedCounterSum = sumCounter(consumers, "cassandramq.retried.total");

            System.out.printf(
                    "Produced: %d messages, throughput: %.2f msg/s, sendFailures=%d%n",
                    producedCount, produceThroughput, sendFailures.get()
            );
            System.out.printf(
                    "Processed: %d/%d messages, throughput: %.2f msg/s, complete=%s%n",
                    processedCount, messages, consumeThroughput, complete
            );
            System.out.printf(
                    "Metrics (sum across consumers) - claimed=%.0f, completed=%.0f, failed=%.0f, retried=%.0f%n",
                    claimedCounterSum, completedCounterSum, failedCounterSum, retriedCounterSum
            );
            for (int i = 0; i < consumers.size(); i++) {
                String wid = "perf-consumer-" + i;
                double consumerCompleted = readCounter(consumers.get(i), "cassandramq.completed.total");
                double consumerClaimed = readCounter(consumers.get(i), "cassandramq.claimed.total");
                System.out.printf("  %s -> claimed=%.0f, completed=%.0f%n", wid, consumerClaimed, consumerCompleted);
            }
            if (!complete) {
                throw new IllegalStateException("Timed out before all messages were processed");
            }
        } finally {
            producerPool.shutdownNow();
            for (QueueClientFactory.QueueClients consumer : consumers) {
                try {
                    consumer.consumer().stop();
                } catch (Exception ignored) {
                    // Best effort cleanup for harness.
                }
            }
            for (QueueClientFactory.QueueClients client : producers) {
                client.close();
            }
            for (QueueClientFactory.QueueClients client : consumers) {
                client.close();
            }
        }
    }

    private static QueueProperties createProperties(
            String queueName,
            String workerId,
            int totalShards,
            int consumerInstances
    ) {
        Properties p = loadDefaultProperties();
        p.setProperty("cassandramq.queue.default-queue", queueName);
        p.setProperty("cassandramq.queue.worker-id", workerId);
        p.setProperty("cassandramq.queue.total-shards", String.valueOf(totalShards));
        p.setProperty("cassandramq.queue.max-owned-shards", String.valueOf(totalShards));
        int target = Math.max(1, (int) Math.ceil(totalShards / (double) Math.max(consumerInstances, 1)));
        p.setProperty("cassandramq.queue.target-owned-shards", String.valueOf(target));
        return QueueProperties.from(p);
    }

    private static Properties loadDefaultProperties() {
        Properties p = new Properties();
        try (InputStream in = QueueThroughputHarness.class.getClassLoader().getResourceAsStream("cassandramq.properties")) {
            if (in == null) {
                throw new IllegalStateException("Resource not found: cassandramq.properties");
            }
            p.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load cassandramq.properties", e);
        }
        return p;
    }

    private static void handleMessage(
            String workerId,
            Message message,
            AtomicLong processed,
            AtomicLong firstProcessNanos,
            AtomicLong lastProcessNanos,
            int printEvery
    ) {
        long now = System.nanoTime();
        firstProcessNanos.compareAndSet(0L, now);
        long count = processed.incrementAndGet();
        lastProcessNanos.set(System.nanoTime());

        if (printEvery > 0 && count % printEvery == 0) {
            System.out.printf(
                    "[%s] processed #%d messageId=%s%n",
                    workerId,
                    count,
                    message.messageId()
            );
        }
    }

    private static double sumCounter(List<QueueClientFactory.QueueClients> clients, String meterName) {
        double total = 0.0;
        for (QueueClientFactory.QueueClients client : clients) {
            total += readCounter(client, meterName);
        }
        return total;
    }

    private static double readCounter(QueueClientFactory.QueueClients client, String meterName) {
        try {
            return client.meterRegistry().get(meterName).counter().count();
        } catch (Exception ignored) {
            // Ignore missing meters for harness reporting.
            return 0.0;
        }
    }

    private static void sendWithRetry(
            CassandraQueueProducer producer,
            String queue,
            byte[] payload,
            int sendRetries,
            long sendRetryDelayMs,
            AtomicLong sendFailures
    ) {
        for (int attempt = 0; attempt <= sendRetries; attempt++) {
            try {
                producer.send(queue, payload);
                return;
            } catch (RuntimeException ex) {
                if (attempt >= sendRetries) {
                    sendFailures.incrementAndGet();
                    throw ex;
                }
                if (sendRetryDelayMs > 0) {
                    try {
                        Thread.sleep(sendRetryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted during producer retry backoff", ie);
                    }
                }
            }
        }
    }
}
