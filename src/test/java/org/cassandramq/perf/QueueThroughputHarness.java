package org.cassandramq.perf;

import org.cassandramq.api.CassandraQueueProducer;
import org.cassandramq.config.QueueClientFactory;
import org.cassandramq.config.QueueProperties;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * Manual throughput harness.
 * Run with:
 * mvn -q -DskipTests test-compile exec:java -Dexec.mainClass=org.leetcode.cassandramq.perf.QueueThroughputHarness -Dexec.classpathScope=test
 */
public final class QueueThroughputHarness {
    private QueueThroughputHarness() {
    }

    public static void main(String[] args) {
        int messages = Integer.parseInt(System.getProperty("cassandramq.perf.messages", "100000"));
        String queue = System.getProperty("cassandramq.perf.queue", "default");

        QueueProperties properties = QueueProperties.loadDefault();
        QueueClientFactory.QueueClients clients = QueueClientFactory.create(properties);
        CassandraQueueProducer producer = clients.producer();

        Instant start = Instant.now();
        for (int i = 0; i < messages; i++) {
            producer.send(queue, ("perf-" + i).getBytes(StandardCharsets.UTF_8));
        }
        Duration elapsed = Duration.between(start, Instant.now());
        long millis = Math.max(elapsed.toMillis(), 1);
        double throughput = (messages * 1000.0) / millis;
        System.out.printf("Sent %d messages in %d ms (%.2f msg/s)%n", messages, millis, throughput);
        clients.close();
    }
}
