# cassandraMQ

Reusable Java JAR for a Cassandra-backed distributed queue with shard ownership, lease-based processing, retries, and Prometheus-friendly metrics.

## Build

```bash
mvn clean verify
```

## Quick Start

1. Build and install the jar locally:

```bash
mvn clean install
```

2. Add dependency to your application:

```xml
<dependency>
  <groupId>org.leetcode</groupId>
  <artifactId>cassandraMQ</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

3. Configure CassandraMQ using classpath `cassandramq.properties` (or `-Dcassandramq.*`) and start producer + consumer:

```java
import org.cassandramq.api.CassandraMQ;
import org.cassandramq.config.QueueClientFactory;

QueueClientFactory.QueueClients clients = CassandraMQ.createDefault();

clients.consumer().start(message -> {
    // business logic
    System.out.println("Received: " + new String(message.payload()));
});

clients.producer().send("default", "hello".getBytes());

Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    clients.consumer().stop();
    clients.close();
}));
```

4. Runtime behavior:
- Producer writes messages to Cassandra.
- Consumer polls available shards, claims messages, executes the handler, and updates status.
- Shutdown should stop the consumer first, then close shared resources with `clients.close()`.

## Configuration

Default configuration is loaded from:

- `src/main/resources/cassandramq.properties`

All values can be overridden using JVM system properties (`-Dcassandramq.*`).

## Cassandra Schema

Apply:

- `src/main/resources/db/schema.cql`

before starting workers.

## Integration Tests

Default build runs unit tests only.

To run Cassandra integration tests:

```bash
mvn -Pit verify \
  -Dcassandramq.it.enabled=true \
  -Dcassandramq.cassandra.contact-points=127.0.0.1 \
  -Dcassandramq.cassandra.local-datacenter=datacenter1 \
  -Dcassandramq.cassandra.keyspace=cassandra_mq
```

## Throughput Harness

Manual harness class:

- `src/test/java/org/leetcode/cassandramq/perf/QueueThroughputHarness.java`

Use it with a running Cassandra cluster and tuned shard/worker counts.

## Operational Notes

Tuning recommendations are in:

- `docs/operational-tuning.md`
