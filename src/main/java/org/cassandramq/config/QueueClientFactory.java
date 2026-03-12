package org.cassandramq.config;

import com.datastax.oss.driver.api.core.CqlSession;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.cassandramq.store.CassandraSessionFactory;
import org.cassandramq.store.SchemaValidator;
import org.cassandramq.api.CassandraQueueConsumer;
import org.cassandramq.api.CassandraQueueProducer;
import org.cassandramq.consumer.ConsumerRuntime;
import org.cassandramq.consumer.DefaultCassandraQueueConsumer;
import org.cassandramq.engine.ExecutionPool;
import org.cassandramq.engine.LeaseRecoveryEngine;
import org.cassandramq.engine.PollingEngine;
import org.cassandramq.engine.RetryManager;
import org.cassandramq.engine.ShardOwnershipManager;
import org.cassandramq.engine.TaskClaimEngine;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.producer.DefaultCassandraQueueProducer;
import org.cassandramq.store.PreparedStatementRegistry;
import org.cassandramq.store.QueueMessageRepository;
import org.cassandramq.store.ShardOwnershipRepository;

import java.util.Optional;

public final class QueueClientFactory {
    private QueueClientFactory() {
    }

    public static QueueClients create(QueueProperties properties) {
        CqlSession session = CassandraSessionFactory.create(properties);
        SchemaValidator.validateRequiredTables(session);
        PreparedStatementRegistry statements = new PreparedStatementRegistry(session);
        QueueMessageRepository messageRepository = new QueueMessageRepository(session, statements);
        ShardOwnershipRepository ownershipRepository = new ShardOwnershipRepository(session, statements);

        MeterRegistry meterRegistry = buildMeterRegistry(properties);
        QueueMetrics metrics = properties.metrics().enabled()
                ? new QueueMetrics(meterRegistry, properties.metrics().namespace())
                : new QueueMetrics();
        RetryManager retryManager = new RetryManager(properties, messageRepository);
        ExecutionPool executionPool = new ExecutionPool(properties, messageRepository, retryManager, metrics);
        ShardOwnershipManager ownershipManager = new ShardOwnershipManager(properties, ownershipRepository, metrics);
        TaskClaimEngine claimEngine = new TaskClaimEngine(properties, messageRepository, executionPool, metrics);
        PollingEngine pollingEngine = new PollingEngine(properties, ownershipManager, messageRepository, claimEngine, metrics);
        LeaseRecoveryEngine leaseRecoveryEngine = new LeaseRecoveryEngine(properties, ownershipManager, messageRepository, metrics);
        ConsumerRuntime runtime = new ConsumerRuntime(ownershipManager, pollingEngine, executionPool, leaseRecoveryEngine);

        CassandraQueueProducer producer = new DefaultCassandraQueueProducer(properties, messageRepository);
        CassandraQueueConsumer consumer = new DefaultCassandraQueueConsumer(runtime);
        return new QueueClients(producer, consumer, session, meterRegistry);
    }

    private static MeterRegistry buildMeterRegistry(QueueProperties properties) {
        if (!properties.metrics().enabled()) {
            return new SimpleMeterRegistry();
        }
        if (properties.metrics().prometheusEnabled()) {
            return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        }
        return new SimpleMeterRegistry();
    }

    public record QueueClients(
            CassandraQueueProducer producer,
            CassandraQueueConsumer consumer,
            CqlSession session,
            MeterRegistry meterRegistry
    ) {
        public void close() {
            meterRegistry.close();
            session.close();
        }

        public Optional<String> prometheusScrape() {
            if (meterRegistry instanceof PrometheusMeterRegistry prometheus) {
                return Optional.of(prometheus.scrape());
            }
            return Optional.empty();
        }
    }
}
