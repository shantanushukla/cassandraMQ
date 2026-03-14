package org.cassandramq.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.cassandramq.config.QueueProperties;

import java.nio.file.Paths;

public final class CassandraSessionFactory {
    private CassandraSessionFactory() {
    }

    public static CqlSession create(QueueProperties properties) {
        QueueProperties.CassandraConfig cfg = properties.cassandra();

        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, cfg.connectTimeout())
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, cfg.requestTimeout())
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1)
                .build();

        if (cfg.secureConnectBundlePath().isBlank()) {
            throw new IllegalArgumentException("cassandramq.cassandra.secure-connect-bundle-path is required");
        }
        if (cfg.clientId().isBlank() || cfg.clientSecret().isBlank()) {
            throw new IllegalArgumentException("Cassandra cloud auth is required: set cassandramq.cassandra.client-id and cassandramq.cassandra.client-secret");
        }

        CqlSessionBuilder builder = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(cfg.secureConnectBundlePath()))
                .withAuthCredentials(cfg.clientId(), cfg.clientSecret())
                .withKeyspace("cassandra_mq")
                .withConfigLoader(loader);

        return builder.build();
    }
}
