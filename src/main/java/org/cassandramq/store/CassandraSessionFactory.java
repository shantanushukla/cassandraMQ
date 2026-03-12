package org.cassandramq.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.cassandramq.config.QueueProperties;

import java.net.InetSocketAddress;

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

        CqlSessionBuilder builder = CqlSession.builder()
                .withLocalDatacenter(cfg.localDatacenter())
                .withKeyspace(cfg.keyspace())
                .withConfigLoader(loader);
        for (String host : cfg.contactPoints().split(",")) {
            builder.addContactPoint(new InetSocketAddress(host.trim(), cfg.port()));
        }

        if (!cfg.username().isBlank()) {
            builder.withAuthCredentials(cfg.username(), cfg.password());
        }
        return builder.build();
    }
}
