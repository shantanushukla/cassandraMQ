package org.cassandramq.api;

import org.cassandramq.config.QueueClientFactory;
import org.cassandramq.config.QueueProperties;

public final class CassandraMQ {
    private CassandraMQ() {
    }

    public static QueueClientFactory.QueueClients createDefault() {
        QueueProperties properties = QueueProperties.loadDefault();
        return QueueClientFactory.create(properties);
    }
}
