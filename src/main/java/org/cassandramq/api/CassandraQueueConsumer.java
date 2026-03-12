package org.cassandramq.api;

public interface CassandraQueueConsumer {
    void start(MessageHandler handler);

    void stop();
}
