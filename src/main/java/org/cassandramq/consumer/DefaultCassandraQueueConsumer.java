package org.cassandramq.consumer;

import org.cassandramq.api.CassandraQueueConsumer;
import org.cassandramq.api.MessageHandler;

import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultCassandraQueueConsumer implements CassandraQueueConsumer {
    private final ConsumerRuntime runtime;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public DefaultCassandraQueueConsumer(ConsumerRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void start(MessageHandler handler) {
        if (started.compareAndSet(false, true)) {
            runtime.start(handler);
        }
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            runtime.stop();
        }
    }
}
