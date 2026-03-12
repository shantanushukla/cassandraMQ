package org.cassandramq.consumer;

import org.cassandramq.api.MessageHandler;
import org.cassandramq.engine.ExecutionPool;
import org.cassandramq.engine.LeaseRecoveryEngine;
import org.cassandramq.engine.PollingEngine;
import org.cassandramq.engine.ShardOwnershipManager;

public final class ConsumerRuntime {
    private final ShardOwnershipManager shardOwnershipManager;
    private final PollingEngine pollingEngine;
    private final ExecutionPool executionPool;
    private final LeaseRecoveryEngine leaseRecoveryEngine;

    public ConsumerRuntime(
            ShardOwnershipManager shardOwnershipManager,
            PollingEngine pollingEngine,
            ExecutionPool executionPool,
            LeaseRecoveryEngine leaseRecoveryEngine
    ) {
        this.shardOwnershipManager = shardOwnershipManager;
        this.pollingEngine = pollingEngine;
        this.executionPool = executionPool;
        this.leaseRecoveryEngine = leaseRecoveryEngine;
    }

    public void start(MessageHandler handler) {
        shardOwnershipManager.start();
        leaseRecoveryEngine.start();
        pollingEngine.start(handler);
    }

    public void stop() {
        pollingEngine.stop();
        leaseRecoveryEngine.stop();
        shardOwnershipManager.stop();
        executionPool.shutdown();
    }
}
