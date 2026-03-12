package org.cassandramq.engine;

import org.cassandramq.api.MessageHandler;
import org.cassandramq.model.Message;

public interface ClaimDispatcher {
    void tryClaimAndExecute(Message message, MessageHandler handler);
}
