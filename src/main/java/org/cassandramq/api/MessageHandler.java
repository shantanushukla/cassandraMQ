package org.cassandramq.api;

import org.cassandramq.model.Message;

@FunctionalInterface
public interface MessageHandler {
    void handle(Message message) throws Exception;
}
