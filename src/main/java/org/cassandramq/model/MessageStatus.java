package org.cassandramq.model;

public enum MessageStatus {
    READY,
    RUNNING,
    COMPLETED,
    RETRY,
    FAILED
}
