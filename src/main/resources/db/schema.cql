CREATE TABLE IF NOT EXISTS queue_messages_by_shard (
    queue_name TEXT,
    shard_id INT,
    bucket_time TIMESTAMP,
    message_id UUID,
    payload BLOB,
    status TEXT,
    lease_expiry TIMESTAMP,
    worker_id TEXT,
    retry_count INT,
    created_time TIMESTAMP,
    PRIMARY KEY ((queue_name, shard_id, bucket_time), message_id)
) WITH default_time_to_live = 259200;

CREATE TABLE IF NOT EXISTS shard_ownership (
    queue_name TEXT,
    shard_id INT,
    worker_id TEXT,
    lease_expiry TIMESTAMP,
    PRIMARY KEY ((queue_name), shard_id)
) WITH default_time_to_live = 259200;
