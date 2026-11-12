-- Kafka dedupe / idempotency table.
-- MessageProcessDAO also creates this table automatically at startup.
CREATE TABLE IF NOT EXISTS kafka_processed_offsets (
    topic VARCHAR(20) NOT NULL,
    partition_no INT NOT NULL,
    record_offset BIGINT NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, partition_no, record_offset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- The preserved output tables are not modified:
--   messageprocess(id, msg_id, user_id, Content, Type, process_time, priority, isPayload, path)
--   mcstats(id, msg_id, sub_table, user_id, action_type, wait_time_ms, processing_time_ms, created_at)
--   users(id, username, password)
--
-- The old routing/sub-table tables (maintable, user_routes, info1..5, warn1..5,
-- fine1..5, severe1..5) are intentionally left in place but are no longer used.
