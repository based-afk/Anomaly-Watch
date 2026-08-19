CREATE DATABASE IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.telemetry (
    id UUID,
    service String,
    timestamp DateTime64(3, 'UTC'),
    cpu_usage Float64,
    memory_usage Float64,
    error_rate Float64,
    is_anomaly UInt8
) ENGINE = MergeTree()
ORDER BY (service, timestamp);
