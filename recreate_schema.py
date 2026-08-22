from clickhouse_driver import Client

client = Client(host='localhost')
client.execute('DROP TABLE IF EXISTS observability.telemetry')
client.execute('CREATE DATABASE IF NOT EXISTS observability')
client.execute('''
CREATE TABLE IF NOT EXISTS observability.telemetry (
    id UUID,
    service String,
    timestamp DateTime64(3, 'UTC'),
    cpu_usage Float64,
    memory_usage Float64,
    error_rate Float64,
    is_anomaly UInt8,
    anomaly_score Float64,
    predictive_alert UInt8
) ENGINE = MergeTree()
ORDER BY (service, timestamp)
''')
print("Schema recreated successfully.")
