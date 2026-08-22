from clickhouse_driver import Client

client = Client(host='localhost')
query = """
SELECT service, timestamp, cpu_usage, memory_usage, anomaly_score, predictive_alert 
FROM observability.telemetry 
WHERE anomaly_score > 0.5 OR predictive_alert = 1
LIMIT 5
"""
print(client.execute(query))
