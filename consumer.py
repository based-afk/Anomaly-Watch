import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from clickhouse_driver import Client

# Configuration
KAFKA_BROKER = "localhost:19092"
TOPIC = "telemetry"
CLICKHOUSE_HOST = "localhost"
BATCH_SIZE = 1000
POLL_TIMEOUT = 1.0

# Kafka Consumer Configuration
# strict rule: disable auto commit to prevent offset progression if CH insert fails
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'clickhouse-ingestion-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# ClickHouse Client
ch_client = Client(host=CLICKHOUSE_HOST, database='observability')

def process_message(msg):
    """ Parse the kafka message and format it for ClickHouse insert """
    try:
        data = json.loads(msg.value().decode('utf-8'))
        # Parse timestamp back to datetime for ClickHouse DateTime64
        # Python 3.7+ isoformat with Z or +00:00
        ts = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        
        return (
            data['id'],
            data['service'],
            ts,
            data['cpu_usage'],
            data['memory_usage'],
            data['error_rate'],
            data['is_anomaly']
        )
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None

def main():
    print("Starting ClickHouse consumer...")
    batch = []
    
    try:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT)
            
            if msg is None:
                pass # No new messages
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass # End of partition
                else:
                    print(f"Consumer error: {msg.error()}")
            else:
                parsed = process_message(msg)
                if parsed:
                    batch.append(parsed)
            
            # If batch is full, or we hit a timeout and have some messages
            if len(batch) >= BATCH_SIZE:
                try:
                    # Batch insert into ClickHouse (strict rule to avoid 'Too many parts')
                    ch_client.execute(
                        'INSERT INTO telemetry (id, service, timestamp, cpu_usage, memory_usage, error_rate, is_anomaly) VALUES',
                        batch
                    )
                    
                    # Strict Rule: commit offset only AFTER successful write to ClickHouse (At-least-once)
                    consumer.commit(asynchronous=False)
                    print(f"Inserted and committed batch of {len(batch)} records.")
                    
                    # Clear batch
                    batch = []
                except Exception as e:
                    print(f"Error inserting to ClickHouse: {e}")
                    print("Will retry. Kafka offset not committed.")
                    time.sleep(2) # Backoff before retry
                    # Note: In a real system, you might want to crash here and let the orchestrator restart,
                    # but for this script, we just retry the insert.
                    
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
