import time
import json
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

# Configuration
KAFKA_BROKER = "localhost:19092"
TOPIC = "telemetry"
SERVICES = ["auth-service", "payment-service", "inventory-service", "user-service"]

# Producer configuration
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'telemetry-generator'
}
producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    # Else, we can silently succeed to avoid flooding logs at 1000 msg/sec

def generate_metric(service_id):
    """
    Generate a standard metric.
    Normally CPU is 10-40%, Memory is 20-50%, Error rate is 0-0.05.
    """
    is_anomaly = 0
    cpu_usage = random.uniform(10.0, 40.0)
    memory_usage = random.uniform(20.0, 50.0)
    error_rate = random.uniform(0.0, 0.05)
    
    # 2% chance to inject an anomaly for precision/recall scoring later
    if random.random() < 0.02:
        is_anomaly = 1
        cpu_usage = random.uniform(85.0, 99.9) # Spike
        memory_usage = random.uniform(80.0, 98.0) # Spike
        error_rate = random.uniform(0.2, 0.5) # Spike
        
    return {
        "id": str(uuid.uuid4()),
        "service": service_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cpu_usage": round(cpu_usage, 2),
        "memory_usage": round(memory_usage, 2),
        "error_rate": round(error_rate, 4),
        "is_anomaly": is_anomaly
    }

def main():
    print(f"Starting telemetry generation to {KAFKA_BROKER} on topic {TOPIC}...")
    try:
        while True:
            # Generate a batch of messages to reach high throughput (~1000/sec)
            for _ in range(100):
                service = random.choice(SERVICES)
                metric = generate_metric(service)
                producer.produce(
                    TOPIC, 
                    value=json.dumps(metric).encode('utf-8'),
                    callback=delivery_report
                )
            
            # Poll to handle delivery callbacks
            producer.poll(0)
            
            # Sleep slightly to control rate (e.g. 100 msgs per 0.1s = 1000/sec)
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("Stopping generator...")
    finally:
        print("Flushing remaining messages...")
        producer.flush()

if __name__ == "__main__":
    main()
