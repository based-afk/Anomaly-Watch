import json
import time
import os
import joblib
import numpy as np
from collections import deque, defaultdict
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
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'clickhouse-ingestion-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])
ch_client = Client(host=CLICKHOUSE_HOST, database='observability')

# ML Loading and State Initialization
# IsolationForest.fit() must never run inside the real-time stream consumer loop.
model_data = None
if os.path.exists('model.pkl'):
    model_data = joblib.load('model.pkl')
    print("Loaded pre-trained model.pkl successfully.")
else:
    print("Warning: model.pkl not found. Run train_model.py first.")

# We need a rolling window of 10 for feature computation to match train_model.py
WINDOW_SIZE = 10
# State: service -> deque of (timestamp_seconds, cpu, mem)
service_state = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

def process_message(msg):
    """ Parse the kafka message, compute ML features, predict, and format for ClickHouse insert """
    try:
        data = json.loads(msg.value().decode('utf-8'))
        
        # Parse timestamp back to datetime for ClickHouse DateTime64
        ts_str = data['timestamp'].replace('Z', '+00:00')
        ts = datetime.fromisoformat(ts_str)
        ts_sec = ts.timestamp()
        
        service = data['service']
        cpu = data['cpu_usage']
        mem = data['memory_usage']
        error_rate = data['error_rate']
        is_anomaly_true = data['is_anomaly']
        
        # Get the history for this service
        history = service_state[service]
        
        # Compute rolling features from PAST history (before appending current)
        if len(history) > 0:
            past_cpus = [x[1] for x in history]
            past_mems = [x[2] for x in history]
            cpu_roll_mean = np.mean(past_cpus)
            cpu_roll_std = np.std(past_cpus)
            mem_roll_mean = np.mean(past_mems)
            mem_roll_std = np.std(past_mems)
            oldest_ts, _, oldest_mem = history[0]
        else:
            cpu_roll_mean, cpu_roll_std = cpu, 0.0
            mem_roll_mean, mem_roll_std = mem, 0.0
            oldest_ts, oldest_mem = ts_sec, mem
            
        # Append current reading to the window
        history.append((ts_sec, cpu, mem))
        
        anomaly_score = 0.0
        predictive_alert = 0
        
        # Apply Machine Learning Model
        if model_data:
            clf = model_data['model']
            min_score = model_data['min_score']
            max_score = model_data['max_score']
            feature_cols = model_data['feature_cols']
            
            # Construct feature vector exactly as in train_model.py
            # ['cpu_usage', 'memory_usage', 'error_rate', 'cpu_roll_mean', 'cpu_roll_std', 'mem_roll_mean', 'mem_roll_std']
            features = np.array([[
                cpu, mem, error_rate, 
                cpu_roll_mean, cpu_roll_std, 
                mem_roll_mean, mem_roll_std
            ]])
            
            # Predict
            raw_score = -clf.decision_function(features)[0]
            # Normalize exactly as during training
            anomaly_score = (raw_score - min_score) / (max_score - min_score + 1e-9)
            
        # --- Predictive Lead-Time Logic (30 minutes in advance claim) ---
        # The exact formula driving this claim:
        # 1. We look at the memory growth over the current rolling window.
        # 2. Rate of memory growth (% per second) = (Current Memory - Oldest Window Memory) / (Current Time - Oldest Time)
        # 3. Time to failure (TTF) = (100% - Current Memory) / Rate
        # 4. If TTF > 0 and TTF <= 1800 seconds (30 minutes), we issue a predictive alert.
        dt = ts_sec - oldest_ts
        if dt > 0:
            mem_rate = (mem - oldest_mem) / dt
            if mem_rate > 0:
                ttf_seconds = (100.0 - mem) / mem_rate
                if ttf_seconds <= 1800: # 30 minutes
                    predictive_alert = 1
        
        return (
            data['id'],
            service,
            ts,
            cpu,
            mem,
            error_rate,
            is_anomaly_true,
            float(anomaly_score),
            predictive_alert
        )
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None

def main():
    print("Starting ClickHouse consumer with real-time ML scoring...")
    batch = []
    
    try:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT)
            
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    print(f"Consumer error: {msg.error()}")
            else:
                parsed = process_message(msg)
                if parsed:
                    batch.append(parsed)
            
            if len(batch) >= BATCH_SIZE:
                try:
                    ch_client.execute(
                        'INSERT INTO telemetry (id, service, timestamp, cpu_usage, memory_usage, error_rate, is_anomaly, anomaly_score, predictive_alert) VALUES',
                        batch
                    )
                    # Strict Rule: commit offset only AFTER successful write to ClickHouse (At-least-once)
                    consumer.commit(asynchronous=False)
                    print(f"Inserted and committed batch of {len(batch)} records (including ML scores).")
                    batch = []
                except Exception as e:
                    print(f"Error inserting to ClickHouse: {e}")
                    print("Will retry. Kafka offset not committed.")
                    time.sleep(2)
                    
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
