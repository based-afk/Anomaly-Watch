import json
import time
import os
import joblib
import numpy as np
import requests
from collections import deque, defaultdict
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_BROKER = "localhost:19092"
TOPIC = "telemetry"
WS_SERVER_URL = "http://localhost:3001/api/ingest"
BATCH_SIZE = 50 # Smaller batch size for real-time smooth UI updates
POLL_TIMEOUT = 0.1

# Kafka Consumer Configuration (Separate group from ClickHouse consumer)
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'live-predictor-group',
    'auto.offset.reset': 'latest', # Start from now for real-time
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# ML Loading and State Initialization
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
    """ Parse the kafka message, compute ML features, predict, and format for WebSocket """
    try:
        data = json.loads(msg.value().decode('utf-8'))
        
        # Parse timestamp
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
        
        # Compute rolling features from PAST history
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
        dt = ts_sec - oldest_ts
        if dt > 0:
            mem_rate = (mem - oldest_mem) / dt
            if mem_rate > 0:
                ttf_seconds = (100.0 - mem) / mem_rate
                if ttf_seconds <= 1800: # 30 minutes
                    predictive_alert = 1
        
        return {
            "id": data['id'],
            "service": service,
            "timestamp": ts_str,
            "cpu_usage": cpu,
            "memory_usage": mem,
            "error_rate": error_rate,
            "is_anomaly": is_anomaly_true,
            "anomaly_score": float(anomaly_score),
            "predictive_alert": predictive_alert
        }
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None

def main():
    print("Starting Live Predictor for WebSocket server...")
    batch = []
    
    try:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT)
            
            if msg is None:
                if len(batch) > 0:
                    try:
                        requests.post(WS_SERVER_URL, json=batch)
                        batch = []
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to push to WS server: {e}")
                        time.sleep(1)
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
            else:
                parsed = process_message(msg)
                if parsed:
                    batch.append(parsed)
            
            if len(batch) >= BATCH_SIZE:
                try:
                    requests.post(WS_SERVER_URL, json=batch)
                    batch = []
                except requests.exceptions.RequestException as e:
                    print(f"Failed to push to WS server: {e}")
                    time.sleep(1)
                    
    except KeyboardInterrupt:
        print("Stopping live predictor...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
