# Predictive AI Monitoring Platform (AnomalyWatch)

An enterprise observability platform designed for high-throughput telemetry ingestion, real-time machine learning anomaly detection, and a high-performance predictive alert dashboard. 

This is a portfolio project built with a strict focus on infrastructure correctness, mechanical sympathy, and SRE design principles.

## 🏗️ Architecture

- **Streaming Layer**: Redpanda (Kafka-compatible) for high-throughput event ingestion.
- **Storage Layer**: ClickHouse (Columnar, MergeTree) for fast time-series persistence.
- **Machine Learning**: Python `scikit-learn` (Isolation Forest) for real-time telemetry scoring.
- **Real-time API**: Node.js WebSocket server with bi-directional ping/pong heartbeats.
- **Frontend Dashboard**: React + Apache ECharts for a dynamic 60FPS UI.

## 🛡️ Strict Correctness & Design Rules

This project strictly adheres to several "hard" engineering rules to prevent silent failures and UI freezing under load:
- **At-Least-Once Delivery**: Kafka offsets are *only* committed after successful micro-batched writes to ClickHouse.
- **Optimized Persistence**: Telemetry is written to ClickHouse in batches of 1,000+ rows to avoid "Too many parts in partition" errors.
- **No Data Leakage**: ML rolling statistics (mean, std) strictly use past timestamps and never peer into future windows.
- **Separation of Concerns**: The ML model `fit()` is trained offline; the real-time loop only executes `predict()`.
- **Flat UI Memory**: The ECharts frontend uses fixed circular buffers (max 500 points) and 500ms state update batching to maintain 60 FPS without tab freezing at 100+ messages/sec.
- **Anti-AI-Slop UI**: The dashboard uses a strict, technical SRE design system. No glassmorphism, glowing effects, or rounded pastel cards. 

## 🚀 Getting Started

Currently, the **Data Ingestion (Week 1)** phase is complete. 

### 1. Start Infrastructure
Spin up the Redpanda broker and ClickHouse database:
```bash
docker compose up -d
```

### 2. Apply ClickHouse Schema
Create the `telemetry` table optimized for time-series:
```bash
cat schema.sql | docker exec -i clickhouse clickhouse-client -n
```

### 3. Run Ingestion Pipeline
Set up a Python virtual environment and run the services:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start generating 1,000+ mock metrics per second
python telemetry_generator.py

# In a separate terminal, start the ClickHouse consumer
source venv/bin/activate
python consumer.py
```

## 🚧 Current Scope Limitations
- **WebSocket Auth**: There is currently no authentication or authorization on the WebSocket layer. This is an explicit cut to keep the focus on infrastructure and streaming performance. 

## 📅 Roadmap
- **Week 1 (Done)**: Ingestion (Redpanda + ClickHouse)
- **Week 2 (Next)**: Machine Learning (Isolation Forest & Lead-time logic)
- **Week 3**: Real-time Node.js WebSocket API
- **Week 4**: React SRE Dashboard & Performance Profiling