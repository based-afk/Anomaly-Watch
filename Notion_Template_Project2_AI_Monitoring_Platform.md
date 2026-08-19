# 📋 Project 2: Predictive AI Monitoring Platform

> **Goal:** Build an enterprise observability tool that ingests high-throughput telemetry streams via Redpanda/Kafka, routes data to ClickHouse, runs Python Isolation Forest ML models to predict failures, and streams alerts to a React dashboard via WebSockets.

## ⚠️ Improvement Notes (read before Week 1)

- [ ] Inject labeled anomalies into synthetic data so precision/recall can be measured, not just anomaly-score thresholds
- [ ] Name the exact feature (e.g. memory growth slope) driving the "30 minutes in advance" claim
- [ ] Back the Model Health & Drift panel with a real scheduled drift-recompute job
- [ ] Commit Kafka offsets only after a successful ClickHouse write (at-least-once delivery)
- [ ] Explicitly note WebSocket auth as a scope cut rather than leaving it unaddressed
- [ ] Validate 60 FPS target against real `setOption()` cost — batch to one call per 500ms tick

---

## 🎨 Frontend Architecture & Anti-AI Design Rules

**Required Pages**
- [ ] `/dashboard` — Live Command Center (40% Predictive Alert Feed | 60% 2×2 Telemetry Metric Grid)
- [ ] `/incidents/:id` — Incident Root-Cause Inspector (synchronized timeline cursor + monospace log terminal)
- [ ] `/topology` — Service Dependency Graph + ML Model Health & Baseline Drift Panel

**Technical Constraints**
- [ ] Circular buffers — max 500 data points per chart, oldest dropped automatically
- [ ] DOM decoupling — ECharts managed imperatively via `useRef`, bypassing React's Virtual DOM
- [ ] Stream controls — "Freeze Feed" button to pause WebSocket updates

**Anti-AI Design System**

| Element | Value |
|---|---|
| Canvas | `#0d0e11` |
| Panel | `#16181d` |
| Border | `#23272e` |
| Status | Green `#10b981` / Amber `#f59e0b` / Red `#ef4444` |
| UI Font | Inter |
| Mono Font | JetBrains Mono (server names, latency, logs) |

**Strict No-Go:** Zero glassmorphism · No rounded pastel cards · No floating AI sparkles

---

## 🟢 Week 1: Data Ingestion & Streaming Infrastructure

**Day 1 — Synthetic Telemetry Generator**
- [ ] Generate mock server metrics (CPU, Memory, Error rates) with timestamps
- [ ] Inject a known labeled anomaly set for later precision/recall scoring

**Day 2 — Redpanda / Kafka Setup**
- [ ] Spin up Redpanda container via Docker Compose
- [ ] Create a telemetry topic and publish mock metric events

**Day 3 — ClickHouse Data Layer**
- [ ] Spin up ClickHouse container with time-series schema
- [ ] Write consumer service to micro-batch write Kafka data into ClickHouse

> **🎯 Definition of Done:** Telemetry generator publishes 1,000+ metrics/sec. ClickHouse consumer ingests in micro-batches with zero drops, verified via `SELECT count() FROM telemetry`.

**🐛 Common AI Bugs**
- ClickHouse Single-Row Inserts — causes "Too many parts in partition"; batch writes in 1,000-row chunks
- Kafka Consumer Group Offset Loops — forgetting to commit offsets / misconfigured `auto.offset.reset` re-reads entire log history on restart

---

## 🟡 Week 2: Machine Learning Anomaly Detection

**Day 4 — Feature Engineering**
- [ ] Calculate rolling averages and standard deviations over time-series windows

**Day 5 — Isolation Forest Model**
- [ ] Train a scikit-learn Isolation Forest on historical metric baselines
- [ ] Output anomaly scores for incoming real-time metric streams

**Day 6 — Predictive Lead-Time Logic**
- [ ] Build threshold rules (e.g. memory growth rate) to flag failures 30 minutes in advance
- [ ] Pin down the exact feature driving the lead-time claim

> **🎯 Definition of Done:** Injected CPU/Memory spikes trigger anomaly scores >0.75, emitting a predictive alert 30 minutes before simulated failure. Precision/recall reported against labeled anomalies.

**🐛 Common AI Bugs**
- Model Retraining in the Ingestion Loop — `fit()` should never run inside the real-time consumer; pre-train, then only `predict`/`decision_function`
- Data Leakage in Rolling Windows — using future data points in rolling std calculations

---

## 🔴 Week 3: Real-Time API & Query Layer

**Day 7 — Node.js WebSocket Server**
- [ ] Set up WebSocket server listening to ML prediction outputs
- [ ] Implement bi-directional ping/pong heartbeat and broadcasting

**Day 8 — ClickHouse Baseline Query Optimization**
- [ ] Write ClickHouse queries comparing live metrics against 10M+ row historical baselines

> **🎯 Definition of Done:** WebSocket broadcasts alerts in <50ms. ClickHouse baseline queries against 10M+ records execute in <150ms.

**🐛 Common AI Bugs**
- Missing WebSocket Heartbeats — no ping/pong leaves zombie TCP connections after disconnects
- PostgreSQL Syntax in ClickHouse — unsupported foreign keys / subqueries

---

## 🔵 Week 4: Multi-Page SRE Dashboard Frontend & Performance Tuning

**Day 9 — Live Command Center (`/dashboard`)**
- [ ] Build 40/60 split: Predictive Alert List (sorted by time-to-failure) + 2×2 ECharts grid
- [ ] Add "Freeze Feed" control

**Day 10 — Root-Cause Inspector (`/incidents/:id`)**
- [ ] Build synchronized crosshair cursor across stacked metric charts
- [ ] Build monospace Log Terminal (`#090a0f` background) with "Show Anomalies Only" toggle

**Day 11 — System Topology Page (`/topology`)**
- [ ] Build service dependency graph with per-node health status
- [ ] Build ML Model Health panel (Drift %, Precision, Average Lead Time) — backed by a real drift job

**Day 12–14 — Performance Profiling & Defense Practice**
- [ ] Verify 60 FPS under 100+ WebSocket msgs/sec via Chrome DevTools
- [ ] Practice explaining row vs. columnar database tradeoffs out loud

> **🎯 Definition of Done:** All 3 pages navigate seamlessly. Frontend sustains 100+ WebSocket updates/sec at 60 FPS with no frame drops or leaks; circular buffers keep browser memory flat.

**🐛 Common AI Bugs**
- State Update React Crash — every WebSocket message hitting `setState` directly causes ~50 re-renders/sec at 50 msgs/sec; batch updates every 500ms, use `useRef` for chart instances
- Chart Memory Leaks — `echarts.init()` without `chart.dispose()` in `useEffect` cleanup fills memory across navigation
