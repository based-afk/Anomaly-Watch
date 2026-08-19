# AGENTS.md — Predictive AI Monitoring Platform

> Read this file in full before making any change to this repository. It defines the project scope, architecture, current phase, and non-negotiable design rules. If a request conflicts with this file, flag the conflict instead of silently overriding it.

## 1. What this project is

An enterprise observability tool: high-throughput telemetry ingested via Redpanda/Kafka, routed to ClickHouse, scored in real time by a Python Isolation Forest model, with alerts streamed to a React dashboard over WebSockets.

This is a portfolio/interview project. Every metric claimed in the Definition of Done (latency, throughput, FPS, precision/recall) must be something you can reproduce live and explain the mechanism behind — not just something that happened once in a demo run. Be ready to explain row vs. columnar database tradeoffs out loud, not just make the system work.

## 2. Stack

- **Streaming:** Redpanda/Kafka, consumer groups, manual offset management
- **Storage:** ClickHouse (columnar, MergeTree-family tables), batched writes only
- **ML:** Python, scikit-learn Isolation Forest — trained offline/on a schedule, never inside the real-time ingestion loop
- **Real-time transport:** Node.js WebSocket server with ping/pong heartbeats
- **Frontend:** React, Apache ECharts (imperative API via `useRef`, not the React wrapper)

## 3. Required pages

| Route | Purpose |
|---|---|
| `/dashboard` | Live Command Center — 40% Predictive Alert Feed (sorted by time-to-failure) / 60% 2×2 Telemetry Metric Grid, plus a "Freeze Feed" control to pause WebSocket updates while inspecting a spike |
| `/incidents/:id` | Root-Cause Inspector — synchronized timeline cursor across stacked metric charts + monospace log terminal (`#090a0f` background) with a "Show Anomalies Only" toggle |
| `/topology` | Service Dependency Graph (health status per node) + ML Model Health & Baseline Drift Panel (shows Drift %, Precision, and Average Lead Time) |

## 4. Correctness rules (do not regress these)

- `IsolationForest.fit()` must never run inside the real-time stream consumer loop. Pre-train the model; the live path only calls `predict`/`decision_function`.
- Rolling statistics (mean, std) must only use past timestamps — never index into future rows of the window. This is data leakage and will silently inflate reported anomaly-detection performance.
- ClickHouse writes must be batched (1,000+ rows). A single-row insert per Kafka event will throw "Too many parts in partition" — this is not an edge case, it will happen quickly under real load.
- Kafka offsets must be committed **after** a successful ClickHouse write, not before (at-least-once delivery) — committing early risks silent data loss if the write fails.
- WebSocket connections must implement bi-directional ping/pong heartbeats alongside message broadcasting. A handler with no heartbeat will accumulate zombie connections on client disconnect/network switch.
- Any anomaly-score threshold (e.g. `>0.75`) reported as a result must be backed by a labeled evaluation set with precision/recall — not just "it fired during the demo." Inject a known, labeled set of anomalies into the synthetic data specifically so this can be scored, not eyeballed.
- Do not write PostgreSQL-style SQL against ClickHouse (no foreign keys, limited subquery support) — verify syntax against ClickHouse docs, not assumed compatibility.
- Kafka consumer must commit offsets correctly and set `auto.offset.reset` deliberately — a consumer group that never commits (or is misconfigured) will re-read the entire topic history on every restart.
- Incoming WebSocket messages must never be pushed directly into React state one-by-one (`setMetrics(prev => [...prev, newMetric])` per message). Batch state updates on a timer (500ms — one `setOption()` call per tick) and keep chart refs in `useRef`. Pushing every message straight into state at 50+ msgs/sec will freeze the tab with re-renders.
- Every `echarts.init()` call must have a matching `chart.dispose()` in the corresponding `useEffect` cleanup function, or memory leaks across page navigation.
- Circular buffers must cap chart data at 500 points max per chart in browser memory, with oldest points dropped automatically — this is what keeps memory flat, not just the batching above.

## 5. Known gaps / open improvements (don't silently "fix" — flag before changing scope)

- The "30 minutes in advance" predictive claim is currently a threshold rule on memory growth rate. The exact feature/formula driving this must be documented in code comments wherever it's implemented — this claim needs to survive being asked "how does it actually know 30 minutes ahead?"
- The Model Health & Baseline Drift panel on `/topology` is a UI target with no backend job behind it yet. Do not fill it with static/mock numbers (Drift %, Precision, Average Lead Time) and consider it done — it needs an actual scheduled drift-recompute job before this page is complete.
- No WebSocket auth exists. Acceptable for this project's scope, but must be stated as an explicit cut in the README, not left unaddressed.
- The 60 FPS / 100+ msgs/sec target is unverified against real `setOption()` call cost — do not assume `useRef` alone guarantees this; state batching (see design rules) is required, not optional. Validate with Chrome DevTools profiling, not assumption.

## 6. Design system — Anti-AI-Slop Rules

This project's visual identity is intentionally the opposite of generic AI-generated UI. Every rule below is enforced, not a suggestion.

**Palette (exact hex, no substitutions):**
- Canvas: `#0d0e11`
- Panel: `#16181d`
- Border: `#23272e`
- Status: Green `#10b981` / Amber `#f59e0b` / Red `#ef4444`
- Log terminal background: `#090a0f`

**Typography:** Inter for UI headers. JetBrains Mono for server names, latency metrics, log outputs.

**Structural rules (reinforced beyond the original brief):**
- **Border radius: 4px maximum, everywhere.** No `rounded-xl`/`rounded-2xl`. This is an SRE tool, not a consumer dashboard.
- **No box-shadows for elevation.** Panels are separated with a 1px solid `#23272e` border. Shadows only on true overlays (modals), single subtle shadow, never a glow.
- **No `hover:scale-*`, no fade-in-on-scroll.** Motion budget is spent entirely on the live telemetry itself (chart updates, alert feed insertion, the synchronized crosshair cursor). If a transition doesn't help you read the data, cut it.
- **Charts: no rainbow gradient fills, no glow around data points or lines.** Use only the fixed status palette (green/amber/red) plus the accent border color for neutral series.
- **Icons functional only** — not paired with every metric label by default.
- **No gradient buttons, no glow-on-hover.**
- **Copy tone:** plain, technical, active voice. Banned words: "unlock," "empower," "seamless," "effortless," "supercharge," "revolutionize," "elevate." Alert and error copy states exactly what triggered and where — no exclamation marks, no "Oops."
- **Accessibility floor:** visible keyboard focus states; respect `prefers-reduced-motion` (this includes the crosshair/chart animations).
- **Strict no-go (original):** zero glassmorphism, no rounded pastel cards, no floating AI sparkles.

## 7. Definition of Done, by phase

- **Week 1 — Ingestion:** telemetry generator publishes 1,000+ metrics/sec to Redpanda; ClickHouse consumer ingests in micro-batches with zero message loss, verified via `SELECT count() FROM telemetry`.
- **Week 2 — ML:** injected CPU/memory spikes trigger anomaly scores >0.75 and a predictive alert ~30 minutes ahead of simulated failure; precision/recall reported against a labeled anomaly set.
- **Week 3 — Real-time API:** WebSocket alerts broadcast in <50ms; ClickHouse baseline queries against 10M+ rows execute in <150ms.
- **Week 4 — Frontend:** all 3 pages navigate seamlessly; sustains 100+ WebSocket updates/sec at 60 FPS (verified in Chrome DevTools) with no frame drops or memory leaks; circular buffers (max 500 points/chart) keep browser memory flat.

## 8. Full bug watchlist (from the original spec — check on every relevant change)

| Area | Bug | Fix |
|---|---|---|
| Ingestion | ClickHouse single-row inserts | Batch writes in 1,000-row chunks — single-row inserts throw "Too many parts" |
| Ingestion | Kafka offset loop | Commit offsets correctly; misconfigured `auto.offset.reset` re-reads full topic history on restart |
| ML | Retraining in the ingestion loop | `fit()` runs offline/pre-trained only; live path calls `predict`/`decision_function` |
| ML | Rolling-window data leakage | Rolling stats use only past timestamps, never future rows |
| ML | Unscored anomaly threshold | No labeled evaluation set behind the `>0.75` claim — inject labeled anomalies, report precision/recall |
| Real-time API | Missing WebSocket heartbeats | Implement ping/pong or zombie connections accumulate on disconnect |
| Real-time API | PostgreSQL syntax in ClickHouse | No foreign keys/unsupported subqueries — verify against ClickHouse docs |
| Real-time API | Offset committed before write succeeds | Commit only after successful ClickHouse write (at-least-once delivery) |
| Frontend | State update crash | Batch WebSocket messages into state every 500ms, not per-message |
| Frontend | Chart memory leaks | Pair every `echarts.init()` with `chart.dispose()` in cleanup |
| Frontend | Unbounded chart memory growth | Circular buffer caps chart data at 500 points, oldest dropped automatically |
| Frontend | AI "sparkle" UI slop | No glassmorphism, no pastel cards, no floating sparkles — see Section 6 |

## 9. Day-by-day build sequence (reference)

Use this as the intended order of work; treat each day's checkbox as a sub-task of that week's Definition of Done in §7.

**Week 1 — Data Ingestion & Streaming Infrastructure**
- Day 1: Synthetic telemetry generator — mock CPU/memory/error-rate metrics with timestamps. *Improvement: inject a known, labeled set of anomalies alongside normal data for later precision/recall scoring.*
- Day 2: Spin up Redpanda via Docker Compose; create a telemetry topic and publish mock events to it.
- Day 3: Spin up ClickHouse with a time-series schema; write a consumer service that micro-batches Kafka data into ClickHouse.

**Week 2 — Machine Learning Anomaly Detection**
- Day 4: Feature engineering — rolling averages and standard deviations over time-series windows.
- Day 5: Train a scikit-learn Isolation Forest on historical baselines; output anomaly scores for incoming real-time streams.
- Day 6: Predictive lead-time logic — threshold rules (e.g. memory growth rate trend) flagging failure warnings 30 minutes in advance. *Improvement: pin down the exact feature driving the lead-time claim so it can be defended, not just demoed.*

**Week 3 — Real-Time API & Query Layer**
- Day 7: Node.js WebSocket server listening to ML prediction outputs; bi-directional ping/pong heartbeat and message broadcasting.
- Day 8: ClickHouse queries comparing live metrics against historical 10M+ row baselines.

**Week 4 — Multi-Page SRE Dashboard Frontend & Performance Tuning**
- Day 9: `/dashboard` — 40/60 split view (Predictive Alert List sorted by time-to-failure + 2×2 ECharts Telemetry Grid); "Freeze Feed" stream control.
- Day 10: `/incidents/:id` — synchronized crosshair cursor across stacked metric charts; monospace log terminal (`#090a0f` background) with "Show Anomalies Only" toggle.
- Day 11: `/topology` — service dependency graph with per-node health status; ML Model Health panel (Drift %, Precision, Average Lead Time). *Improvement: back this panel with an actual scheduled drift-recompute job, not static/mock numbers.*
- Day 12–14: Performance profiling and defense practice — verify 60 FPS under 100+ WebSocket msgs/sec via Chrome DevTools; practice explaining row vs. columnar database tradeoffs out loud.

## 10. Before you touch code

1. Identify which phase (ingestion / ML / real-time API / frontend) the change belongs to, and re-read the relevant Definition of Done in §7 and, if useful, the corresponding day(s) in §9.
2. Check the change against §4 (correctness rules), §6 (design rules), and §8 (bug watchlist) before writing it.
3. If a change touches a "known gap" in §5, state that explicitly rather than quietly expanding scope or faking the missing piece with mock data.
