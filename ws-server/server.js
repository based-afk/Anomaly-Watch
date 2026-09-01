const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const { createClient } = require('@clickhouse/client');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const clickhouse = createClient({
  url: 'http://localhost:8123',
  database: 'observability',
});

// WebSocket strict ping/pong heartbeat implementation
function heartbeat() {
  this.isAlive = true;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', heartbeat);
  console.log('New WebSocket client connected');

  ws.on('close', () => {
    console.log('WebSocket client disconnected');
  });
});

// Interval to ping clients and terminate broken connections
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log('Terminating zombie connection');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 10000); // Check every 10 seconds

wss.on('close', () => {
  clearInterval(interval);
});

// Endpoint for the Python live predictor to push metrics to
app.post('/api/ingest', (req, res) => {
  const data = req.body;
  // data is expected to be an array of metrics or a single metric object
  const messages = Array.isArray(data) ? data : [data];
  
  const payload = JSON.stringify(messages);
  
  // Broadcast to all connected clients
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });

  res.status(200).send({ success: true, count: messages.length });
});

// Day 8: Baseline Query Endpoint
// Compares live metrics against historical baselines (simulating 10M+ rows)
app.get('/api/baselines', async (req, res) => {
  try {
    const query = `
      SELECT 
        service,
        avg(cpu_usage) as baseline_cpu,
        stddevPop(cpu_usage) as std_cpu,
        avg(memory_usage) as baseline_mem,
        stddevPop(memory_usage) as std_mem,
        count() as row_count
      FROM telemetry
      GROUP BY service
    `;
    const resultSet = await clickhouse.query({ query });
    const data = await resultSet.json();
    res.json(data.data);
  } catch (error) {
    console.error('ClickHouse Error:', error);
    res.status(500).json({ error: 'Failed to fetch baselines' });
  }
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
