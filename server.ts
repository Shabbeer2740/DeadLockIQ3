/**
 * DeadLockIQ Full-Stack Concurrency Proxy & WebSocket Server
 *
 * Integrates:
 * - Express REST API
 * - WebSocket Server on /ws
 * - Real-time state broadcasting
 * - Prometheus metrics
 * - Vite middleware integration (on port 3000)
 */

import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';
import { transactionManager } from './src/server/transactionManager.ts';
import { lockCoordinator, redisLockEngine } from './src/server/lockCoordinator.ts';
import { waitForGraph } from './src/server/waitForGraph.ts';
import { recoveryEngine } from './src/server/recoveryEngine.ts';
import { scenarioSimulator, ScenarioType } from './src/server/simulator.ts';
import { benchmarkEngine } from './src/server/benchmark.ts';
import { metricsManager } from './src/server/metricsManager.ts';
import { postgresLockMonitor } from './src/server/postgresMonitor.ts';
import { fencingManager } from './src/server/fencing.ts';
import { WebSocketEvent, WSEventType } from './src/types/index.ts';

const app = express();
const port = Number(process.env.PORT) || 3000;
app.use(express.json({ limit: '32kb' }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const supportedScenarios = new Set<ScenarioType>([
  'TWO_WAY_DEADLOCK', 'THREE_WAY_DEADLOCK', 'N_WAY_DEADLOCK', 'NORMAL_CONTENTION',
  'MULTIPLE_DEADLOCKS', 'STALE_WORKER', 'FENCING_VIOLATION', 'RETRY_STORM',
  'POSTGRES_DEADLOCK', 'REDIS_FAILURE', 'WORKER_CRASH', 'NETWORK_DELAY', 'CROSS_SYSTEM_DEADLOCK',
]);

// Active WebSocket clients set
const clients = new Set<WebSocket>();

function broadcast(type: WSEventType, payload: any) {
  const event: WebSocketEvent = {
    type,
    payload,
    timestamp: Date.now(),
  };
  const message = JSON.stringify(event);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

// Hook up state callbacks to WebSockets
transactionManager.setOnStateChange((tx, oldState, newState) => {
  if (oldState === 'CREATED' && newState === 'CREATED') {
    waitForGraph.registerTransaction(tx.id, tx.workerId, tx.state, tx.priority, tx.effectivePriority, tx.fencingToken);
    broadcast('TRANSACTION_CREATED', { tx, oldState, newState });
  } else {
    waitForGraph.updateTransactionState(tx.id, newState, tx.effectivePriority);
    if (newState === 'COMPLETED' || newState === 'FAILED') waitForGraph.removeTransaction(tx.id);
    broadcast('TRANSACTION_STATE_CHANGED', { tx, oldState, newState });
  }
  broadcast('GRAPH_EDGE_ADDED', waitForGraph.getSnapshot());
  broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
});

lockCoordinator.setOnLockEvent(evt => {
  broadcast(evt.type as WSEventType, evt);
  broadcast('GRAPH_EDGE_ADDED', waitForGraph.getSnapshot());
  broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
});

recoveryEngine.setCallbacks(
  incident => {
    broadcast('DEADLOCK_DETECTED', incident);
    broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
  },
  (txId, retryCount) => {
    broadcast('RETRY_STARTED', { txId, retryCount });
  }
);

scenarioSimulator.setLogEmitter((message, level = 'info') => {
  broadcast('LOG_EVENT', { message, level, timestamp: Date.now() });
});

wss.on('connection', ws => {
  clients.add(ws);
  metricsManager.setWsClientCount(clients.size);

  // Send initial snapshot on connect
  const initialData: WebSocketEvent = {
    type: 'INIT_STATE',
    payload: {
      transactions: transactionManager.getAllTransactions(),
      locks: redisLockEngine.getAllLocks(),
      graph: waitForGraph.getSnapshot(),
      incidents: recoveryEngine.getAllIncidents(),
      metrics: metricsManager.getSnapshot(),
      postgresLocks: postgresLockMonitor.getObservedLocks(),
      postgresHasDeadlock: postgresLockMonitor.isDeadlockObserved(),
      fencingHistory: fencingManager.getHistory(),
    },
    timestamp: Date.now(),
  };

  ws.send(JSON.stringify(initialData));

  ws.on('close', () => {
    clients.delete(ws);
    metricsManager.setWsClientCount(clients.size);
  });
});

// Periodic broadcast of graph snapshot & live metrics
const periodicBroadcast = setInterval(() => {
  if (clients.size > 0) {
    broadcast('GRAPH_EDGE_ADDED', waitForGraph.getSnapshot());
    broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
    broadcast('POSTGRES_LOCKS_UPDATED', {
      locks: postgresLockMonitor.getObservedLocks(),
      status: postgresLockMonitor.getStatus(),
      hasDeadlock: postgresLockMonitor.isDeadlockObserved(),
    });
  }
}, 1000);

async function shutdown() {
  clearInterval(periodicBroadcast);
  recoveryEngine.reset();
  for (const client of wss.clients) client.close(1001, 'Server shutting down');
  await new Promise<void>(resolve => server.close(() => resolve()));
  await postgresLockMonitor.close();
  redisLockEngine.close();
}

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    void shutdown().catch(() => {
      process.exitCode = 1;
    });
  });
}

// ==========================================
// REST API ROUTES
// ==========================================

// Health Check
app.get('/api/v1/health', (req, res) => {
  res.json({
    status: 'HEALTHY',
    readiness: redisLockEngine.getLockMode() === 'LIVE_REDIS'
      ? 'READY'
      : redisLockEngine.getLockMode() === 'IN_MEMORY_SIMULATION'
        ? 'SIMULATION'
        : 'DEGRADED',
    service: 'DeadLockIQ Distributed Concurrency Engine',
    uptime: process.uptime(),
    timestamp: Date.now(),
    redis: redisLockEngine.getConnectionState(),
    redisConnectionState: redisLockEngine.getConnectionState(),
    redisConnectionError: redisLockEngine.getLastConnectionError(),
    lockMode: redisLockEngine.getLockMode(),
    postgresObserver: postgresLockMonitor.getStatus(),
    activeWorkers: clients.size,
  });
});

// Prometheus OpenMetrics
app.get('/metrics', (req, res) => {
  res.setHeader('Content-Type', 'text/plain; version=0.0.4');
  res.send(metricsManager.toPrometheusFormat());
});

// Metrics JSON Snapshot
app.get('/api/v1/metrics', (req, res) => {
  res.json(metricsManager.getSnapshot());
});

// Transactions API
app.get('/api/v1/transactions', (req, res) => {
  res.json({
    transactions: transactionManager.getAllTransactions(),
    total: transactionManager.getAllTransactions().length,
  });
});

app.get('/api/v1/transactions/:id', (req, res) => {
  const tx = transactionManager.getTransaction(req.params.id);
  if (!tx) {
    return res.status(404).json({ error: 'Transaction not found' });
  }
  res.json(tx);
});

app.post('/api/v1/transactions', (req, res) => {
  const { id, workerId, priority, maxRetries, workDoneUnits, rollbackCost } = req.body;
  if (typeof id !== 'string' || !id.trim() || id.length > 128 || typeof workerId !== 'string' || !workerId.trim() || workerId.length > 128) {
    return res.status(400).json({ error: 'id and workerId are required' });
  }
  if ([priority, maxRetries, workDoneUnits, rollbackCost].some(value => value !== undefined && (typeof value !== 'number' || !Number.isFinite(value) || value < 0))) {
    return res.status(400).json({ error: 'priority, maxRetries, workDoneUnits, and rollbackCost must be non-negative finite numbers' });
  }
  if (maxRetries !== undefined && !Number.isSafeInteger(maxRetries)) {
    return res.status(400).json({ error: 'maxRetries must be a safe integer' });
  }
  if (transactionManager.getTransaction(id)) return res.status(409).json({ error: 'Transaction id already exists' });
  const tx = transactionManager.createTransaction({
    id,
    workerId,
    priority,
    maxRetries,
    workDoneUnits,
    rollbackCost,
  });
  res.status(201).json(tx);
});

// Locks API
app.post('/api/v1/locks/acquire', async (req, res) => {
  const { resourceId, txId, workerId, ttlMs } = req.body;
  if ([resourceId, txId, workerId].some(value => typeof value !== 'string' || !value.trim() || value.length > 128)) {
    return res.status(400).json({ error: 'resourceId, txId, and workerId are required' });
  }
  if (ttlMs !== undefined && (!Number.isSafeInteger(ttlMs) || ttlMs <= 0 || ttlMs > 86400000)) {
    return res.status(400).json({ error: 'ttlMs must be an integer between 1 and 86400000' });
  }
  try {
    const result = await lockCoordinator.acquireLock(resourceId, txId, workerId, ttlMs);
    const status = result.success ? 200 : result.waiting ? 202 : result.error?.startsWith('REDIS_OFFLINE') ? 503 : 409;
    res.status(status).json(result);
  } catch {
    res.status(500).json({ success: false, message: 'Lock acquisition failed' });
  }
});

app.post('/api/v1/locks/release', async (req, res) => {
  const { resourceId, txId, fencingToken } = req.body;
  if (typeof resourceId !== 'string' || !resourceId.trim() || resourceId.length > 128 || typeof txId !== 'string' || !txId.trim() || txId.length > 128) {
    return res.status(400).json({ error: 'resourceId and txId are required' });
  }
  if (fencingToken !== undefined && (!Number.isSafeInteger(fencingToken) || fencingToken <= 0)) {
    return res.status(400).json({ error: 'fencingToken must be a positive safe integer' });
  }
  try {
    const result = await lockCoordinator.releaseLock(resourceId, txId, fencingToken);
    const status = result.success ? 200 : result.error?.startsWith('REDIS_OFFLINE') ? 503 : result.error?.startsWith('LOCK_NOT_FOUND') ? 404 : 409;
    res.status(status).json(result);
  } catch {
    res.status(500).json({ success: false, message: 'Lock release failed' });
  }
});

app.post('/api/v1/locks/validate', async (req, res) => {
  const { resourceId, fencingToken } = req.body;
  if (typeof resourceId !== 'string' || !resourceId.trim() || resourceId.length > 128 || !Number.isSafeInteger(fencingToken) || fencingToken <= 0) {
    return res.status(400).json({ valid: false, error: 'resourceId and a positive safe-integer fencingToken are required' });
  }
  const result = await redisLockEngine.validateFencingToken(resourceId, fencingToken);
  const status = result.valid ? 200 : result.reason?.startsWith('REDIS_OFFLINE') ? 503 : 409;
  res.status(status).json(result);
});

// Graph API
app.get('/api/v1/graph', (req, res) => {
  res.json(waitForGraph.getSnapshot());
});

// Deadlocks & Incidents API
app.get('/api/v1/deadlocks', (req, res) => {
  res.json({
    incidents: recoveryEngine.getAllIncidents(),
    total: recoveryEngine.getAllIncidents().length,
  });
});

app.get('/api/v1/deadlocks/:id', (req, res) => {
  const inc = recoveryEngine.getIncident(req.params.id);
  if (!inc) {
    return res.status(404).json({ error: 'Incident not found' });
  }
  res.json(inc);
});

// PostgreSQL Lock Observer
app.get('/api/v1/postgres/locks', (req, res) => {
  res.json({
    locks: postgresLockMonitor.getObservedLocks(),
    hasDeadlock: postgresLockMonitor.isDeadlockObserved(),
    status: postgresLockMonitor.getStatus(),
  });
});

// Simulator Control APIs
app.post('/api/v1/simulator/scenarios/:scenario', async (req, res) => {
  const scenario = req.params.scenario as ScenarioType;
  const timeoutMs = 30000;
  if (!supportedScenarios.has(scenario)) {
    return res.status(400).json({ success: false, message: `Unknown scenario: ${req.params.scenario}`, timedOut: false });
  }

  try {
    const result = await Promise.race([
      scenarioSimulator.runScenario(scenario),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`Scenario ${scenario} timed out after ${timeoutMs}ms`)), timeoutMs)),
    ]);
    if (!result.success) {
      const status = result.timedOut ? 504 : 500;
      return res.status(status).json(result);
    }
    res.json(result);
  } catch (error: any) {
    const message = error?.message || `Scenario ${scenario} failed unexpectedly`;
    const timedOut = /timed out|timeout/i.test(message);
    res.status(timedOut ? 504 : 500).json({
      success: false,
      message,
      timedOut,
    });
  }
});

app.post('/api/v1/simulator/reset', async (req, res) => {
  if (scenarioSimulator.getIsRunning()) {
    return res.status(409).json({ success: false, message: 'Cannot reset while a scenario is executing' });
  }
  try {
    await scenarioSimulator.resetAll();
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Simulator reset failed';
    return res.status(503).json({ success: false, message });
  }
  broadcast('INIT_STATE', {
    transactions: [],
    locks: [],
    graph: waitForGraph.getSnapshot(),
    incidents: [],
    metrics: metricsManager.getSnapshot(),
    postgresLocks: postgresLockMonitor.getObservedLocks(),
    postgresHasDeadlock: postgresLockMonitor.isDeadlockObserved(),
    fencingHistory: [],
  });
  res.json({ success: true, message: 'All engine state, locks, and graphs reset successfully' });
});

// Benchmark API
app.post('/api/v1/benchmark/run', async (req, res) => {
  const workers = req.body.workers === undefined ? 500 : req.body.workers;
  if (!Number.isSafeInteger(workers) || workers < 1 || workers > 2500) {
    return res.status(400).json({ error: 'workers must be an integer between 1 and 2500' });
  }
  try {
    const result = await benchmarkEngine.runBenchmark(workers);
    res.json(result);
  } catch {
    res.status(500).json({ error: 'Benchmark failed' });
  }
});

app.use((error: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  if (res.headersSent) return next(error);
  const status = Number.isInteger(error?.status) && error.status >= 400 && error.status < 500 ? error.status : 500;
  res.status(status).json({ success: false, message: status === 400 ? 'Invalid request body' : 'Request could not be processed' });
});

// ==========================================
// VITE SPA MIDDLEWARE / STATIC ASSETS
// ==========================================
async function startServer() {
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static('dist'));
  }

  server.listen(port, '0.0.0.0', () => {
    console.log(`[DeadLockIQ] Server running at http://0.0.0.0:${port}`);
    console.log(`[DeadLockIQ] WebSocket server live at ws://0.0.0.0:${port}/ws`);
  });
}

startServer().catch(err => {
  console.error('[DeadLockIQ] Failed to start server:', err);
});
