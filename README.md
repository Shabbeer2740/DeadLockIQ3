# DeadLockIQ: Distributed Concurrency & Deadlock Resolution Platform

> **DETECT → EXPLAIN → RESOLVE → RETRY → PROVE**

DeadLockIQ is a single-coordinator concurrency and deadlock-recovery prototype. It tracks transaction dependencies in an in-memory Wait-For Graph (WFG), detects cycles, chooses a victim, and demonstrates rollback/retry. Redis locking fails closed when Redis is unavailable; an in-memory simulator is available only when explicitly enabled. PostgreSQL waits are observed separately and are not integrated into cross-system recovery.

---

## 1. Architectural Highlights

- **Redis Lock Engine**: Namespaced lease keys, TTLs, Redis-generated fencing tokens, and atomic acquire/release when Redis is connected. Offline distributed operations fail closed; explicit in-memory simulation is process-local.
- **Monotonic Fencing Tokens**: 64-bit monotonically increasing sequence numbers issued with every lease grant. Stale workers delayed by GC or network partitions are blocked from mutating protected resources.
- **In-Memory Wait-For Graph (WFG)**: High-speed directed dependency graph tracking active locks and wait edges.
- **Incremental Cycle Detection**: Incremental reachability algorithm triggered on edge addition (`TxA -> TxB`) checking if `TxB` can reach `TxA`. Achieves sub-millisecond detection without full O(V+E) graph scans on uncontended locks.
- **Explainable Multi-Factor Victim Selection**:
  $$\text{Score} = (\text{Priority}_{\text{eff}} \times 5.0) + (|\text{Locks}| \times 4.0) + (\text{Work} \times 2.0) + (\text{Retries} \times 15.0) - (\text{RollbackCost} \times 1.5)$$
  Dynamic priority aging prevents starvation. The transaction with the lowest survival score is chosen, and a natural language data-driven explanation is generated comparing all candidates.
- **Automated Rollback & Jittered Retry Engine**:
  $$\text{Delay} = \min(\text{MaxDelay}, \text{BaseDelay} \times 2^{\text{RetryCount}}) + \text{Jitter}$$
  Jitter can spread retries under contention; progress depends on the lock and transaction state being released correctly.
- **PostgreSQL Lock Observation**: Polls `pg_locks` and `pg_stat_activity` when PostgreSQL is available. The `POSTGRES_DEADLOCK` demo is simulated and does not prove live PostgreSQL deadlock detection.
- **Live Reactive Dashboard**: Interactive React Flow visualizer with animated dependency edges, pulsing deadlock cycle highlights, live metrics, and real-time streaming WebSocket event logs.

---

## 2. Scenario Catalog

1. **Two-Way Deadlock**: Classic circular wait between 2 transactions on 2 resources.
2. **Three-Way Deadlock**: Triangular circular wait ring (`Tx1 -> ResB -> Tx2 -> ResC -> Tx3 -> ResA -> Tx1`).
3. **N-Way Deadlock**: 5-worker circular dependency ring.
4. **Normal Contention**: Queues on a shared resource without creating a deadlock cycle.
5. **Multiple Deadlocks**: Creates two independent two-transaction cycles.
6. **Stale Worker**: Expires a lease and demonstrates rejection of an older fencing token.
7. **Fencing Violation**: Runs the stale-worker fencing demonstration.
8. **Retry Storm**: Exercises concurrent workers contending for one resource and jittered retry behavior.
9. **PostgreSQL Deadlock**: Seeds simulated PostgreSQL lock records; this is not a live database deadlock test.
10. **Redis Failure**: Injects a simulated Redis failure. This is separate from testing a real Redis outage.
11. **Worker Crash**: Lets a lease expire after its owner stops releasing it.
12. **Network Delay**: Injects synthetic latency into lock operations.
13. **Cross-System Deadlock**: Unsupported until PostgreSQL wait edges and worker cancellation are integrated into the shared recovery protocol. The scenario returns an explicit error without modifying a database.

---

## 3. Quick Start

### Docker Compose

The backend requires explicit `REDIS_URL` and `DATABASE_URL`. Compose starts the backend without requiring a local Redis service; the local Redis, PostgreSQL, and monitoring containers are opt-in profiles:

```powershell
$env:REDIS_URL = 'rediss://default:<ROTATED_UPSTASH_TOKEN>@<UPSTASH_HOST>:6379'
$env:DATABASE_URL = '<POSTGRES_CONNECTION_STRING>'
docker compose up -d --build
```

For local PostgreSQL, activate `local-postgres` and set `POSTGRES_PASSWORD`; use `postgres` as the host in `DATABASE_URL` from inside Compose. For local Redis, activate `local-redis` and set `REDIS_URL=redis://redis:6379`. For Prometheus/Grafana, activate `observability` and set `GRAFANA_ADMIN_PASSWORD`. Keep credentials in ignored `.env` or a secret manager, never in tracked configuration. The root `Dockerfile` builds the existing Node/TypeScript server.

- Application: `http://localhost:3000`
- Redis: `localhost:6379`
- PostgreSQL: `localhost:5432`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`

Check service state with `docker compose ps` and logs with `docker compose logs backend postgres`.

### Running the Node/TypeScript Application

```bash
npm install
npm run dev
```

The application listens on port `3000` and exposes WebSockets at `/ws`.

For live Upstash locking, configure the rotated URL and start the app:

```powershell
$env:REDIS_URL = 'rediss://default:<UPSTASH_TOKEN>@<UPSTASH_HOST>:6379'
$env:REDIS_TLS = 'true'
npm run dev
```

Replace the placeholders with values from your provider; do not commit credentials. A `rediss://` URL automatically enables TLS. Set `DATABASE_URL` separately if PostgreSQL monitoring is needed.

Without `REDIS_URL`, the app reports `REDIS_OFFLINE` and fails lock requests closed. For isolated local demonstrations only, explicitly set `$env:LOCK_MODE = 'IN_MEMORY_SIMULATION'`; that mode is process-local and is not a distributed lock implementation. PostgreSQL remains independently `OFFLINE` unless configured and reachable.

---

## 4. REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/health` | Process liveness, readiness, Redis mode, PostgreSQL status, and worker count |
| `GET` | `/metrics` | Prometheus OpenMetrics exporter endpoint |
| `GET` | `/api/v1/metrics` | JSON snapshot of P50, P95, P99 latencies & counters |
| `GET` | `/api/v1/transactions` | List all active and completed transactions |
| `POST` | `/api/v1/transactions` | Register a new transaction with priority |
| `POST` | `/api/v1/locks/acquire` | Acquire distributed lock with lease TTL |
| `POST` | `/api/v1/locks/release` | Atomically release lock verifying fencing token |
| `POST` | `/api/v1/locks/validate` | Validate a fencing token against the active lock authority |
| `GET` | `/api/v1/graph` | Wait-For Graph snapshot formatted for React Flow |
| `GET` | `/api/v1/deadlocks` | List all recorded deadlock incidents |
| `POST` | `/api/v1/simulator/scenarios/:name` | Run one of the 13 scenarios |
| `POST` | `/api/v1/simulator/reset` | Reset all engine state, locks, and graphs |
| `POST` | `/api/v1/benchmark/run` | Run a Redis-backed sequential latency/overhead benchmark |

Scenario names are uppercase identifiers, for example `TWO_WAY_DEADLOCK` and `NORMAL_CONTENTION`. Reset between independent scenario runs:

```powershell
Invoke-RestMethod -Method Post http://localhost:3000/api/v1/simulator/reset
Invoke-RestMethod -Method Post http://localhost:3000/api/v1/simulator/scenarios/TWO_WAY_DEADLOCK
```

Reset applies to this server process and releases only the Redis leases it locally owns. It does not flush the Redis database. Reset is rejected during scenario execution and returns an error if owned live leases cannot be verified as released. Check the response body, transaction states, graph, incidents, and metrics after each run. HTTP success alone does not prove recovery completed.

Useful inspection endpoints include `/api/v1/health`, `/api/v1/transactions`, `/api/v1/graph`, `/api/v1/deadlocks`, `/api/v1/metrics`, and `/api/v1/postgres/locks`.

---

## 5. Go Backend Reference Implementation

The Go reference codebase is located in `backend/`:
- `backend/internal/fencing/`: Monotonic 64-bit atomic sequence generator.
- `backend/internal/graph/`: Concurrency-safe Wait-For Graph using `sync.RWMutex` with BFS incremental reachability.
- `backend/tests/`: Unit tests with `-race` support (`go test -race ./...`).

---

## 6. Verification

Run frontend checks from the repository root:

```powershell
npx tsc --noEmit
npm run build
```

Run Go checks from `backend/`:

```powershell
go test ./...
go test -race ./...
go build ./...
```

These commands verify compilation and tests, not live Redis/PostgreSQL connectivity or browser scenario recovery. Record measured performance only from a fresh benchmark run in the target environment; no fixed performance figures are claimed here.
