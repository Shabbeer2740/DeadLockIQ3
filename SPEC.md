# DeadLockIQ Technical Specification

## 1. Architecture & Limitations
DeadLockIQ is a single-coordinator distributed deadlock detection and recovery proxy prototype.
**Backend**: Node.js/TypeScript (Express + WebSocket); `backend/` is a separate Go reference implementation.
**Frontend**: React + TypeScript (Real-time Dashboard)
**Dependencies**: Real Redis (compatible instance), Real PostgreSQL.

**Limitations**:
- This is a single-coordinator prototype.
- **NOT** Highly Available (HA). Coordinator failure is not automatically recovered.
- Does not implement distributed consensus for multiple coordinators.
- PostgreSQL lock observations are not integrated into the shared wait-for graph or recovery cancellation; cross-system recovery is unsupported.
- Redis outages fail lock requests closed unless explicit single-process simulation mode is selected.
- Not production-ready.

## 2. Transaction States
- `WAITING`: Transaction is blocked waiting for a lock.
- `ACQUIRED`: Transaction successfully acquired the lock.
- `DEADLOCK_DETECTED`: Transaction is part of an identified cycle.
- `VICTIM_SELECTED`: Transaction chosen to be aborted to break a deadlock.
- `ABORTING`: Cancellation signals sent to the worker.
- `ROLLED_BACK`: Actual database rollback completed, and locks released.
- `BACKOFF`: Transaction is waiting (exponential backoff + jitter) before retry.
- `RETRYING`: Transaction is re-attempting acquisition.
- `FAILED`: Transaction exhausted max retries.
- `COMPLETED`: Transaction finished successfully.

## 3. Lock Management (Redis)
- Uses real Redis (e.g., via `ioredis` or `go-redis`).
- **TTL/Lease**: Locks expire automatically if the worker crashes (TTL).
- **Fencing Tokens**: Monotonically increasing sequence number per resource, generated on lock acquisition.
- **Ownership Validation**: Atomic check-and-release via Lua script ensures only the current owner can release the lock.
- **Stale Owner Protection**: If a worker stalls and its TTL expires, a new worker acquires the lock. The stale worker's attempt to release it later will fail.

## 4. Wait-For Graph (WFG) Semantics & Registration Race
- Directed graph where nodes are transactions and edges represent dependencies (`TxA -> TxB` means `TxA` is waiting on a lock held by `TxB`).
- **Wait-Registration Race Handling**: 
  1. Attempt Redis acquisition.
  2. If failed, retrieve current holder.
  3. Revalidate holder still owns the lock.
  4. Only then, add edge to WFG.
  5. On successful acquisition, immediately remove any wait edge.

## 5. Cycle Detection & Revalidation
- **Incremental Detection**: Triggered on edge addition via BFS reachability check.
- **Revalidation**: Immediately before victim selection, EVERY edge in the candidate cycle is re-verified against the underlying Redis/PostgreSQL state. If any dependency is broken (e.g. lock released, TTL expired), the cycle is discarded and no victim is killed.

## 6. Victim Selection
- Deterministic, explainable scoring algorithm.
- Lowest score = selected victim.
- Formula: `(EffectivePriority * 5.0) + (LocksHeld * 4.0) + (WorkDone * 2.0) + (Retries * 15.0) - (RollbackCost * 1.5)`
- **Priority Aging**: `EffectivePriority = BasePriority + (WaitTimeMs / AgingFactor)` prevents starvation.

## 7. Cancellation & Rollback
- **Cancellation**: The Node prototype models victim rollback and retry; it does not implement a distributed worker-context cancellation protocol.
- **PostgreSQL Rollback**: The application layer explicitly executes a `ROLLBACK` on the real PostgreSQL transaction, releasing row/table locks. Redis locks are explicitly released. Graph edges are removed.

## 8. Retry, Backoff, and Jitter
- Uses exponential backoff: `delay = min(MaxDelay, BaseDelay * 2^RetryCount) + Jitter`.
- Jitter disperses retry storms.
- Ensures fresh lock/wait states on retry.

## 9. PostgreSQL Integration
- Monitored via `pg_locks` and `pg_stat_activity` using real `pgx` driver.
- Application identity set via `application_name = deadlockiq-<tx_id>`.
- PostgreSQL dependencies are observed separately and are not integrated into the wait-for graph; real cross-system cycle detection is not implemented.

## 10. Failure Handling
- **Worker Crash**: Lock TTL expires, lock becomes available.
- **Stale Worker**: Rejected upon release/renewal attempt due to mismatched fencing token/owner check.
- **Telemetry Failure**: Lock path is independent; continues functioning.
- **Coordinator Failure**: Entire system state halts; manual/external restart required (Limitation).

## 11. Observability & WebSockets
- **Prometheus Metrics**: `active_transactions`, `waiting_transactions`, `deadlocks_detected_total`, `lock_acquisition_latency`, etc.
- **WebSockets**: Real-time broadcast of graph state, metrics, and timeline events for the frontend.

## 12. Testing Requirements
- Comprehensive race testing (`go test -race ./...`).
- Deterministic scenarios: 2-way, 3-way, N-way, contention without deadlock, cross-system deadlock, and revalidation race.
