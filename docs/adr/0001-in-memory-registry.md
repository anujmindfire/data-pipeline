# ADR 0001 — In-Memory Job Progress Registry

**Status:** Accepted  
**Date:** 2026-06-15  

---

## Context

The pipeline server must expose real-time progress for running jobs (e.g. `processed_records`, `failed_records`, `processing_rate`, `stage_latencies`) via the `GET /pipelines/{id}/progress` REST endpoint.

Three implementation options were evaluated:

| Option | Description | Latency | Complexity |
|---|---|---|---|
| **A — DB polling** | Read counters from PostgreSQL on every API call | High (~5–50 ms per query) | Low |
| **B — External cache** | Push counters to Redis / Memcached, API reads from cache | Low (~1 ms) | High (requires infra) |
| **C — In-memory registry** | Maintain a `sync.RWMutex` guarded `map` inside the server process | Lowest (~µs) | Low |

---

## Decision

**Option C** — a package-level in-memory registry (`service.GlobalRegistry`) was chosen.

The registry stores a `*JobProgressTracker` per job ID under a `sync.RWMutex`, allowing concurrent read access from the HTTP layer and exclusive write access from the pipeline goroutines. A throttled background goroutine (500 ms ticker) flushes counters to PostgreSQL for durability and cancellation polling — but API reads never touch the database for progress data.

```
┌─────────────────────────────────────────────────────┐
│ Pipeline goroutines ──write──► JobProgressTracker    │
│                                     ▲               │
│ HTTP GET /progress ─read──►  GlobalRegistry (µs)    │
│                                     │               │
│ 500 ms ticker ──────────────► PostgreSQL (durability)│
└─────────────────────────────────────────────────────┘
```

---

## Consequences

### Positive
- **Zero-latency reads.** Progress API responses are sub-millisecond regardless of database load.
- **No additional infrastructure.** Avoids a Redis dependency, simplifying deployment.
- **Simple concurrency model.** `sync.RWMutex` is idiomatic Go; no channels or additional goroutines needed for reads.

### Negative / Trade-offs
- **Not distributed.** If the server is horizontally scaled to multiple instances, each holds its own registry. A job running on instance A will return `404 Not Found` for a progress request routed to instance B. Mitigation: sticky sessions or API gateway routing by `job_id`.
- **State lost on crash.** In-memory state is ephemeral. If the server restarts mid-job, in-flight progress is gone. The 500 ms DB flush ensures final and intermediate counts are persisted; a restarted server falls back to DB data on the `GET /progress` endpoint.
- **Memory growth.** Long-running servers accumulate stale registry entries for completed jobs. Mitigation: registry entries are candidates for eviction once the job reaches a terminal state (`COMPLETED`, `FAILED`, `CANCELLED`).

---

## Alternatives Considered and Rejected

### Redis / Memcached (Option B)
Rejected because it introduces an external dependency, requires connection pool management, and adds operational overhead (HA Redis). The throughput of a single server instance does not justify this complexity.

### Pure DB polling (Option A)
Rejected because high-frequency `GET /progress` calls under dashboard load would stress PostgreSQL and introduce noticeable latency on the progress API response.

---

## Related Files

- [`service/pipeline.go`](../../apps/server/service/pipeline.go) — `GlobalRegistry`, `Registry`, `JobProgressTracker`
- [`controller/pipeline.go`](../../apps/server/controller/pipeline.go) — `GetProgress` handler that reads from registry first, then falls back to DB
