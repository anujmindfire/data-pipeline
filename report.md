# Golang Concurrent Data Processing Pipeline - Design & Trade-offs Report

## 1. System Design Choices

*   **Linear Channel Coupling**: We designed a unidirectional pipeline stages topology:
    `Ingestion ➜ Validation ➜ Transformation ➜ Aggregation ➜ Export`.
    Stages are linked by buffered Go channels. Channel buffering is set to a moderate size (500) to act as a shock absorber, smoothing out ingestion spikes while preventing out-of-memory issues from deep backlogs.
*   **Dynamic Unified Schema**: Records are mapped to a unified `map[string]any` structure wrapped in a structured `Record` object containing metadata (ID, source, validation state, timestamps). This schema mapping translates heterogeneous formats (CSV, JSON, REST APIs) into a consistent internal model.
*   **GORM PostgreSQL Persistence**: We selected PostgreSQL (via `gorm.io/driver/postgres`) as our persistent store. This supports robust relational tracking, concurrent multi-worker job updates, and clean relational schemas for historical tracking, failed record audits, and aggregation results.
*   **Streaming File Exports**: To support large-scale files (e.g., millions of records) without out-of-memory (OOM) risks, validation, transformation, and exports are fully streamed. Transformed records are written directly to disk (JSON Lines or CSV append) as they flow, avoiding the need to buffer massive arrays in memory.
*   **Hybrid Memory-DB State Registry**: Real-time progress is tracked in a thread-safe, in-memory `Registry` that handles fast API polling `/progress` requests in microseconds. A throttled background routine syncs absolute counters to the PostgreSQL database every 500ms, removing DB write bottlenecks.

---

## 2. Concurrency Model

Our concurrency architecture employs Go's core primitives to build an efficient multi-stage pipeline:

```
Ingestion Stage (Sync WG) ➜ recordsCh ➜ Validation (Worker Pool) ➜ validatedCh ➜ Transformation (Worker Pool) ➜ transformedCh ➜ Aggregator (Fan-in) ➜ exportRecordsCh ➜ Export (Stream Exporter)
```

1.  **Ingestion (Fan-out)**: Spawns one dedicated goroutine per input source. They query APIs or open files in parallel and emit unified records to the `recordsCh`. A coordinator waitgroup waits for all ingestion routines to finish and safely closes the channel.
2.  **Worker Pools (Fan-out/Fan-in)**: The Validation and Transformation stages run pre-parameterized, configurable numbers of worker goroutines. To avoid parsing configurations inside the hot path, validation and transform rules are pre-compiled into a list of closure functions before workers start, ensuring high CPU performance.
3.  **Aggregation (Fan-in)**: A single worker goroutine consumes `transformedCh` and accumulates statistics in thread-safe, local memory maps (supporting global sums, averages, min, max, and group-by groupings). This avoids lock contention. Once completed, it forwards computed results to `resultCh`.
4.  **Graceful Cancellation**: A root `context.Context` is propagated through all goroutines. If a job is aborted via `PATCH /cancel` or times out, the context is cancelled, triggering a cascading exit across all workers and avoiding resource leaks.

---

## 3. Engineering Trade-offs

| Choice Made | Advantages | Disadvantages / Trade-offs |
| :--- | :--- | :--- |
| **GORM PostgreSQL DB** | Robust concurrency support, query efficiency, native ACID transactions, seamless container orchestration. | Requires running a database engine/container (unlike a self-contained embedded SQLite file). |
| **Linear Pipeline Channels** | Strong decoupling of stages, excellent streamability, clear stages separation. | High channel-allocation overhead; memory copying across channels incurs minor CPU cost. |
| **In-Memory Registry** | Incredibly low latency for API queries, zero database bottleneck on fast counters. | Active job states are lost if the server crashes mid-run; state is recovered only up to the last 500ms Postgres sync. |
| **Pre-compiled Closures** | Dynamic rules are loaded via JSON but executed with native speed (compiled regex/math). | Rule definitions are limited to simple predefined operators (min, max, trim, lowercase). |
