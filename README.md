# Robust Concurrent Data Processing Pipeline

This repository hosts a robust, high-performance, and fully concurrent data processing pipeline implemented in Go. It ingests streaming datasets from local files or external HTTP APIs in parallel, validates records against schemas, normalizes and enriches payloads through pre-compiled transform rules, computes mathematical aggregations (including group-by operations), and persists results to both streaming disk files (CSV/JSON) and an embedded SQLite database—all managed via a REST API and a visual real-time monitoring dashboard.

---

## 1. Pipeline Architecture

The system utilizes standard Go channels and worker pools to achieve high throughput and thread-safe streaming coordination:

```
                  ┌──────────────────────┐
                  │ Ingest Source Gorout │ (CSV/JSON/REST API)
                  └──────────┬───────────┘
                             │ (recordsCh)
                             ▼
                  ┌──────────────────────┐
                  │  Validation Workers  │ (Fan-out worker pool)
                  └──────────┬───────────┘
                             │ (validatedCh)
                             ▼
                  ┌──────────────────────┐
                  │  Transform Workers   │ (Fan-out worker pool)
                  └──────────┬───────────┘
                             │ (transformedCh)
                             ▼
                  ┌──────────────────────┐
                  │  Aggregation Engine  │ (Single worker Fan-in)
                  └──────────┬───────────┘
                             │ (exportRecordsCh)
                             ▼
                  ┌──────────────────────┐
                  │    Export Stream     │ (JSON Lines, CSV, & SQLite writes)
                  └──────────────────────┘
```

### Key Technical Characteristics
- **Linear Coupling**: Stages are linked via buffered channels, providing non-blocking operation and moderate memory control.
- **Worker Pools (Fan-out/Fan-in)**: Parameterized workers handle parallel validation and transformation.
- **Precompiled Execution Rules**: Input rules are compiled into closure functions upon startup, preventing repetitive config parsing inside the hot processing path.
- **Zero-CGO SQLite Persistence**: Persists run telemetry using a pure Go SQLite database operating in **Write-Ahead Logging (WAL)** mode for high concurrency.
- **Memory-Efficient Stream Exports**: Out-Of-Memory (OOM) situations are entirely bypassed by streaming processed outputs directly to files rather than loading arrays in memory.
- **Event-Driven Telemetry**: Stages write to `progressCh` and `errorCh` in a race-free cascading shutdown model, tracking latencies dynamically.

---

## 2. Directory Layout

The application is organized as a Go Monorepo containing isolated microservices and shared utilities:

```
├── apps/
│   ├── server/           # API Gateway HTTP REST Service
│   │   ├── controller/   # API handlers & Swagger validators (pipeline.go)
│   │   ├── service/      # Business logic execution tracker (pipeline.go)
│   │   ├── routes/       # Endpoint routing & static file serving (pipeline.go)
│   │   └── main.go       # Server gateway entry point
│   ├── worker/           # Background Queue Poll Loop Worker Service
│   │   └── main.go       # Polling runner executing sequential queued jobs
│   └── web/              # Frontend visual SPA Dashboard (app.js, index.html, style.css)
├── packages/
│   └── shared/           # Core shared libraries
│       ├── config/       # GORM PostgreSQL connection pool initializer
│       ├── models/       # Shared database entities (PipelineJob, JobError, JobResults) & specifications
│       ├── repository/   # GORM database data access objects
│       ├── pipeline/     # Concurrency pipeline execution stages (Ingest, Validate, Transform, Aggregate, Export)
│       └── utils/        # Common parsing functions (common.go) and API messages (constant.go)
└── test/                 # Consolidated independent unit and integration tests
```

---

## 3. Getting Started

### Local Setup (Using Go)

#### Prerequisites
- Go 1.22 or higher (successfully compiled and tested on Go 1.26.0)
- PostgreSQL database running locally

#### Running the API Gateway Server
Start the HTTP REST API and SPA Dashboard server:
```bash
go run apps/server/main.go
```
The server will:
1. Ensure the `data/` and `samples/` folders exist.
2. Auto-generate a biometric CSV dataset (`samples/biometrics_sample.csv`) for quick ingestion testing.
3. Establish connection to the database.
4. Listen on port `8080` serving the dynamic dashboard.

#### Running the Background Worker
In a separate terminal, launch the worker polling queue:
```bash
go run apps/worker/main.go
```
The worker will:
1. Connect to GORM PostgreSQL database.
2. Continually scan for jobs marked with a `PENDING` status.
3. Retrieve specifications, update status to `RUNNING`, and orchestrate parallel stage executions.

---

### Containerized Setup (Using Docker Compose)

You can launch the database, API server gateway, background worker, and frontend dashboard with a single command:
```bash
docker compose up -d --build
```

#### Port Mappings & Volumes
*   **API Gateway Port**: Exposed on host port `8080`. Access the dashboard at **[http://localhost:8080](http://localhost:8080)**.
*   **PostgreSQL Port (`5433:5432`)**: The database container port `5432` is mapped to host port **`5433`**. This allows you to run test suites locally on your host machine while interacting directly with the active Docker database!
*   **Persistency**: Host folder `./data` is mapped to `/app/data` to persist run outputs.
*   **Samples Mount**: Host folder `./samples` is mapped to `/app/samples`, allowing you to drop custom CSV/JSON files locally and ingest them via API specs.

To shut down:
```bash
docker compose down --remove-orphans
```

---

## 4. Visual Monitoring Dashboard

Navigate to **[http://localhost:8080](http://localhost:8080)** to access the visual SPA. It includes:
*   **Preset Templates**: Instant launch triggers for COVID-19 CSV records, Crypto market JSON APIs, and biometrics height/weights datasets.
*   **JSON Editor**: Raw JSON validation and dispatch control.
*   **Active Telemetry Grid**: Visual metrics cards showing processing speeds (recs/s), failure rates, and duration.
*   **Inspection Audits**:
    *   *Latency Latches*: A Chart.js horizontal bar graph showing latency breakdowns across all processing stages.
    *   *Aggregations*: Final computed sums, averages, and group-by category objects.
    *   *Failed Audits*: Raw payload strings and error reasons for validation failures.
    *   *Wipe/Abort*: Controls to terminate running jobs or erase registry history.

---

## 5. REST API Endpoints

All payloads are parsed in JSON formats. Error responses return a unified `ErrorResponse` schema (`{"error": "message"}`).

| Method | Endpoint | Success Code | Error Codes | Description |
| :--- | :--- | :---: | :---: | :--- |
| `POST` | `/api/v1/pipelines` | `201 Created` | `400`, `500` | Start a new pipeline job |
| `GET` | `/api/v1/pipelines` | `200 OK` | `500` | List all historical and active jobs |
| `GET` | `/api/v1/pipelines/{id}` | `200 OK` | `400`, `404` | Get GORM database metadata block |
| `GET` | `/api/v1/pipelines/{id}/progress` | `200 OK` | `400`, `404` | Get real-time status and telemetry |
| `GET` | `/api/v1/pipelines/{id}/results` | `200 OK` | `400`, `404` | Get finalized aggregations and output file links |
| `GET` | `/api/v1/pipelines/{id}/errors` | `200 OK` | `400`, `500` | Audit specific validation/transform failures |
| `PATCH` | `/api/v1/pipelines/{id}/cancel` | `200 OK` | `400` | Abort a running pipeline gracefully mid-stream |
| `DELETE` | `/api/v1/pipelines/{id}` | `200 OK` | `400`, `500` | Delete job metadata and error logs from registry |
| `GET` | `/metrics` | `200 OK` | — | Prometheus diagnostics exporter |

---

## 6. Running the Test Suite

Our tests include validation checks, transformation casting, calculations, integration runs, and graceful cancellation flows.

To run the complete test suite with the race detector enabled:
```bash
go test -v ./... -race
```
*(All tests execute against the Docker PostgreSQL database on port `5433` and pass race-free.)*
