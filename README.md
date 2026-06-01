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

## 2. Getting Started

### Local Setup (Using Go)

#### Prerequisites
- Go 1.22 or higher (successfully compiled and tested on Go 1.26.0)

#### Installation
Clone or navigate to the workspace directory:
```bash
cd /home/lenovo/Documents/data-processing-pipeline
```

#### Running the Server
Start the REST API and SPA Dashboard server:
```bash
go run cmd/server/main.go
```
The server will boot up and:
1. Ensure the `data/` and `samples/` directories exist.
2. Auto-generate a local biometric CSV dataset (`samples/biometrics_sample.csv`) for testing.
3. Start the WAL-mode SQLite database.
4. Launch the HTTP server on [http://localhost:8080](http://localhost:8080).

### Containerized Setup (Using Docker)

#### Prerequisites
- Docker and Docker Compose installed

#### Running with Docker Compose (Recommended)
You can boot up the entire backend service, database, and embedded dashboard with a single command:
```bash
docker compose up -d --build
```
This command builds the lightweight Go binary inside a multi-stage builder and mounts local folders. 
- **Persistency**: The `./data` folder on your host machine is mapped to `/app/data` inside the container. This ensures that your SQLite database records and exported CSV/JSON files remain fully persistent on your host machine across builds or restarts.
- **Custom Datasets**: The `./samples` folder on your host machine is mapped to `/app/samples`, allowing you to add custom CSV/JSON files locally on your host and run pipelines against them.

To view logs:
```bash
docker compose logs -f
```

To stop the service:
```bash
docker compose down
```

#### Running with Plain Docker
Alternatively, build and run the image directly:
```bash
# Build the image
docker build -t pipeline-service .

# Run the container mapping ports and folders
docker run -d -p 8080:8080 -v $(pwd)/data:/app/data -v $(pwd)/samples:/app/samples --name pipeline-service pipeline-service
```

---

## 3. Visual Monitoring Dashboard

Once the server is running, open your web browser and navigate to:
👉 **[http://localhost:8080](http://localhost:8080)**

The visual dashboard features:
- **Presets Selector**: Load preset configurations (COVID-19 Latest CSV, Crypto markets JSON API, and Biometric height/weight CSV) automatically with one click.
- **Interactive Editor**: Review and adjust specifications dynamically.
- **Active Telemetry Grid**: Track live running states, success rates, processed counters, and rates in records/sec.
- **Inspection Audits Modal**:
  - **Latency Latches**: Shows a real-time bar chart of stage durations in milliseconds (utilizing Chart.js).
  - **Aggregations Viewer**: Renders computed sums, averages, and group-by outcomes.
  - **Failed Audits Logs**: Shows exact reasons why individual records failed validation or transformation.
  - **Abort & Wipe Controls**: Stop active jobs or delete historical jobs.

---

## 4. REST API Endpoints

All payload parameters are encoded in standard JSON formats.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/v1/pipelines` | Start a new pipeline job run |
| `GET` | `/api/v1/pipelines` | List all historical and active jobs |
| `GET` | `/api/v1/pipelines/:id` | Get job metadata and completion summary |
| `GET` | `/api/v1/pipelines/:id/progress` | Get real-time percent, counters, latencies, and rates |
| `GET` | `/api/v1/pipelines/:id/results` | Get finalized aggregations and output file links |
| `GET` | `/api/v1/pipelines/:id/errors` | Audit specific failures (stage, record, and cause) |
| `PATCH` | `/api/v1/pipelines/:id/cancel` | Stop a running pipeline gracefully mid-stream |
| `DELETE` | `/api/v1/pipelines/:id` | Delete job metadata and logs from registry |
| `GET` | `/metrics` | Prometheus-compatible diagnostics exporter |

### Example Dispatch Payload (POST `/api/v1/pipelines`)
Submit a job configuration via `curl`:
```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "id": "covid-run-01",
    "name": "COVID Global Ingestion",
    "sources": [
      {
        "id": "covid-latest",
        "type": "csv",
        "path": "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv",
        "schema": {
          "location": "country",
          "new_cases": "cases",
          "new_deaths": "deaths"
        }
      }
    ],
    "validation_rules": [
      { "field": "country", "rule": "required" },
      { "field": "cases", "rule": "min", "param": "0" }
    ],
    "transform_rules": [
      { "field": "country", "rule": "upper" },
      { "field": "cases", "rule": "cast", "param": "float" },
      { "field": "deaths", "rule": "cast", "param": "float" },
      { "field": "processed_at", "rule": "enrich_time" }
    ],
    "aggregation_specs": [
      { "field": "cases", "func": "sum", "target": "total_cases" },
      { "field": "cases", "func": "max", "target": "max_single_country_cases" },
      { "field": "cases", "func": "sum", "group_by": "country", "target": "cases_by_country" }
    ],
    "export_targets": [
      { "type": "json", "path": "data/exports/covid_run_01.json" },
      { "type": "csv", "path": "data/exports/covid_run_01.csv" }
    ],
    "workers": {
      "validation": 8,
      "transformation": 8
    }
  }'
```

---

## 5. Running the Test Suite

Our tests include comprehensive validation checks, transformations, aggregations, parallel worker counts, end-to-end integration runs, and graceful cancellations under the Go race detector.

To run the complete test suite:
```bash
go test -v ./... -race
```
All tests are 100% race-free and pass cleanly.
