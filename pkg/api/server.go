package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"data-processing-pipeline/pkg/db"
	"data-processing-pipeline/pkg/pipeline"

	"github.com/google/uuid"
)

type Server struct {
	db       *db.DB
	addr     string
	listener *http.Server
}

func NewServer(addr string, database *db.DB) *Server {
	return &Server{
		db:   database,
		addr: addr,
	}
}

// Start boots up the HTTP server on the configured address.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// CORS and Content-Type Middleware
	handler := corsMiddleware(mux)

	// Static SPA Dashboard routes
	// Note: We serve static files from './web' directory
	fs := http.FileServer(http.Dir("./web"))
	mux.Handle("GET /", fs)

	// API REST Endpoints
	mux.HandleFunc("POST /api/v1/pipelines", s.handleCreatePipeline)
	mux.HandleFunc("GET /api/v1/pipelines", s.handleListPipelines)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", s.handleGetPipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/progress", s.handleGetProgress)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/results", s.handleGetResults)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/errors", s.handleGetErrors)
	mux.HandleFunc("PATCH /api/v1/pipelines/{id}/cancel", s.handleCancelPipeline)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", s.handleDeletePipeline)
	
	// Prometheus metrics endpoint
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	s.listener = &http.Server{
		Addr:    s.addr,
		Handler: handler,
	}

	fmt.Printf("[API] Server listening on http://%s\n", s.addr)
	return s.listener.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.listener != nil {
		return s.listener.Shutdown(ctx)
	}
	return nil
}

// ----------------------------------------------------
// Handlers
// ----------------------------------------------------

func (s *Server) handleCreatePipeline(w http.ResponseWriter, r *http.Request) {
	var spec pipeline.JobSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Validation checks on specifications
	if len(spec.Sources) == 0 {
		writeJSONError(w, http.StatusBadRequest, "At least one ingestion source is required")
		return
	}

	// Populate Job ID if empty
	if spec.ID == "" {
		spec.ID = uuid.New().String()[:8] // Short unique ID
	}
	if spec.Name == "" {
		spec.Name = "Pipeline-" + spec.ID
	}

	// Validate paths and types
	for i, src := range spec.Sources {
		if src.ID == "" {
			spec.Sources[i].ID = fmt.Sprintf("src-%d", i+1)
		}
		if src.Type == "" || src.Path == "" {
			writeJSONError(w, http.StatusBadRequest, "Source type and path are required")
			return
		}
	}

	// Set default workers if unset
	if spec.WorkerPoolSizes.Validation <= 0 {
		spec.WorkerPoolSizes.Validation = 3
	}
	if spec.WorkerPoolSizes.Transformation <= 0 {
		spec.WorkerPoolSizes.Transformation = 3
	}

	// Automatically establish export targets if none specified
	if len(spec.ExportTargets) == 0 {
		spec.ExportTargets = []pipeline.ExportTargetSpec{
			{
				Type: "json",
				Path: filepath.Join("data", "exports", spec.ID, "exported_records.json"),
			},
		}
	}

	specJSON, _ := json.Marshal(spec)

	// Save to DB in pending state
	if err := s.db.CreateJob(spec.ID, spec.Name, string(specJSON)); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to register job in database: "+err.Error())
		return
	}

	// Start pipeline concurrently in the background
	go func() {
		ctx := context.Background()
		err := pipeline.RunPipeline(ctx, s.db, &spec)
		if err != nil {
			fmt.Printf("[API] Runtime pipeline error on job %s: %v\n", spec.ID, err)
		}
	}()

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": "Pipeline job successfully created and queued",
		"job_id":  spec.ID,
		"name":    spec.Name,
		"status":  pipeline.StatusPending,
	})
}

func (s *Server) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	dbJobs, err := s.db.ListJobs()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to list jobs: "+err.Error())
		return
	}

	// Merge active in-memory counters for real-time progress lists
	type enrichedJob struct {
		db.PipelineJob
		ActiveProgress *pipeline.JobProgress `json:"active_progress,omitempty"`
	}

	enrichedList := make([]enrichedJob, len(dbJobs))
	for i, dj := range dbJobs {
		enrichedList[i] = enrichedJob{PipelineJob: dj}
		if jp, ok := pipeline.GlobalRegistry.Get(dj.ID); ok {
			extProgress := jp.ToExternal()
			enrichedList[i].ActiveProgress = &extProgress
		}
	}

	_ = json.NewEncoder(w).Encode(enrichedList)
}

func (s *Server) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing path parameter: id")
		return
	}

	job, err := s.db.GetJob(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(job)
}

func (s *Server) handleGetProgress(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing path parameter: id")
		return
	}

	// 1. Check in-memory active registry first (provides extreme real-time speed)
	if jp, ok := pipeline.GlobalRegistry.Get(id); ok {
		extProgress := jp.ToExternal()
		_ = json.NewEncoder(w).Encode(extProgress)
		return
	}

	// 2. Fallback to SQLite DB if finished/archived
	job, err := s.db.GetJob(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "Pipeline progress not found: "+err.Error())
		return
	}

	percent := 0.0
	if job.TotalRecords > 0 {
		percent = float64(job.ProcessedRecords+job.FailedRecords) * 100.0 / float64(job.TotalRecords)
	} else if job.Status == "COMPLETED" {
		percent = 100.0
	}

	pending := int64(job.TotalRecords - (job.ProcessedRecords + job.FailedRecords))
	if pending < 0 {
		pending = 0
	}

	_ = json.NewEncoder(w).Encode(pipeline.JobProgress{
		JobID:            job.ID,
		RecordsProcessed: int64(job.ProcessedRecords),
		RecordsPending:   pending,
		ErrorCount:       int64(job.FailedRecords),
		PercentComplete:  percent,
		ProcessingRate:   0.0,
		Name:             "Pipeline-" + job.ID,
		Status:           pipeline.JobStatus(job.Status),
		TotalRecords:     int64(job.TotalRecords),
		ProcessedRecords: int64(job.ProcessedRecords),
		FailedRecords:    int64(job.FailedRecords),
		StartTime:        job.CreatedAt,
		EndTime:          job.CompletedAt,
	})
}

func (s *Server) handleGetResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing path parameter: id")
		return
	}

	results, err := s.db.GetJobResults(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "Job results not generated or not completed yet: "+err.Error())
		return
	}

	var aggregatedResult pipeline.AggregatedResult
	_ = json.Unmarshal([]byte(results.ResultsJSON), &aggregatedResult)

	var paths []string
	_ = json.Unmarshal([]byte(results.ExportPaths), &paths)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"job_id":       results.JobID,
		"aggregates":   aggregatedResult,
		"export_files": paths,
		"updated_at":   results.UpdatedAt,
	})
}

func (s *Server) handleGetErrors(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing path parameter: id")
		return
	}

	errs, err := s.db.GetJobErrors(id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to retrieve errors: "+err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(errs)
}

func (s *Server) handleCancelPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing path parameter: id")
		return
	}

	jp, ok := pipeline.GlobalRegistry.Get(id)
	if !ok {
		// Fallback to SQLite DB if not running
		job, err := s.db.GetJob(id)
		if err == nil && (job.Status == "PENDING" || job.Status == "RUNNING") {
			cancelledSummary := "Cancelled before active run"
			_ = s.db.CompleteJob(id, string(pipeline.StatusCancelled), &cancelledSummary)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "Archived pending job marked cancelled in database"})
			return
		}
		writeJSONError(w, http.StatusBadRequest, "Pipeline is not actively running or registry expired")
		return
	}

	// Trigger cancellation via context func
	jp.Cancel()

	_ = json.NewEncoder(w).Encode(map[string]string{
		"message": "Cancellation request successfully dispatched to running goroutines",
		"job_id":  id,
	})
}

func (s *Server) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing path parameter: id")
		return
	}

	// Check if running
	if jp, ok := pipeline.GlobalRegistry.Get(id); ok {
		jp.Cancel()
	}

	// Delete from database
	if err := s.db.DeleteJob(id); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to delete job metadata: "+err.Error())
		return
	}

	pipeline.GlobalRegistry.Delete(id)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"message": "Job run and database metadata successfully deleted",
		"job_id":  id,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	jobs, err := s.db.ListJobs()
	if err != nil {
		http.Error(w, "Failed to fetch metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Count job statuses
	statusCount := map[string]int{
		"PENDING":   0,
		"RUNNING":   0,
		"COMPLETED": 0,
		"FAILED":    0,
		"CANCELLED": 0,
	}

	var processedTotal, failedTotal, inputTotal int64

	for _, j := range jobs {
		statusCount[j.Status]++
		processedTotal += int64(j.ProcessedRecords)
		failedTotal += int64(j.FailedRecords)
		inputTotal += int64(j.TotalRecords)
	}

	// Output Prometheus standard format
	var out strings.Builder
	out.WriteString("# HELP pipeline_jobs_total Total number of pipeline jobs.\n")
	out.WriteString("# TYPE pipeline_jobs_total counter\n")
	for status, count := range statusCount {
		out.WriteString(fmt.Sprintf("pipeline_jobs_total{status=\"%s\"} %d\n", status, count))
	}

	out.WriteString("\n# HELP pipeline_records_processed_total Total records successfully processed & exported.\n")
	out.WriteString("# TYPE pipeline_records_processed_total counter\n")
	out.WriteString(fmt.Sprintf("pipeline_records_processed_total %d\n", processedTotal))

	out.WriteString("\n# HELP pipeline_records_failed_total Total record validation/transformation failures.\n")
	out.WriteString("# TYPE pipeline_records_failed_total counter\n")
	out.WriteString(fmt.Sprintf("pipeline_records_failed_total %d\n", failedTotal))

	out.WriteString("\n# HELP pipeline_records_ingested_total Total raw records ingested from sources.\n")
	out.WriteString("# TYPE pipeline_records_ingested_total counter\n")
	out.WriteString(fmt.Sprintf("pipeline_records_ingested_total %d\n", inputTotal))

	_, _ = w.Write([]byte(out.String()))
}

// ----------------------------------------------------
// Helpers & Middleware
// ----------------------------------------------------

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if !strings.HasPrefix(r.URL.Path, "/metrics") && !strings.HasPrefix(r.URL.Path, "/") {
			w.Header().Set("Content-Type", "application/json")
		}

		next.ServeHTTP(w, r)
	})
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
