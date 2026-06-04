/*
Package controller implements the API layer controllers responsible for handling HTTP requests.
PipelineController handles requests for creating, listing, cancelling, deleting, and fetching pipeline jobs,
their current progress, processing errors, and aggregation results.
It parses and validates request payloads, orchestrates actions using the service layer,
queries the database via the repositories, and outputs JSON responses.
*/
package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"data-processing-pipeline/apps/server/service"
	"data-processing-pipeline/packages/shared/models"
	"data-processing-pipeline/packages/shared/repository"
	"data-processing-pipeline/packages/shared/utils"

	"github.com/google/uuid"
)

type PipelineController struct {
	pipelineService *service.PipelineService
	jobRepo         *repository.PipelineJobRepository
	errRepo         *repository.JobErrorRepository
	resultRepo      *repository.JobResultsRepository
}

func NewPipelineController(
	pipelineService *service.PipelineService,
	jobRepo *repository.PipelineJobRepository,
	errRepo *repository.JobErrorRepository,
	resultRepo *repository.JobResultsRepository,
) *PipelineController {
	return &PipelineController{
		pipelineService: pipelineService,
		jobRepo:         jobRepo,
		errRepo:         errRepo,
		resultRepo:      resultRepo,
	}
}

func (c *PipelineController) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var spec models.JobSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeJSONError(w, http.StatusBadRequest, utils.MsgInvalidPayload+err.Error())
		return
	}

	// Validation checks on specifications
	if len(spec.Sources) == 0 {
		writeJSONError(w, http.StatusBadRequest, utils.MsgSourceRequired)
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
			writeJSONError(w, http.StatusBadRequest, utils.MsgSourceFieldsRequired)
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
		spec.ExportTargets = []models.ExportTargetSpec{
			{
				Type: "json",
				Path: filepath.Join("data", "exports", spec.ID, "exported_records.json"),
			},
		}
	}

	specJSON, _ := json.Marshal(spec)

	// Save to DB in pending state
	job := &models.PipelineJob{
		ID:        spec.ID,
		Status:    string(models.StatusPending),
		Config:    string(specJSON),
		CreatedAt: time.Now(),
	}

	if err := c.jobRepo.Save(job); err != nil {
		writeJSONError(w, http.StatusInternalServerError, utils.MsgDBRegisterFailed+err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": utils.MsgCreateSuccess,
		"job_id":  spec.ID,
		"name":    spec.Name,
		"status":  models.StatusPending,
	})
}

func (c *PipelineController) ListPipelines(w http.ResponseWriter, r *http.Request) {
	dbJobs, err := c.jobRepo.FindAll()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, utils.MsgListJobsFailed+err.Error())
		return
	}

	// Merge active in-memory counters for real-time progress lists
	type enrichedJob struct {
		models.PipelineJob
		ActiveProgress *models.JobProgress `json:"active_progress,omitempty"`
	}

	enrichedList := make([]enrichedJob, len(dbJobs))
	for i, dj := range dbJobs {
		enrichedList[i] = enrichedJob{PipelineJob: dj}
		if jp, ok := service.GlobalRegistry.Get(dj.ID); ok {
			extProgress := jp.ToExternal()
			enrichedList[i].ActiveProgress = &extProgress
		}
	}

	_ = json.NewEncoder(w).Encode(enrichedList)
}

func (c *PipelineController) GetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}

	job, err := c.jobRepo.FindOne(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, utils.MsgJobNotFound+err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(job)
}

func (c *PipelineController) GetProgress(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}

	// 1. Check in-memory active registry first
	if jp, ok := service.GlobalRegistry.Get(id); ok {
		extProgress := jp.ToExternal()
		_ = json.NewEncoder(w).Encode(extProgress)
		return
	}

	// 2. Fallback to PostgreSQL DB if finished/archived
	job, err := c.jobRepo.FindOne(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, utils.MsgProgressNotFound+err.Error())
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

	_ = json.NewEncoder(w).Encode(models.JobProgress{
		JobID:            job.ID,
		RecordsProcessed: int64(job.ProcessedRecords),
		RecordsPending:   pending,
		ErrorCount:       int64(job.FailedRecords),
		PercentComplete:  percent,
		ProcessingRate:   0.0,
		Name:             "Pipeline-" + job.ID,
		Status:           job.Status,
		TotalRecords:     int64(job.TotalRecords),
		ProcessedRecords: int64(job.ProcessedRecords),
		FailedRecords:    int64(job.FailedRecords),
		StartTime:        job.CreatedAt,
		EndTime:          job.CompletedAt,
	})
}

func (c *PipelineController) GetResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}

	results, err := c.resultRepo.FindOne(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, utils.MsgResultsNotReady+err.Error())
		return
	}

	var aggregatedResult models.AggregatedResult
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

func (c *PipelineController) GetErrors(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}

	errs, err := c.errRepo.FindByJobID(id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, utils.MsgRetrieveErrorsFailed+err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(errs)
}

func (c *PipelineController) CancelPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}

	jp, ok := service.GlobalRegistry.Get(id)
	if !ok {
		// Fallback to DB if not running
		job, err := c.jobRepo.FindOne(id)
		if err == nil && (job.Status == "PENDING" || job.Status == "RUNNING") {
			cancelledSummary := utils.MsgCancelArchived
			_ = c.jobRepo.Complete(id, string(models.StatusCancelled), &cancelledSummary)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": utils.MsgCancelArchivedRes})
			return
		}
		writeJSONError(w, http.StatusBadRequest, utils.MsgNotRunning)
		return
	}

	// Trigger cancellation via context func
	jp.Cancel()

	_ = json.NewEncoder(w).Encode(map[string]string{
		"message": utils.MsgCancelDispatched,
		"job_id":  id,
	})
}

func (c *PipelineController) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}

	// Check if running
	if jp, ok := service.GlobalRegistry.Get(id); ok {
		jp.Cancel()
	}

	// Delete from database
	if err := c.jobRepo.Delete(id); err != nil {
		writeJSONError(w, http.StatusInternalServerError, utils.MsgDeleteFailed+err.Error())
		return
	}

	service.GlobalRegistry.Delete(id)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"message": utils.MsgDeleteSuccess,
		"job_id":  id,
	})
}

func (c *PipelineController) GetMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	jobs, err := c.jobRepo.FindAll()
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

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
