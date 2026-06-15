/*
Pipeline handles requests for creating, listing, cancelling, deleting, and fetching pipeline jobs,
their current progress, processing errors, and aggregation results.
It parses and validates request payloads, orchestrates actions using the service layer,
queries the database via the repositories, and outputs JSON responses.
*/
package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"data-processing-pipeline/apps/server/service"
	"data-processing-pipeline/packages/shared/models"
	"data-processing-pipeline/packages/shared/repository"
	"data-processing-pipeline/packages/shared/utils"

	"github.com/google/uuid"
)

// Helper to validate job IDs (only 1-64 alphanumeric, hyphen, or underscore characters)
func isValidJobID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// Helper to validate paths against directory traversal and confine to allowed sandboxes
func isSafePath(pathStr string) bool {
	cleaned := filepath.Clean(pathStr)
	
	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return false
	}
	
	var absPath string
	if filepath.IsAbs(cleaned) {
		absPath = cleaned
	} else {
		absPath = filepath.Join(cwd, cleaned)
	}
	absPath = filepath.Clean(absPath)
	
	// Allowed sandboxes:
	// 1. cwd/data
	// 2. cwd/samples
	// 3. system temp directory (for tests)
	dataSandbox := filepath.Join(cwd, "data")
	samplesSandbox := filepath.Join(cwd, "samples")
	tempSandbox := os.TempDir()
	
	if strings.HasPrefix(absPath, dataSandbox) || strings.HasPrefix(absPath, samplesSandbox) || strings.HasPrefix(absPath, tempSandbox) {
		return true
	}
	return false
}

// Helper to parse and validate API version from path, Accept header, or X-API-Version header
func getAndValidateVersion(r *http.Request) (string, error) {
	version := r.PathValue("version")
	if version == "" {
		version = r.Header.Get("X-API-Version")
	}
	if version == "" {
		accept := r.Header.Get("Accept")
		if strings.Contains(accept, "vnd.pipeline.v1") {
			version = "v1"
		} else if strings.Contains(accept, "vnd.pipeline.v2") {
			version = "v2"
		}
	}
	if version == "" {
		version = "v1"
	}
	if version != "v1" {
		return "", fmt.Errorf("unsupported API version: %s", version)
	}
	return version, nil
}

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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	var spec models.JobSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeJSONError(w, http.StatusBadRequest, utils.MsgInvalidPayload+err.Error())
		return
	}

	// Validate Job ID if specified
	if spec.ID != "" && !isValidJobID(spec.ID) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job ID: must be 1-64 alphanumeric, hyphen, or underscore characters")
		return
	}

	// Validate Job Name
	if spec.Name != "" && (len(spec.Name) == 0 || len(spec.Name) > 255) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job name: must be 1-255 characters")
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

	// Validate paths and types of Ingest Sources
	for i, src := range spec.Sources {
		if src.ID == "" {
			spec.Sources[i].ID = fmt.Sprintf("src-%d", i+1)
		} else if !isValidJobID(src.ID) {
			writeJSONError(w, http.StatusBadRequest, "Invalid source ID: must be 1-64 alphanumeric, hyphen, or underscore characters")
			return
		}
		if src.Type == "" || src.Path == "" {
			writeJSONError(w, http.StatusBadRequest, utils.MsgSourceFieldsRequired)
			return
		}
		srcType := strings.ToLower(src.Type)
		if srcType != "csv" && srcType != "json" && srcType != "api" {
			writeJSONError(w, http.StatusBadRequest, "Unsupported source type: "+src.Type)
			return
		}
		if srcType == "api" {
			if !strings.HasPrefix(src.Path, "http://") && !strings.HasPrefix(src.Path, "https://") {
				writeJSONError(w, http.StatusBadRequest, "API source path must start with http:// or https://")
				return
			}
		} else {
			cleaned := filepath.Clean(src.Path)
			if !isSafePath(cleaned) {
				writeJSONError(w, http.StatusBadRequest, "Directory traversal path patterns are not allowed in source path")
				return
			}
			spec.Sources[i].Path = cleaned
		}
	}

	// Validate ValidationRules
	for _, rule := range spec.ValidationRules {
		if rule.Field == "" {
			writeJSONError(w, http.StatusBadRequest, "Validation rule 'field' cannot be empty")
			return
		}
		ruleType := strings.ToLower(rule.Rule)
		if ruleType != "required" && ruleType != "min" && ruleType != "max" && ruleType != "type" && ruleType != "regex" {
			writeJSONError(w, http.StatusBadRequest, "Unsupported validation rule: "+rule.Rule)
			return
		}
		if ruleType == "type" {
			typeParam := strings.ToLower(rule.Param)
			if typeParam != "int" && typeParam != "float" && typeParam != "bool" && typeParam != "string" {
				writeJSONError(w, http.StatusBadRequest, "Unsupported type validation parameter: "+rule.Param)
				return
			}
		}
		if ruleType == "regex" {
			if _, err := regexp.Compile(rule.Param); err != nil {
				writeJSONError(w, http.StatusBadRequest, "Invalid regex pattern: "+err.Error())
				return
			}
		}
	}

	// Validate TransformRules
	for _, rule := range spec.TransformRules {
		if rule.Field == "" {
			writeJSONError(w, http.StatusBadRequest, "Transform rule 'field' cannot be empty")
			return
		}
		ruleType := strings.ToLower(rule.Rule)
		if ruleType != "cast" && ruleType != "trim" && ruleType != "lower" && ruleType != "upper" && ruleType != "enrich_time" && ruleType != "add_constant" {
			writeJSONError(w, http.StatusBadRequest, "Unsupported transform rule: "+rule.Rule)
			return
		}
		if ruleType == "cast" {
			castParam := strings.ToLower(rule.Param)
			if castParam != "float" && castParam != "int" && castParam != "string" && castParam != "bool" {
				writeJSONError(w, http.StatusBadRequest, "Unsupported cast transform parameter: "+rule.Param)
				return
			}
		}
	}

	// Validate AggregationSpecs
	for _, agg := range spec.AggregationTypes {
		if agg.Field == "" || agg.Target == "" {
			writeJSONError(w, http.StatusBadRequest, "Aggregation field and target cannot be empty")
			return
		}
		fnType := strings.ToLower(agg.Func)
		if fnType != "count" && fnType != "sum" && fnType != "avg" && fnType != "min" && fnType != "max" {
			writeJSONError(w, http.StatusBadRequest, "Unsupported aggregation function: "+agg.Func)
			return
		}
	}

	// Validate ExportTargets
	for i, exp := range spec.ExportTargets {
		if exp.Type == "" || exp.Path == "" {
			writeJSONError(w, http.StatusBadRequest, "Export target type and path cannot be empty")
			return
		}
		expType := strings.ToLower(exp.Type)
		if expType != "json" && expType != "csv" {
			writeJSONError(w, http.StatusBadRequest, "Unsupported export target type: "+exp.Type)
			return
		}
		cleaned := filepath.Clean(exp.Path)
		if !isSafePath(cleaned) {
			writeJSONError(w, http.StatusBadRequest, "Directory traversal path patterns are not allowed in export path")
			return
		}
		spec.ExportTargets[i].Path = cleaned
	}

	// Validate WorkerPoolSizes
	if spec.WorkerPoolSizes.Validation > 100 || spec.WorkerPoolSizes.Transformation > 100 {
		writeJSONError(w, http.StatusBadRequest, "Worker pool sizes exceed maximum limit of 100")
		return
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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}
	if !isValidJobID(id) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job ID format")
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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}
	if !isValidJobID(id) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job ID format")
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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}
	if !isValidJobID(id) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job ID format")
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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}
	if !isValidJobID(id) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job ID format")
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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}
	if !isValidJobID(id) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job ID format")
		return
	}

	jp, ok := service.GlobalRegistry.Get(id)
	if !ok {
		// Fallback to DB if not running
		job, err := c.jobRepo.FindOne(id)
		if err == nil && (job.Status == "PENDING" || job.Status == "RUNNING") {
			cancelledSummary := utils.MsgCancelArchived
			_ = c.jobRepo.Complete(id, string(models.StatusCancelled), &cancelledSummary)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": utils.MsgCancelArchivedRes,
				"job_id":  id,
			})
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
	// Validate API version
	if _, err := getAndValidateVersion(r); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, utils.MsgMissingID)
		return
	}
	if !isValidJobID(id) {
		writeJSONError(w, http.StatusBadRequest, "Invalid job ID format")
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
