package pipeline_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"data-processing-pipeline/pkg/config"
	"data-processing-pipeline/pkg/models"
	"data-processing-pipeline/pkg/repository"
	"data-processing-pipeline/pkg/service"

	"github.com/joho/godotenv"
)

func TestFullPipelineIntegration(t *testing.T) {
	// Load env file if available
	_ = godotenv.Load("../../.env")

	// Connect to GORM PostgreSQL Database
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skipf("Skipping integration test: PostgreSQL database is not reachable. Err: %v", err)
	}

	// 1. Create temporary directory for isolated tests
	tempDir, err := os.MkdirTemp("", "pipeline_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 2. Generate temporary mock biometrics CSV source
	mockCsvPath := filepath.Join(tempDir, "biometrics_input.csv")
	csvContent := `Idx,Height,Weight
1,60.0,110.0
2,70.0,150.0
3,65.0,130.0
4,20.0,120.0
5,68.0,30.0
`
	err = os.WriteFile(mockCsvPath, []byte(csvContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write mock CSV: %v", err)
	}

	// 3. Initialize repository, service layer
	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)

	pipelineService := service.NewPipelineService(jobRepo, errRepo, resultRepo)

	// 4. Formulate JobSpec with validation, transformation, aggregations, and streaming export targets
	exportJsonPath := filepath.Join(tempDir, "exported_records.json")
	exportCsvPath := filepath.Join(tempDir, "exported_records.csv")

	spec := &models.JobSpec{
		ID:   "integration-test-job-01",
		Name: "Test Concurrency Run",
		Sources: []models.SourceSpec{
			{
				ID:   "biometrics-csv-source",
				Type: "csv",
				Path: mockCsvPath,
				Schema: map[string]string{
					"Height": "height",
					"Weight": "weight",
				},
			},
		},
		ValidationRules: []models.ValidationRuleSpec{
			{Field: "height", Rule: "min", Param: "30"}, // Raw idx 4 has height 20 -> fails
			{Field: "weight", Rule: "min", Param: "40"}, // Raw idx 5 has weight 30 -> fails
		},
		TransformRules: []models.TransformRuleSpec{
			{Field: "height", Rule: "cast", Param: "float"},
			{Field: "weight", Rule: "cast", Param: "float"},
			{Field: "height", Rule: "add_constant", Param: "2.0"}, // Normalizes by adding offset
			{Field: "processed_stamp", Rule: "enrich_time"},
		},
		AggregationTypes: []models.AggregationSpec{
			{Field: "height", Func: "avg", Target: "average_height"},
			{Field: "weight", Func: "sum", Target: "total_weight"},
			{Field: "weight", Func: "count", Target: "record_count"},
		},
		ExportTargets: []models.ExportTargetSpec{
			{Type: "json", Path: exportJsonPath},
			{Type: "csv", Path: exportCsvPath},
		},
		WorkerPoolSizes: models.WorkerConfig{
			Validation:     3,
			Transformation: 3,
		},
	}

	// Clean database state for this specific test ID to avoid primary key conflicts
	_ = database.Delete(&models.PipelineJob{}, "id = ?", spec.ID)
	_ = database.Delete(&models.JobError{}, "job_id = ?", spec.ID)
	_ = database.Delete(&models.JobResults{}, "job_id = ?", spec.ID)

	// Create pending job in the PostgreSQL database first
	specJSON, _ := json.Marshal(spec)
	job := &models.PipelineJob{
		ID:        spec.ID,
		Status:    string(models.StatusPending),
		Config:    string(specJSON),
		CreatedAt: time.Now(),
	}
	err = jobRepo.Save(job)
	if err != nil {
		t.Fatalf("Failed to create pending test job record: %v", err)
	}

	// 5. Run the Concurrent Pipeline
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = pipelineService.RunPipeline(ctx, spec)
	if err != nil {
		t.Fatalf("RunPipeline returned critical error: %v", err)
	}

	// 6. Assertions on completed database states
	dbJob, err := jobRepo.FindOne(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job from DB: %v", err)
	}

	if dbJob.Status != "COMPLETED" {
		t.Errorf("Expected job status COMPLETED, got %s. Summary: %v", dbJob.Status, dbJob.ErrorSummary)
	}

	// Total input rows = 5
	if dbJob.TotalRecords != 5 {
		t.Errorf("Expected TotalRecords 5, got %d", dbJob.TotalRecords)
	}

	// Success processed = 3 (Index 1, 2, 3)
	if dbJob.ProcessedRecords != 3 {
		t.Errorf("Expected ProcessedRecords 3, got %d", dbJob.ProcessedRecords)
	}

	// Failed records = 2 (Index 4 fails height check, Index 5 fails weight check)
	if dbJob.FailedRecords != 2 {
		t.Errorf("Expected FailedRecords 2, got %d", dbJob.FailedRecords)
	}

	// 7. Assertions on audit log database errors
	errs, err := errRepo.FindByJobID(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job errors: %v", err)
	}

	if len(errs) != 2 {
		for _, e := range errs {
			t.Logf("AUDIT ERROR: stage=%s, raw_data=%s, msg=%s", e.Stage, e.RawData, e.ErrorMessage)
		}
		t.Errorf("Expected 2 job errors in database audit, got %d", len(errs))
	}

	// Verify error messages
	for _, e := range errs {
		if e.Stage != "validation" {
			t.Errorf("Expected validation stage error, got %s", e.Stage)
		}
	}

	// 8. Assertions on generated export files
	if _, err := os.Stat(exportJsonPath); os.IsNotExist(err) {
		t.Errorf("Expected export JSON file to exist at %s", exportJsonPath)
	}

	if _, err := os.Stat(exportCsvPath); os.IsNotExist(err) {
		t.Errorf("Expected export CSV file to exist at %s", exportCsvPath)
	}

	// 9. Assertions on computed aggregations results
	results, err := resultRepo.FindOne(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job results from DB: %v", err)
	}

	var aggs models.AggregatedResult
	err = json.Unmarshal([]byte(results.ResultsJSON), &aggs)
	if err != nil {
		t.Fatalf("Failed to parse results JSON: %v", err)
	}

	if count, ok := aggs.Sums["record_count"]; !ok || count != 3 {
		t.Errorf("Expected record_count 3, got %v", aggs.Sums["record_count"])
	}

	if sum, ok := aggs.Sums["total_weight"]; !ok || sum != 390.0 {
		t.Errorf("Expected total_weight 390.0, got %v", aggs.Sums["total_weight"])
	}

	if avg, ok := aggs.Averages["average_height"]; !ok || avg != 67.0 {
		t.Errorf("Expected average_height 67.0, got %v", aggs.Averages["average_height"])
	}
}

func TestPipelineCancellation(t *testing.T) {
	_ = godotenv.Load("../../.env")
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skipf("Skipping cancellation test: PostgreSQL database is not reachable. Err: %v", err)
	}

	tempDir, err := os.MkdirTemp("", "pipeline_cancel_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mockCsvPath := filepath.Join(tempDir, "biometrics_cancel.csv")
	var sb string
	sb += "Idx,Height,Weight\n"
	for i := 1; i <= 10000; i++ {
		sb += "1,65.0,120.0\n"
	}
	_ = os.WriteFile(mockCsvPath, []byte(sb), 0644)

	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)

	pipelineService := service.NewPipelineService(jobRepo, errRepo, resultRepo)

	spec := &models.JobSpec{
		ID:   "cancel-test-job-01",
		Name: "Cancelled Pipeline Run",
		Sources: []models.SourceSpec{
			{
				ID:   "cancel-source",
				Type: "csv",
				Path: mockCsvPath,
				Schema: map[string]string{
					"Height": "height",
				},
			},
		},
		WorkerPoolSizes: models.WorkerConfig{
			Validation:     2,
			Transformation: 2,
		},
		ExportTargets: []models.ExportTargetSpec{
			{Type: "json", Path: filepath.Join(tempDir, "cancel_export.json")},
		},
	}

	_ = database.Delete(&models.PipelineJob{}, "id = ?", spec.ID)

	specJSON, _ := json.Marshal(spec)
	job := &models.PipelineJob{
		ID:        spec.ID,
		Status:    string(models.StatusPending),
		Config:    string(specJSON),
		CreatedAt: time.Now(),
	}
	err = jobRepo.Save(job)
	if err != nil {
		t.Fatalf("Failed to create pending test job record: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = pipelineService.RunPipeline(ctx, spec)
	}()

	time.Sleep(5 * time.Millisecond)
	cancel()

	time.Sleep(100 * time.Millisecond)

	dbJob, err := jobRepo.FindOne(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job details: %v", err)
	}

	if dbJob.Status != "CANCELLED" {
		t.Errorf("Expected job status CANCELLED, got %s", dbJob.Status)
	}
}
