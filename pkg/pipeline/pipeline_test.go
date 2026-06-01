package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"data-processing-pipeline/pkg/db"
)

func TestFullPipelineIntegration(t *testing.T) {
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
	// Index 4 height=20 (invalid: < 30)
	// Index 5 weight=30 (invalid: < 40)
	err = os.WriteFile(mockCsvPath, []byte(csvContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write mock CSV: %v", err)
	}

	// 3. Open temporary SQLite database
	mockDbPath := filepath.Join(tempDir, "test_pipeline.db")
	database, err := db.NewDB(mockDbPath)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	defer database.Close()

	// 4. Formulate JobSpec with validation, transformation, aggregations, and streaming export targets
	exportJsonPath := filepath.Join(tempDir, "exported_records.json")
	exportCsvPath := filepath.Join(tempDir, "exported_records.csv")

	spec := &JobSpec{
		ID:   "integration-test-job-01",
		Name: "Test Concurrency Run",
		Sources: []SourceSpec{
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
		ValidationRules: []ValidationRuleSpec{
			{Field: "height", Rule: "min", Param: "30"}, // Raw idx 4 has height 20 -> fails
			{Field: "weight", Rule: "min", Param: "40"}, // Raw idx 5 has weight 30 -> fails
		},
		TransformRules: []TransformRuleSpec{
			{Field: "height", Rule: "cast", Param: "float"},
			{Field: "weight", Rule: "cast", Param: "float"},
			{Field: "height", Rule: "add_constant", Param: "2.0"}, // Normalizes by adding offset
			{Field: "processed_stamp", Rule: "enrich_time"},
		},
		AggregationSpecs: []AggregationSpec{
			{Field: "height", Func: "avg", Target: "average_height"},
			{Field: "weight", Func: "sum", Target: "total_weight"},
			{Field: "weight", Func: "count", Target: "record_count"},
		},
		ExportTargets: []ExportTargetSpec{
			{Type: "json", Path: exportJsonPath},
			{Type: "csv", Path: exportCsvPath},
		},
		Workers: WorkerConfig{
			Validation:     3,
			Transformation: 3,
		},
	}

	// Create pending job in the SQLite database first
	err = database.CreateJob(spec.ID, spec.Name, "")
	if err != nil {
		t.Fatalf("Failed to create pending test job record: %v", err)
	}

	// 5. Run the Concurrent Pipeline
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = RunPipeline(ctx, database, spec)
	if err != nil {
		t.Fatalf("RunPipeline returned critical error: %v", err)
	}

	// 6. Assertions on completed database states
	job, err := database.GetJob(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job from DB: %v", err)
	}

	if job.Status != "COMPLETED" {
		t.Errorf("Expected job status COMPLETED, got %s. Summary: %v", job.Status, job.ErrorSummary)
	}

	// Total input rows = 5
	if job.TotalRecords != 5 {
		t.Errorf("Expected TotalRecords 5, got %d", job.TotalRecords)
	}

	// Success processed = 3 (Index 1, 2, 3)
	if job.ProcessedRecords != 3 {
		t.Errorf("Expected ProcessedRecords 3, got %d", job.ProcessedRecords)
	}

	// Failed records = 2 (Index 4 fails height check, Index 5 fails weight check)
	if job.FailedRecords != 2 {
		t.Errorf("Expected FailedRecords 2, got %d", job.FailedRecords)
	}

	// 7. Assertions on audit log database errors
	errs, err := database.GetJobErrors(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job errors: %v", err)
	}

	if len(errs) != 2 {
		for _, e := range errs {
			t.Logf("AUDIT ERROR: stage=%s, source=%s, record=%s, msg=%s", e.Stage, e.SourceID, e.RecordID, e.ErrorMessage)
		}
		t.Errorf("Expected 2 job errors in SQLite audit, got %d", len(errs))
	}

	// Verify error messages
	for _, e := range errs {
		if e.Stage != "validation" {
			t.Errorf("Expected validation stage error, got %s", e.Stage)
		}
	}

	// 8. Assertions on generated export files
	// Verify JSON export file exists
	if _, err := os.Stat(exportJsonPath); os.IsNotExist(err) {
		t.Errorf("Expected export JSON file to exist at %s", exportJsonPath)
	}

	// Verify CSV export file exists
	if _, err := os.Stat(exportCsvPath); os.IsNotExist(err) {
		t.Errorf("Expected export CSV file to exist at %s", exportCsvPath)
	}



	// 9. Assertions on computed aggregations results
	results, err := database.GetJobResults(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job results from DB: %v", err)
	}

	var aggs map[string]any
	err = json.Unmarshal([]byte(results.ResultsJSON), &aggs)
	if err != nil {
		t.Fatalf("Failed to parse results JSON: %v", err)
	}

	// Valid rows:
	// Row 1: Height=60 + 2 = 62, Weight=110
	// Row 2: Height=70 + 2 = 72, Weight=150
	// Row 3: Height=65 + 2 = 67, Weight=130
	// Total processed: count = 3
	// Sum weight: 110 + 150 + 130 = 390.0
	// Avg height: (62 + 72 + 67) / 3 = 67.0

	if count, ok := aggs["record_count"].(float64); !ok || count != 3 {
		t.Errorf("Expected record_count 3, got %v", aggs["record_count"])
	}

	if sum, ok := aggs["total_weight"].(float64); !ok || sum != 390.0 {
		t.Errorf("Expected total_weight 390.0, got %v", aggs["total_weight"])
	}

	if avg, ok := aggs["average_height"].(float64); !ok || avg != 67.0 {
		t.Errorf("Expected average_height 67.0, got %v", aggs["average_height"])
	}
}

func TestPipelineCancellation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pipeline_cancel_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mockCsvPath := filepath.Join(tempDir, "biometrics_cancel.csv")
	// Generate massive dataset to ensure running is cancelled mid-stream
	var sb string
	sb += "Idx,Height,Weight\n"
	for i := 1; i <= 10000; i++ {
		sb += "1,65.0,120.0\n"
	}
	_ = os.WriteFile(mockCsvPath, []byte(sb), 0644)

	mockDbPath := filepath.Join(tempDir, "cancel.db")
	database, err := db.NewDB(mockDbPath)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer database.Close()

	spec := &JobSpec{
		ID:   "cancel-test-job-01",
		Name: "Cancelled Pipeline Run",
		Sources: []SourceSpec{
			{
				ID:   "cancel-source",
				Type: "csv",
				Path: mockCsvPath,
				Schema: map[string]string{
					"Height": "height",
				},
			},
		},
		Workers: WorkerConfig{
			Validation:     2,
			Transformation: 2,
		},
		ExportTargets: []ExportTargetSpec{
			{Type: "json", Path: filepath.Join(tempDir, "cancel_export.json")},
		},
	}

	// Create pending job in SQLite first
	err = database.CreateJob(spec.ID, spec.Name, "")
	if err != nil {
		t.Fatalf("Failed to create pending test job record: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start pipeline
	go func() {
		_ = RunPipeline(ctx, database, spec)
	}()

	// Wait 1 millisecond and cancel
	time.Sleep(1 * time.Millisecond)
	cancel()

	// Wait a bit for pipeline to gracefully tear down
	time.Sleep(100 * time.Millisecond)

	job, err := database.GetJob(spec.ID)
	if err != nil {
		t.Fatalf("Failed to fetch job details: %v", err)
	}

	if job.Status != "CANCELLED" {
		t.Errorf("Expected job status CANCELLED, got %s", job.Status)
	}
}
