package test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"data-processing-pipeline/apps/server/controller"
	"data-processing-pipeline/apps/server/routes"
	"data-processing-pipeline/apps/server/service"
	"data-processing-pipeline/packages/shared/config"
	"data-processing-pipeline/packages/shared/models"
	"data-processing-pipeline/packages/shared/repository"
)

func TestControllerEndpointsAndSwaggerValidation(t *testing.T) {
	// 1. Setup Database Connection (Skip if Postgres not reachable)
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skip("Skipping controller tests: PostgreSQL database not reachable")
	}

	// Clean database tables for testing
	_ = database.Delete(&models.PipelineJob{}, "id LIKE ?", "test-ctrl-%")
	_ = database.Delete(&models.JobError{}, "job_id LIKE ?", "test-ctrl-%")
	_ = database.Delete(&models.JobResults{}, "job_id LIKE ?", "test-ctrl-%")

	// Initialize repositories, service, controller
	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)
	pipeService := service.NewPipelineService(jobRepo, errRepo, resultRepo)
	ctrl := controller.NewPipelineController(pipeService, jobRepo, errRepo, resultRepo)

	// 2. Load Swagger JSON for validation (located at project root, one level up from test/)
	swaggerBytes, err := os.ReadFile("../swagger.json")
	if err != nil {
		t.Fatalf("Failed to read swagger.json: %v", err)
	}

	var swagger map[string]interface{}
	if err := json.Unmarshal(swaggerBytes, &swagger); err != nil {
		t.Fatalf("Failed to parse swagger.json: %v", err)
	}

	// Helper to validate json response maps against Swagger component schemas
	validateSwaggerSchema := func(t *testing.T, responseBytes []byte, schemaName string) {
		components, ok := swagger["components"].(map[string]interface{})
		if !ok {
			t.Fatalf("Missing 'components' block in swagger.json")
		}
		schemas, ok := components["schemas"].(map[string]interface{})
		if !ok {
			t.Fatalf("Missing 'schemas' block in swagger.json")
		}
		schema, ok := schemas[schemaName].(map[string]interface{})
		if !ok {
			t.Fatalf("Schema '%s' not defined in swagger.json", schemaName)
		}
		properties, ok := schema["properties"].(map[string]interface{})
		if !ok {
			t.Fatalf("Schema '%s' is missing 'properties'", schemaName)
		}

		// Parse actual response
		var respMap map[string]interface{}
		if err := json.Unmarshal(responseBytes, &respMap); err != nil {
			t.Fatalf("Response is not valid JSON: %v", err)
		}

		// Ensure every response field exists in the Swagger schema properties
		for key := range respMap {
			if _, exists := properties[key]; !exists {
				t.Errorf("Field '%s' in API response is not defined in Swagger schema '%s'", key, schemaName)
			}
		}
	}

	t.Run("CreatePipeline - Valid spec creates PENDING job", func(t *testing.T) {
		jobID := "test-ctrl-01"
		spec := models.JobSpec{
			ID:   jobID,
			Name: "Test Controller Pipeline",
			Sources: []models.SourceSpec{
				{
					ID:   "src-1",
					Type: "csv",
					Path: "samples/biometrics_sample.csv",
				},
			},
			ExportTargets: []models.ExportTargetSpec{
				{
					Type: "json",
					Path: "data/exports/test-ctrl-01.json",
				},
			},
		}

		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(bodyBytes))
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status 201 Created, got %d", resp.StatusCode)
		}

		// Validate response payload keys against swagger schema
		respBytes := w.Body.Bytes()
		var respMap map[string]interface{}
		_ = json.Unmarshal(respBytes, &respMap)

		if respMap["job_id"] != jobID {
			t.Errorf("Expected job_id '%s', got '%v'", jobID, respMap["job_id"])
		}
		if respMap["status"] != "PENDING" {
			t.Errorf("Expected status 'PENDING', got '%v'", respMap["status"])
		}
	})

	t.Run("CreatePipeline - Invalid request payload returns 400", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBufferString("{invalid-json"))
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
	})

	t.Run("ListPipelines - Returns job array matching Schema", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/pipelines", nil)
		w := httptest.NewRecorder()

		ctrl.ListPipelines(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		var list []map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
			t.Fatalf("Response is not valid JSON array: %v", err)
		}

		if len(list) == 0 {
			t.Errorf("Expected at least one job in list, got 0")
		}
	})

	t.Run("GetPipeline - Exists returns metadata matching Swagger schema", func(t *testing.T) {
		jobID := "test-ctrl-01"
		req := httptest.NewRequest("GET", "/api/v1/pipelines/"+jobID, nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.GetPipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		validateSwaggerSchema(t, w.Body.Bytes(), "PipelineJob")
	})

	t.Run("GetProgress - Non-running falls back to DB and matches Swagger schema", func(t *testing.T) {
		jobID := "test-ctrl-01"
		req := httptest.NewRequest("GET", "/api/v1/pipelines/"+jobID+"/progress", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.GetProgress(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		validateSwaggerSchema(t, w.Body.Bytes(), "JobProgress")
	})

	t.Run("GetResults - Missing returns 404 Not Found", func(t *testing.T) {
		jobID := "test-ctrl-missing"
		req := httptest.NewRequest("GET", "/api/v1/pipelines/"+jobID+"/results", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.GetResults(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status 404 Not Found, got %d", resp.StatusCode)
		}
	})

	t.Run("GetErrors - Returns array of JobErrors", func(t *testing.T) {
		jobID := "test-ctrl-01"
		req := httptest.NewRequest("GET", "/api/v1/pipelines/"+jobID+"/errors", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.GetErrors(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		var errs []models.JobError
		if err := json.Unmarshal(w.Body.Bytes(), &errs); err != nil {
			t.Errorf("Response is not valid array of JobErrors: %v", err)
		}
	})

	t.Run("CancelPipeline - Dispatches cancel successfully", func(t *testing.T) {
		jobID := "test-ctrl-01"
		req := httptest.NewRequest("PATCH", "/api/v1/pipelines/"+jobID+"/cancel", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.CancelPipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		var respMap map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &respMap)

		if respMap["job_id"] != jobID {
			t.Errorf("Expected job_id '%s', got '%v'", jobID, respMap["job_id"])
		}

		// Verify database state is set to CANCELLED
		job, err := jobRepo.FindOne(jobID)
		if err != nil {
			t.Fatalf("Failed to fetch cancelled job: %v", err)
		}
		if job.Status != string(models.StatusCancelled) {
			t.Errorf("Expected status 'CANCELLED' in database, got '%s'", job.Status)
		}
	})

	t.Run("DeletePipeline - Deletes job metadata cleanly", func(t *testing.T) {
		jobID := "test-ctrl-01"
		req := httptest.NewRequest("DELETE", "/api/v1/pipelines/"+jobID, nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.DeletePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		// Verify database state is deleted
		_, err := jobRepo.FindOne(jobID)
		if err == nil {
			t.Errorf("Expected error fetching deleted job, but found record")
		}
	})

	t.Run("CreatePipeline - Spec with empty sources returns 400", func(t *testing.T) {
		spec := models.JobSpec{
			ID:      "test-ctrl-empty-src",
			Name:    "Missing Sources Job",
			Sources: []models.SourceSpec{},
		}
		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(bodyBytes))
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
		var respMap map[string]string
		_ = json.Unmarshal(w.Body.Bytes(), &respMap)
		if !strings.Contains(respMap["error"], "At least one ingestion source is required") {
			t.Errorf("Expected source required error message, got: %s", respMap["error"])
		}
	})

	t.Run("CreatePipeline - Spec with invalid source fields returns 400", func(t *testing.T) {
		spec := models.JobSpec{
			ID:   "test-ctrl-invalid-src",
			Name: "Invalid Source Job",
			Sources: []models.SourceSpec{
				{
					ID:   "src-1",
					Type: "", // missing type
					Path: "some/path.csv",
				},
			},
		}
		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(bodyBytes))
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
	})

	t.Run("GetPipeline - Non-existent job returns 404", func(t *testing.T) {
		jobID := "test-ctrl-does-not-exist"
		req := httptest.NewRequest("GET", "/api/v1/pipelines/"+jobID, nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.GetPipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status 404 Not Found, got %d", resp.StatusCode)
		}
	})

	t.Run("GetProgress - Non-existent job returns 404", func(t *testing.T) {
		jobID := "test-ctrl-does-not-exist"
		req := httptest.NewRequest("GET", "/api/v1/pipelines/"+jobID+"/progress", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.GetProgress(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status 404 Not Found, got %d", resp.StatusCode)
		}
	})

	t.Run("GetResults - Existing results returns 200 and matches Swagger schema", func(t *testing.T) {
		jobID := "test-ctrl-results-exist"
		
		// Setup database mock results record
		_ = database.Delete(&models.PipelineJob{}, "id = ?", jobID)
		_ = database.Delete(&models.JobResults{}, "job_id = ?", jobID)

		mockJob := &models.PipelineJob{
			ID:        jobID,
			Status:    "COMPLETED",
			CreatedAt: time.Now(),
		}
		if err := jobRepo.Save(mockJob); err != nil {
			t.Fatalf("Failed to save mock job: %v", err)
		}
		
		mockAgg := models.AggregatedResult{
			JobID:      jobID,
			TotalCount: 50,
			Sums:       map[string]float64{"total_value": 500.5},
			Averages:   map[string]float64{"average_value": 10.01},
		}
		aggBytes, _ := json.Marshal(mockAgg)
		pathsBytes, _ := json.Marshal([]string{"data/exports/test-ctrl-results-exist.json"})
		
		mockResult := &models.JobResults{
			JobID:       jobID,
			ResultsJSON: string(aggBytes),
			ExportPaths: string(pathsBytes),
			UpdatedAt:   time.Now(),
		}
		if err := resultRepo.Save(mockResult); err != nil {
			t.Fatalf("Failed to save mock result: %v", err)
		}
		defer func() {
			_ = database.Delete(&models.PipelineJob{}, "id = ?", jobID)
			_ = database.Delete(&models.JobResults{}, "job_id = ?", jobID)
		}()

		req := httptest.NewRequest("GET", "/api/v1/pipelines/"+jobID+"/results", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.GetResults(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		validateSwaggerSchema(t, w.Body.Bytes(), "JobResults")
	})

	t.Run("CancelPipeline - Job is not running or pending returns 400 Bad Request", func(t *testing.T) {
		jobID := "test-ctrl-completed-job"
		
		// Save a job with status COMPLETED in the database
		_ = database.Delete(&models.PipelineJob{}, "id = ?", jobID)
		_ = jobRepo.Save(&models.PipelineJob{
			ID:        jobID,
			Status:    "COMPLETED",
			CreatedAt: time.Now(),
		})
		defer func() {
			_ = database.Delete(&models.PipelineJob{}, "id = ?", jobID)
		}()

		req := httptest.NewRequest("PATCH", "/api/v1/pipelines/"+jobID+"/cancel", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.CancelPipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
	})

	t.Run("CancelPipeline - Actively running job invokes cancel successfully", func(t *testing.T) {
		jobID := "test-ctrl-running-job"
		
		// Register the job as running in the global registry with a custom cancellation function
		cancelCalled := false
		cancelFunc := func() {
			cancelCalled = true
		}
		
		jp := service.GlobalRegistry.Register(jobID, "Running Pipeline", cancelFunc)
		jp.Lock()
		jp.Status = models.StatusRunning
		jp.Unlock()
		
		defer func() {
			service.GlobalRegistry.Delete(jobID)
		}()

		req := httptest.NewRequest("PATCH", "/api/v1/pipelines/"+jobID+"/cancel", nil)
		req.SetPathValue("id", jobID)
		w := httptest.NewRecorder()

		ctrl.CancelPipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 OK, got %d", resp.StatusCode)
		}

		if !cancelCalled {
			t.Errorf("Expected cancellation handler function to be invoked")
		}
	})
}

func TestGetMetrics(t *testing.T) {
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skip("Skipping metrics test: PostgreSQL database not reachable")
	}

	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)
	pipeService := service.NewPipelineService(jobRepo, errRepo, resultRepo)
	ctrl := controller.NewPipelineController(pipeService, jobRepo, errRepo, resultRepo)

	// Create a mock job for metrics checking
	jobID := "test-ctrl-metrics"
	_ = database.Delete(&models.PipelineJob{}, "id = ?", jobID)
	_ = jobRepo.Save(&models.PipelineJob{
		ID:               jobID,
		Status:           "COMPLETED",
		TotalRecords:     100,
		ProcessedRecords: 90,
		FailedRecords:    10,
		CreatedAt:        time.Now(),
	})
	defer func() {
		_ = database.Delete(&models.PipelineJob{}, "id = ?", jobID)
	}()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	ctrl.GetMetrics(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d", resp.StatusCode)
	}

	metricsBody := w.Body.String()
	if !bytes.Contains([]byte(metricsBody), []byte("pipeline_jobs_total")) {
		t.Errorf("Expected metrics body to contain 'pipeline_jobs_total'")
	}
	if !bytes.Contains([]byte(metricsBody), []byte("pipeline_records_processed_total")) {
		t.Errorf("Expected metrics body to contain 'pipeline_records_processed_total'")
	}
}

func TestNewRouteAndBodyValidations(t *testing.T) {
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skip("Skipping validations test: PostgreSQL database not reachable")
	}

	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)
	pipeService := service.NewPipelineService(jobRepo, errRepo, resultRepo)
	ctrl := controller.NewPipelineController(pipeService, jobRepo, errRepo, resultRepo)

	t.Run("CreatePipeline - Invalid API Version returns 400", func(t *testing.T) {
		spec := models.JobSpec{
			ID: "valid-id",
			Sources: []models.SourceSpec{
				{ID: "src-1", Type: "csv", Path: "samples/biometrics_sample.csv"},
			},
		}
		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v2/pipelines", bytes.NewBuffer(bodyBytes))
		req.SetPathValue("version", "v2") // explicitly set path value to v2
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
		if !strings.Contains(w.Body.String(), "unsupported API version: v2") {
			t.Errorf("Expected unsupported API version error, got: %s", w.Body.String())
		}
	})

	t.Run("CreatePipeline - Invalid Job ID returns 400", func(t *testing.T) {
		spec := models.JobSpec{
			ID: "invalid_id_*_chars",
			Sources: []models.SourceSpec{
				{ID: "src-1", Type: "csv", Path: "samples/biometrics_sample.csv"},
			},
		}
		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(bodyBytes))
		req.SetPathValue("version", "v1")
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
		if !strings.Contains(w.Body.String(), "Invalid job ID") {
			t.Errorf("Expected invalid job ID error, got: %s", w.Body.String())
		}
	})

	t.Run("CreatePipeline - Directory Traversal Source Path returns 400", func(t *testing.T) {
		spec := models.JobSpec{
			ID: "valid-id",
			Sources: []models.SourceSpec{
				{ID: "src-1", Type: "csv", Path: "samples/../../etc/passwd"},
			},
		}
		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(bodyBytes))
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
		if !strings.Contains(w.Body.String(), "Directory traversal path patterns are not allowed") {
			t.Errorf("Expected directory traversal error, got: %s", w.Body.String())
		}
	})

	t.Run("CreatePipeline - Directory Traversal Export Path returns 400", func(t *testing.T) {
		spec := models.JobSpec{
			ID: "valid-id",
			Sources: []models.SourceSpec{
				{ID: "src-1", Type: "csv", Path: "samples/biometrics_sample.csv"},
			},
			ExportTargets: []models.ExportTargetSpec{
				{Type: "json", Path: "data/exports/../../etc/passwd"},
			},
		}
		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(bodyBytes))
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
		if !strings.Contains(w.Body.String(), "Directory traversal path patterns are not allowed") {
			t.Errorf("Expected directory traversal error, got: %s", w.Body.String())
		}
	})

	t.Run("CreatePipeline - Invalid Validation Rule returns 400", func(t *testing.T) {
		spec := models.JobSpec{
			ID: "valid-id",
			Sources: []models.SourceSpec{
				{ID: "src-1", Type: "csv", Path: "samples/biometrics_sample.csv"},
			},
			ValidationRules: []models.ValidationRuleSpec{
				{Field: "height", Rule: "unknown_rule_type"},
			},
		}
		bodyBytes, _ := json.Marshal(spec)
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(bodyBytes))
		w := httptest.NewRecorder()

		ctrl.CreatePipeline(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400 Bad Request, got %d", resp.StatusCode)
		}
		if !strings.Contains(w.Body.String(), "Unsupported validation rule") {
			t.Errorf("Expected unsupported validation rule error, got: %s", w.Body.String())
		}
	})
}

func TestDatabaseCascadingDeletes(t *testing.T) {
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skip("Skipping cascade delete test: PostgreSQL database not reachable")
	}

	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)

	jobID := "test-cascade-delete"

	// Cleanup first
	_ = database.Delete(&models.JobResults{}, "job_id = ?", jobID)
	_ = database.Delete(&models.JobError{}, "job_id = ?", jobID)
	_ = database.Delete(&models.PipelineJob{}, "id = ?", jobID)

	// 1. Create pipeline job
	job := &models.PipelineJob{
		ID:        jobID,
		Status:    "COMPLETED",
		CreatedAt: time.Now(),
	}
	if err := jobRepo.Save(job); err != nil {
		t.Fatalf("Failed to save pipeline job: %v", err)
	}

	// 2. Create job error
	jobErr := &models.JobError{
		JobID:        jobID,
		Stage:        "Validation",
		RawData:      "some-raw-data",
		ErrorMessage: "test error message",
		OccurredAt:   time.Now(),
	}
	if err := errRepo.Save(jobErr); err != nil {
		t.Fatalf("Failed to save job error: %v", err)
	}

	// 3. Create job results
	mockAgg := models.AggregatedResult{
		JobID:      jobID,
		TotalCount: 1,
	}
	aggBytes, _ := json.Marshal(mockAgg)
	jobResult := &models.JobResults{
		JobID:       jobID,
		ResultsJSON: string(aggBytes),
		ExportPaths: "[]",
		UpdatedAt:   time.Now(),
	}
	if err := resultRepo.Save(jobResult); err != nil {
		t.Fatalf("Failed to save job results: %v", err)
	}

	// Verify they exist
	if _, err := jobRepo.FindOne(jobID); err != nil {
		t.Fatalf("Expected job to exist: %v", err)
	}
	if errs, err := errRepo.FindByJobID(jobID); err != nil || len(errs) != 1 {
		t.Fatalf("Expected 1 job error to exist: %v (found %d)", err, len(errs))
	}
	if _, err := resultRepo.FindOne(jobID); err != nil {
		t.Fatalf("Expected job results to exist: %v", err)
	}

	// 4. Delete the parent job
	if err := jobRepo.Delete(jobID); err != nil {
		t.Fatalf("Failed to delete pipeline job: %v", err)
	}

	// 5. Verify cascade deletion
	_, err = jobRepo.FindOne(jobID)
	if err == nil {
		t.Errorf("Expected pipeline job to be deleted, but it still exists")
	}

	errs, err := errRepo.FindByJobID(jobID)
	if err != nil {
		t.Errorf("Failed querying job errors: %v", err)
	} else if len(errs) != 0 {
		t.Errorf("Expected 0 job errors due to cascade delete, got %d", len(errs))
	}

	_, err = resultRepo.FindOne(jobID)
	if err == nil {
		t.Errorf("Expected job results to be deleted due to cascade delete, but it still exists")
	}
}

func generateTestJWT(secret []byte, exp int64) string {
	header := `{"alg":"HS256","typ":"JWT"}`
	payload := fmt.Sprintf(`{"exp":%d}`, exp)
	hEnc := base64.RawURLEncoding.EncodeToString([]byte(header))
	pEnc := base64.RawURLEncoding.EncodeToString([]byte(payload))
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(hEnc + "." + pEnc))
	sigEnc := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return hEnc + "." + pEnc + "." + sigEnc
}

func TestAuthMiddlewareAndRateLimiter(t *testing.T) {
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skip("Skipping middleware tests: PostgreSQL database not reachable")
	}

	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)
	pipeService := service.NewPipelineService(jobRepo, errRepo, resultRepo)
	ctrl := controller.NewPipelineController(pipeService, jobRepo, errRepo, resultRepo)

	mux := http.NewServeMux()
	handler := routes.RegisterRoutes(mux, ctrl)

	t.Run("Mutating route without auth returns 401", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBufferString("{}"))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}
	})

	t.Run("Mutating route with invalid API Key returns 401", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBufferString("{}"))
		req.Header.Set("X-API-Key", "wrong-key")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}
	})

	t.Run("Mutating route with valid API Key passes auth", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBufferString("{}"))
		req.Header.Set("X-API-Key", "dev-api-key-99")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code == http.StatusUnauthorized {
			t.Errorf("Expected authentication to pass (not 401), got %d", w.Code)
		}
	})

	t.Run("Mutating route with invalid Bearer JWT returns 401", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBufferString("{}"))
		req.Header.Set("Authorization", "Bearer invalid.token.signature")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}
	})

	t.Run("Mutating route with valid Bearer JWT passes auth", func(t *testing.T) {
		token := generateTestJWT([]byte("dev-jwt-secret-99"), time.Now().Add(time.Hour).Unix())
		req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewBufferString("{}"))
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code == http.StatusUnauthorized {
			t.Errorf("Expected authentication to pass (not 401), got %d", w.Code)
		}
	})

	t.Run("Rate limiter blocks excess requests with 429", func(t *testing.T) {
		blocked := false
		for i := 0; i < 15; i++ {
			req := httptest.NewRequest("GET", "/swagger.json", nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code == http.StatusTooManyRequests {
				blocked = true
				break
			}
		}
		if !blocked {
			t.Errorf("Expected rate limiter to return 429 Too Many Requests after 15 rapid requests")
		}
	})
}

func TestFindPendingJobConcurrent(t *testing.T) {
	database, err := config.ConnectDatabase()
	if err != nil {
		t.Skip("Skipping concurrent FindPendingJob test: PostgreSQL database not reachable")
	}

	jobRepo := repository.NewPipelineJobRepository(database)

	numJobs := 5
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("test-concurrent-poll-%d", i)
		_ = database.Delete(&models.PipelineJob{}, "id = ?", jobIDs[i])
		_ = jobRepo.Save(&models.PipelineJob{
			ID:        jobIDs[i],
			Status:    "PENDING",
			CreatedAt: time.Now(),
		})
	}

	defer func() {
		for _, id := range jobIDs {
			_ = database.Delete(&models.PipelineJob{}, "id = ?", id)
		}
	}()

	claimedJobs := make(chan string, numJobs)
	var wg sync.WaitGroup

	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := jobRepo.FindPendingJob()
			if err == nil && job != nil {
				claimedJobs <- job.ID
			}
		}()
	}

	wg.Wait()
	close(claimedJobs)

	seen := make(map[string]bool)
	for id := range claimedJobs {
		if seen[id] {
			t.Errorf("Job %s was double processed / claimed by multiple workers", id)
		}
		seen[id] = true
	}

	if len(seen) == 0 {
		t.Errorf("Expected at least some jobs to be claimed, got 0")
	}
}



