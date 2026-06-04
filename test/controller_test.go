package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"data-processing-pipeline/apps/server/controller"
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
