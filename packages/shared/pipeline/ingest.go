package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"data-processing-pipeline/packages/shared/logger"
	"data-processing-pipeline/packages/shared/models"
)

// IngestSource orchestrates reading from a specific source configuration and sends unified Records.
func IngestSource(ctx context.Context, jobID string, src models.SourceSpec, recordsCh chan<- models.Record, progressCh chan<- ProgressEvent, errorCh chan<- ErrorEvent) {
	defer func() {
		// Signal source ingestion completion
		progressCh <- ProgressEvent{
			JobID:    jobID,
			SourceID: src.ID,
			Stage:    "ingest",
			Type:     ProgressSourceCompleted,
		}
	}()

	var reader io.ReadCloser
	var err error

	// Determine source location type (HTTP or Local)
	isHTTP := strings.HasPrefix(src.Path, "http://") || strings.HasPrefix(src.Path, "https://")

	if isHTTP {
		resp, errFetch := fetchWithRetry(ctx, src.Path)
		if errFetch != nil {
			sendIngestError(jobID, src.ID, "fetch_http", errFetch, errorCh)
			return
		}
		reader = resp.Body
	} else {
		file, errFile := os.Open(src.Path)
		if errFile != nil {
			sendIngestError(jobID, src.ID, "open_file", errFile, errorCh)
			return
		}
		reader = file
	}
	defer reader.Close()

	// Parse based on type
	switch strings.ToLower(src.Type) {
	case "csv":
		err = parseCSV(ctx, jobID, src, reader, recordsCh, progressCh, errorCh)
	case "json", "api":
		err = parseJSON(ctx, jobID, src, reader, recordsCh, progressCh, errorCh)
	default:
		err = fmt.Errorf("unsupported source type: %s", src.Type)
		sendIngestError(jobID, src.ID, "unsupported_type", err, errorCh)
	}

	if err != nil {
		sendIngestError(jobID, src.ID, "parsing", err, errorCh)
	}
}

// fetchWithRetry performs an HTTP GET with up to 3 retries and exponential backoff (1s, 2s, 4s).
// Network errors and 5xx server errors are retried; 4xx client errors fail immediately.
func fetchWithRetry(ctx context.Context, rawURL string) (*http.Response, error) {
	const maxAttempts = 4
	backoff := time.Second
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
		if err != nil {
			return nil, err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			logger.Warn(ctx, "HTTP fetch failed, will retry",
				"url", rawURL, "attempt", attempt, "max_attempts", maxAttempts, "error", err)
			continue
		}

		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			logger.Warn(ctx, "HTTP fetch server error, will retry",
				"url", rawURL, "attempt", attempt, "status", resp.StatusCode)
			continue
		}

		if resp.StatusCode >= 400 {
			resp.Body.Close()
			return nil, fmt.Errorf("bad status code: %d", resp.StatusCode)
		}

		return resp, nil
	}

	return nil, fmt.Errorf("all %d fetch attempts failed: %w", maxAttempts, lastErr)
}

func parseCSV(ctx context.Context, jobID string, src models.SourceSpec, reader io.Reader, recordsCh chan<- models.Record, progressCh chan<- ProgressEvent, errorCh chan<- ErrorEvent) error {
	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	
	// Read headers
	headers, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	for i, h := range headers {
		headers[i] = strings.Trim(strings.TrimSpace(h), "\"")
	}

	lineNum := 1
	rawData := make(map[string]any, len(headers))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			sendIngestError(jobID, src.ID, fmt.Sprintf("line_%d", lineNum), err, errorCh)
			lineNum++
			continue
		}

		lineNum++
		
		// Clear reused map to avoid reallocation
		for k := range rawData {
			delete(rawData, k)
		}

		// Construct record
		for i, val := range row {
			if i < len(headers) {
				rawData[headers[i]] = val
			}
		}

		payload := mapRecord(rawData, src.Schema)
		rawBytes, _ := json.Marshal(rawData)

		recordsCh <- models.Record{
			JobID:      jobID,
			RowID:      int64(lineNum),
			RawPayload: rawBytes,
			ParsedData: payload,
			IsValid:    true,
			SourceID:   src.ID,
			Timestamp:  time.Now(),
		}

		progressCh <- ProgressEvent{
			JobID:    jobID,
			SourceID: src.ID,
			Stage:    "ingest",
			Type:     ProgressRecordProcessed,
			Delta:    1,
		}
	}
	return nil
}

func parseJSON(ctx context.Context, jobID string, src models.SourceSpec, reader io.Reader, recordsCh chan<- models.Record, progressCh chan<- ProgressEvent, errorCh chan<- ErrorEvent) error {
	// Decode JSON
	var raw interface{}
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	switch val := raw.(type) {
	case []interface{}: // Array of JSON records
		for idx, item := range val {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			recordMap, ok := item.(map[string]interface{})
			if !ok {
				sendIngestError(jobID, src.ID, fmt.Sprintf("index_%d", idx), fmt.Errorf("record is not a JSON object"), errorCh)
				continue
			}

			payload := mapRecord(recordMap, src.Schema)
			rawBytes, _ := json.Marshal(recordMap)
			recordsCh <- models.Record{
				JobID:      jobID,
				RowID:      int64(idx + 1),
				RawPayload: rawBytes,
				ParsedData: payload,
				IsValid:    true,
				SourceID:   src.ID,
				Timestamp:  time.Now(),
			}

			progressCh <- ProgressEvent{
				JobID:    jobID,
				SourceID: src.ID,
				Stage:    "ingest",
				Type:     ProgressRecordProcessed,
				Delta:    1,
			}
		}
	case map[string]interface{}: // Single JSON object
		payload := mapRecord(val, src.Schema)
		rawBytes, _ := json.Marshal(val)
		recordsCh <- models.Record{
			JobID:      jobID,
			RowID:      1,
			RawPayload: rawBytes,
			ParsedData: payload,
			IsValid:    true,
			SourceID:   src.ID,
			Timestamp:  time.Now(),
		}

		progressCh <- ProgressEvent{
			JobID:    jobID,
			SourceID: src.ID,
			Stage:    "ingest",
			Type:     ProgressRecordProcessed,
			Delta:    1,
		}
	default:
		return fmt.Errorf("unexpected JSON structure: must be an array or an object")
	}

	return nil
}

// Maps raw map fields to target schema standard names, supporting direct copies for non-mapped fields.
func mapRecord(raw map[string]any, mapping map[string]string) map[string]any {
	payload := make(map[string]any, len(raw)+len(mapping))
	// Base copy of all fields
	for k, v := range raw {
		payload[k] = v
	}

	// Apply schema mappings (overwrite or add new standardized keys)
	for rawKey, targetKey := range mapping {
		if val, ok := raw[rawKey]; ok {
			payload[targetKey] = val
		}
	}
	return payload
}

func sendIngestError(jobID, sourceID, ref string, err error, errorCh chan<- ErrorEvent) {
	errorCh <- ErrorEvent{
		JobID:        jobID,
		Stage:        "ingest",
		RawData:      fmt.Sprintf(`{"source_id": "%s", "ref": "%s"}`, sourceID, ref),
		ErrorMessage: err.Error(),
	}
}

// Types of progress updates
type ProgressEventType string

const (
	ProgressRecordProcessed ProgressEventType = "record_processed"
	ProgressSourceCompleted ProgressEventType = "source_completed"
	ProgressStageCompleted  ProgressEventType = "stage_completed"
)

// ProgressEvent wraps progress reports flowing through channels
type ProgressEvent struct {
	JobID    string            `json:"job_id"`
	SourceID string            `json:"source_id"`
	Stage    string            `json:"stage"`
	Type     ProgressEventType `json:"type"`
	Delta    int64             `json:"delta"`
}

// ErrorEvent wraps record-level or stage-level errors
type ErrorEvent struct {
	JobID        string `json:"job_id"`
	Stage        string `json:"stage"`
	RawData      string `json:"raw_data"`
	ErrorMessage string `json:"error_message"`
}
