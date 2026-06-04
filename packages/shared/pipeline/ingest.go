package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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
		req, errReq := http.NewRequestWithContext(ctx, "GET", src.Path, nil)
		if errReq != nil {
			sendIngestError(jobID, src.ID, "init_request", errReq, errorCh)
			return
		}

		resp, errResp := http.DefaultClient.Do(req)
		if errResp != nil {
			sendIngestError(jobID, src.ID, "fetch_http", errResp, errorCh)
			return
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			sendIngestError(jobID, src.ID, "fetch_http", fmt.Errorf("bad status code: %d", resp.StatusCode), errorCh)
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

func parseCSV(ctx context.Context, jobID string, src models.SourceSpec, reader io.Reader, recordsCh chan<- models.Record, progressCh chan<- ProgressEvent, errorCh chan<- ErrorEvent) error {
	csvReader := csv.NewReader(reader)
	
	// Read headers
	headers, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	lineNum := 1
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
		// Construct record
		rawData := make(map[string]any)
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
	payload := make(map[string]any)
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

// Custom parser to aid basic conversions
func ParseFloat(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		clean := strings.TrimSpace(v)
		if clean == "" || clean == "null" || clean == "N/A" {
			return 0, nil
		}
		return strconv.ParseFloat(clean, 64)
	default:
		return 0, fmt.Errorf("unable to convert type %T to float", val)
	}
}
