package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"data-processing-pipeline/pkg/db"
)

// StartExportStage handles streaming record exports and writing final aggregations.
func StartExportStage(
	ctx context.Context,
	jobSpec *JobSpec,
	database *db.DB,
	transformedCh <-chan Record,
	resultCh <-chan AggregatedResult,
	errorCh chan<- ErrorEvent,
	progressCh chan<- ProgressEvent,
	exportDone chan<- struct{},
) {
	go func() {
		defer close(exportDone)

		var jsonFile *os.File
		var csvFile *os.File
		var csvWriter *csv.Writer
		var csvHeaders []string
		var recordCount int64

		// Output file paths stored to save in database
		exportedPaths := []string{}

		// Initialize export files based on configurations
		for _, target := range jobSpec.ExportTargets {
			dir := filepath.Dir(target.Path)
			if err := os.MkdirAll(dir, 0755); err != nil {
				sendExportError(jobSpec.ID, "setup_dirs", err, errorCh)
				return
			}

			switch strings.ToLower(target.Type) {
			case "json":
				file, err := os.Create(target.Path)
				if err != nil {
					sendExportError(jobSpec.ID, "create_json_file", err, errorCh)
					return
				}
				jsonFile = file
				exportedPaths = append(exportedPaths, target.Path)
				// Write starting bracket for JSON array
				_, _ = jsonFile.WriteString("[\n")

			case "csv":
				file, err := os.Create(target.Path)
				if err != nil {
					sendExportError(jobSpec.ID, "create_csv_file", err, errorCh)
					return
				}
				csvFile = file
				csvWriter = csv.NewWriter(csvFile)
				exportedPaths = append(exportedPaths, target.Path)
			}
		}

		// Close file streams at completion
		defer func() {
			if jsonFile != nil {
				_, _ = jsonFile.WriteString("\n]")
				jsonFile.Close()
			}
			if csvWriter != nil {
				csvWriter.Flush()
			}
			if csvFile != nil {
				csvFile.Close()
			}
		}()

		// Consume transformed records for streaming export
		consumerChan := make(chan Record, 100)
		
		// We spawn a helper to feed the consumer to avoid blocking the main thread
		go func() {
			defer close(consumerChan)
			for record := range transformedCh {
				consumerChan <- record
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case record, ok := <-consumerChan:
				if !ok {
					goto ReadResults
				}

				recordCount++

				// 1. Export as JSON Stream
				if jsonFile != nil {
					data, err := json.Marshal(record.ParsedData)
					if err != nil {
						sendExportError(jobSpec.ID, "marshal_json_record", err, errorCh)
					} else {
						if recordCount > 1 {
							_, _ = jsonFile.WriteString(",\n")
						}
						_, _ = jsonFile.Write(data)
					}
				}

				// 2. Export as CSV Stream
				if csvWriter != nil {
					if csvHeaders == nil {
						// Collect headers from payload
						csvHeaders = make([]string, 0, len(record.ParsedData))
						for k := range record.ParsedData {
							csvHeaders = append(csvHeaders, k)
						}
						if err := csvWriter.Write(csvHeaders); err != nil {
							sendExportError(jobSpec.ID, "write_csv_headers", err, errorCh)
						}
					}

					row := make([]string, len(csvHeaders))
					for i, h := range csvHeaders {
						val := record.ParsedData[h]
						if val == nil {
							row[i] = ""
						} else {
							row[i] = fmt.Sprintf("%v", val)
						}
					}

					if err := csvWriter.Write(row); err != nil {
						sendExportError(jobSpec.ID, "write_csv_row", err, errorCh)
					}
				}

				// Report record export progress
				progressCh <- ProgressEvent{
					JobID:    jobSpec.ID,
					SourceID: record.SourceID,
					Stage:    "export",
					Type:     ProgressRecordProcessed,
					Delta:    1,
				}
			}
		}

	ReadResults:
		// Now consume final aggregated results
		select {
		case <-ctx.Done():
			return
		case results, ok := <-resultCh:
			if !ok {
				results = AggregatedResult{
					JobID: jobSpec.ID,
				}
			}

			// Export final aggregations JSON
			resultsJSON, err := json.MarshalIndent(results, "", "  ")
			if err != nil {
				sendExportError(jobSpec.ID, "marshal_results", err, errorCh)
				return
			}

			// Save aggregated file path
			resultsPath := filepath.Join(filepath.Dir(jobSpec.ExportTargets[0].Path), "summary_results.json")
			if len(jobSpec.ExportTargets) > 0 {
				if err := os.WriteFile(resultsPath, resultsJSON, 0644); err == nil {
					exportedPaths = append(exportedPaths, resultsPath)
				} else {
					sendExportError(jobSpec.ID, "write_results_file", err, errorCh)
				}
			}

			// Save into SQLite Database
			pathsJSON, _ := json.Marshal(exportedPaths)
			if err := database.SaveJobResults(jobSpec.ID, string(resultsJSON), string(pathsJSON)); err != nil {
				sendExportError(jobSpec.ID, "db_save_results", err, errorCh)
			}
			progressCh <- ProgressEvent{
				JobID:    jobSpec.ID,
				Stage:    "export",
				Type:     ProgressStageCompleted,
				Delta:    0,
			}
		}
	}()
}

func sendExportError(jobID string, ref string, err error, errorCh chan<- ErrorEvent) {
	errorCh <- ErrorEvent{
		JobID:        jobID,
		Stage:        "export",
		RawData:      fmt.Sprintf(`{"ref": "%s"}`, ref),
		ErrorMessage: err.Error(),
	}
}

