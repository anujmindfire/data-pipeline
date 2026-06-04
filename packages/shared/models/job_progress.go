/*
JobProgress represents real-time execution statistics queried during an active run.
It tracks metrics like processed/pending records, error count, overall completion percentage,
processing speed (records per second), start/end times, and worker pool latencies for the frontend dashboard.
*/
package models

import (
	"time"
)

type JobProgress struct {
	JobID            string            `json:"job_id"`
	RecordsProcessed int64             `json:"records_processed"`
	RecordsPending   int64             `json:"records_pending"`
	ErrorCount       int64             `json:"error_count"`
	PercentComplete  float64           `json:"percent_complete"`
	ProcessingRate   float64           `json:"processing_rate"`

	Name             string            `json:"name,omitempty"`
	Status           string            `json:"status,omitempty"`
	TotalRecords     int64             `json:"total_records,omitempty"`
	ProcessedRecords int64             `json:"processed_records,omitempty"`
	FailedRecords    int64             `json:"failed_records,omitempty"`
	StartTime        time.Time         `json:"start_time,omitempty"`
	EndTime          *time.Time        `json:"end_time,omitempty"`
	StageLatencies   map[string]float64 `json:"stage_latencies_ms,omitempty"`
}
