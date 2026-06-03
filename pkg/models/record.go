/*
Record is the internal message envelope that streams concurrently through pipeline stage channels.
It tracks the record source, unique row offsets, parsed payload map, verification state (IsValid),
individual field errors, raw bytes for audit logs, and processing timestamps.
*/
package models

import (
	"time"
)

type Record struct {
	JobID      string         `json:"job_id"`
	RowID      int64          `json:"row_id"`
	RawPayload []byte         `json:"raw_payload"`
	ParsedData map[string]any `json:"parsed_data"`
	IsValid    bool           `json:"is_valid"`

	SourceID   string         `json:"source_id,omitempty"`
	Errors     []string       `json:"errors,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
}
