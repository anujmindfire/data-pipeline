/*
JobResults stores the final outputs of a completed pipeline run in PostgreSQL.
It houses the final computed metrics as a JSON string, maps the absolute file paths on the host
machine where CSV/JSON record listings were exported, and tracks when the results were finalized.
*/
package models

import (
	"time"
)

type JobResults struct {
	JobID       string    `gorm:"primaryKey;type:varchar(50)" json:"job_id"`
	ResultsJSON string    `gorm:"type:text;not null" json:"results_json"`
	ExportPaths string    `gorm:"type:text;not null" json:"export_paths"`
	UpdatedAt   time.Time `json:"updated_at"`
}
