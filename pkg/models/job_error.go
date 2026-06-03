/*
JobError audits and records runtime failures of individual records at different pipeline stages.
It tracks the associated JobID, the pipeline stage where the error occurred (Ingestion, Validation,
or Transformation), a raw string representation of the problematic payload, and the specific failure reason.
*/
package models

import (
	"time"
)

type JobError struct {
	ID           uint      `gorm:"primaryKey;autoIncrement" json:"id"`
	JobID        string    `gorm:"index;type:varchar(50);not null" json:"job_id"`
	Stage        string    `gorm:"type:varchar(50);not null" json:"stage"`
	RawData      string    `gorm:"type:text" json:"raw_data"`
	ErrorMessage string    `gorm:"type:text;not null" json:"error_message"`
	OccurredAt   time.Time `json:"occurred_at"`
}
