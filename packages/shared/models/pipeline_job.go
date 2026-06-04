/*
PipelineJob represents a single pipeline run's persistent state stored in PostgreSQL.
It tracks the overall metadata, job status (PENDING, RUNNING, COMPLETED, etc.),
total processed/failed records, and timestamps for scheduling, starting, and completing the run.
*/
package models

import (
	"time"
)

type PipelineJob struct {
	ID               string     `gorm:"primaryKey;type:varchar(50)" json:"id"`
	Status           string     `gorm:"type:varchar(20);not null" json:"status"`
	Config           string     `gorm:"type:text" json:"config"`
	TotalRecords     int        `gorm:"default:0" json:"total_records"`
	ProcessedRecords int        `gorm:"default:0" json:"processed_records"`
	FailedRecords    int        `gorm:"default:0" json:"failed_records"`
	CreatedAt        time.Time  `json:"created_at"`
	StartedAt        *time.Time `json:"started_at"`
	CompletedAt      *time.Time `json:"completed_at"`
	ErrorSummary     *string    `gorm:"type:text" json:"error_summary,omitempty"`
}

type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusCancelled JobStatus = "CANCELLED"
)
