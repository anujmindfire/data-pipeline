package pipeline

import (
	"time"
)

// JobSpec holds the configuration for a single pipeline run.
type JobSpec struct {
	ID               string                `json:"id"`
	Name             string                `json:"name"`
	Sources          []SourceSpec          `json:"sources"`
	ValidationRules  []ValidationRuleSpec  `json:"validation_rules"`
	TransformRules   []TransformRuleSpec   `json:"transform_rules"`
	AggregationSpecs []AggregationSpec     `json:"aggregation_specs"`
	ExportTargets    []ExportTargetSpec    `json:"export_targets"`
	Workers          WorkerConfig          `json:"workers"`
}

// WorkerConfig defines worker pool sizes for parallel stages.
type WorkerConfig struct {
	Validation     int `json:"validation"`
	Transformation int `json:"transformation"`
}

// SourceSpec represents an input data source.
type SourceSpec struct {
	ID     string            `json:"id"`
	Type   string            `json:"type"` // "csv", "json", "api"
	Path   string            `json:"path"` // Local path or HTTP URL
	Schema map[string]string `json:"schema"` // Mapping of raw column -> standard field name
}

// ValidationRuleSpec defines the checks to run against data fields.
type ValidationRuleSpec struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`  // "required", "min", "max", "type", "regex"
	Param string `json:"param"` // E.g., "0" for min, "float" for type, regex string
}

// TransformRuleSpec defines actions to clean or enrich data.
type TransformRuleSpec struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`  // "cast", "trim", "lower", "upper", "enrich_time", "add_constant"
	Param string `json:"param"` // E.g., "float", "int", "string", value
}

// AggregationSpec defines computed statistics.
type AggregationSpec struct {
	Field   string `json:"field"`             // Field to aggregate
	Func    string `json:"func"`              // "count", "sum", "avg", "min", "max"
	GroupBy string `json:"group_by,omitempty"` // Field to group by
	Target  string `json:"target"`            // Key name in the final results map
}

// ExportTargetSpec represents an output persistence destination.
type ExportTargetSpec struct {
	Type string `json:"type"` // "csv", "json", "db"
	Path string `json:"path"` // File path or table name
}

// Record is the unified payload flowing through the pipeline channels.
type Record struct {
	JobID     string         `json:"job_id"`
	SourceID  string         `json:"source_id"`
	RecordID  string         `json:"record_id"` // E.g., line number or identifier
	RawData   map[string]any `json:"raw_data,omitempty"`
	Payload   map[string]any `json:"payload"`
	Valid     bool           `json:"valid"`
	Errors    []string       `json:"errors,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

// JobStatus represents the state of a job run.
type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusCancelled JobStatus = "CANCELLED"
)

// PipelineMetric holds structural diagnostic parameters for monitoring.
type PipelineMetric struct {
	JobID            string            `json:"job_id"`
	Status           JobStatus         `json:"status"`
	TotalRecords     int64             `json:"total_records"`
	ProcessedRecords int64             `json:"processed_records"`
	FailedRecords    int64             `json:"failed_records"`
	ProcessingRate   float64           `json:"processing_rate_eps"` // Events per second
	StartTime        time.Time         `json:"start_time"`
	EndTime          *time.Time        `json:"end_time,omitempty"`
	StageLatencies   map[string]double `json:"stage_latencies_ms"`
}

// Custom double for compatibility if we want simpler floats.
type double = float64
