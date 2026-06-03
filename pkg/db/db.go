package db

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type PipelineJob struct {
	ID               string     `json:"id"`
	Status           string     `json:"status"`
	Config           string     `json:"config"`
	TotalRecords     int        `json:"total_records"`
	ProcessedRecords int        `json:"processed_records"`
	FailedRecords    int        `json:"failed_records"`
	CreatedAt        time.Time  `json:"created_at"`
	StartedAt        *time.Time `json:"started_at"`
	CompletedAt      *time.Time `json:"completed_at"`
	ErrorSummary     *string    `json:"error_summary,omitempty"`
}

type JobError struct {
	ID           int       `json:"id"`
	JobID        string    `json:"job_id"`
	Stage        string    `json:"stage"`
	RawData      string    `json:"raw_data"`
	ErrorMessage string    `json:"error_message"`
	OccurredAt   time.Time `json:"occurred_at"`
}

type JobResults struct {
	JobID       string    `json:"job_id"`
	ResultsJSON string    `json:"results_json"`
	ExportPaths string    `json:"export_paths"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type DB struct {
	db *sql.DB
	mu sync.RWMutex
}

// NewDB opens a new SQLite database at the specified path and initializes it.
func NewDB(filepath string) (*DB, error) {
	conn, err := sql.Open("sqlite", filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure SQLite for high concurrency and robustness
	_, err = conn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	_, err = conn.Exec("PRAGMA busy_timeout=5000;")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	d := &DB{db: conn}
	if err := d.initSchemas(); err != nil {
		conn.Close()
		return nil, err
	}

	return d, nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

// initSchemas creates the database tables if they do not exist.
func (d *DB) initSchemas() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			config TEXT NOT NULL,
			total_records INT DEFAULT 0,
			processed_records INT DEFAULT 0,
			failed_records INT DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME,
			completed_at DATETIME,
			error_summary TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS job_errors (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			raw_data TEXT NOT NULL,
			error_message TEXT NOT NULL,
			occurred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS job_results (
			job_id TEXT PRIMARY KEY,
			results_json TEXT NOT NULL,
			export_paths TEXT NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
		);`,
	}

	for _, query := range queries {
		if _, err := d.db.Exec(query); err != nil {
			return fmt.Errorf("schema init failed: %w", err)
		}
	}
	return nil
}

// CreateJob registers a new job entry.
func (d *DB) CreateJob(id string, name string, configSpec string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `INSERT INTO jobs (id, status, config, created_at) VALUES (?, 'PENDING', ?, ?)`
	_, err := d.db.Exec(query, id, configSpec, time.Now())
	return err
}

// StartJob updates status to RUNNING and sets started_at.
func (d *DB) StartJob(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `UPDATE jobs SET status = 'RUNNING', started_at = ? WHERE id = ?`
	_, err := d.db.Exec(query, time.Now(), id)
	return err
}

// UpdateJobStatus updates the status of the job.
func (d *DB) UpdateJobStatus(id string, status string, errorSummary *string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var err error
	if errorSummary != nil {
		query := `UPDATE jobs SET status = ?, error_summary = ? WHERE id = ?`
		_, err = d.db.Exec(query, status, *errorSummary, id)
	} else {
		query := `UPDATE jobs SET status = ? WHERE id = ?`
		_, err = d.db.Exec(query, status, id)
	}
	return err
}

// CompleteJob sets the status to COMPLETED or FAILED and marks completion time.
func (d *DB) CompleteJob(id string, status string, errorSummary *string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `UPDATE jobs SET status = ?, completed_at = ?, error_summary = ? WHERE id = ?`
	_, err := d.db.Exec(query, status, time.Now(), errorSummary, id)
	return err
}

// SetTotalRecords sets the estimated or total number of records for a job.
func (d *DB) SetTotalRecords(id string, total int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `UPDATE jobs SET total_records = ? WHERE id = ?`
	_, err := d.db.Exec(query, total, id)
	return err
}

// IncrementJobProgress increments progress metrics.
func (d *DB) IncrementJobProgress(id string, processedCount int, failedCount int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `UPDATE jobs SET 
		processed_records = processed_records + ?, 
		failed_records = failed_records + ? 
		WHERE id = ?`
	_, err := d.db.Exec(query, processedCount, failedCount, id)
	return err
}

// SetJobCounts directly updates absolute processed and failed counts for a job.
func (d *DB) SetJobCounts(id string, processedCount int, failedCount int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `UPDATE jobs SET 
		processed_records = ?, 
		failed_records = ? 
		WHERE id = ?`
	_, err := d.db.Exec(query, processedCount, failedCount, id)
	return err
}

// InsertJobError stores validation or runtime failure details.
func (d *DB) InsertJobError(jobID string, stage string, rawData string, errMsg string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `INSERT INTO job_errors (job_id, stage, raw_data, error_message, occurred_at) 
		VALUES (?, ?, ?, ?, ?)`
	_, err := d.db.Exec(query, jobID, stage, rawData, errMsg, time.Now())
	return err
}

// SaveJobResults stores aggregation results and paths to exported files.
func (d *DB) SaveJobResults(jobID string, resultsJSON string, exportPathsJSON string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `INSERT INTO job_results (job_id, results_json, export_paths, updated_at) 
		VALUES (?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET 
			results_json = excluded.results_json,
			export_paths = excluded.export_paths,
			updated_at = excluded.updated_at`
	_, err := d.db.Exec(query, jobID, resultsJSON, exportPathsJSON, time.Now())
	return err
}

// GetJob returns job metadata by ID.
func (d *DB) GetJob(id string) (*PipelineJob, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `SELECT id, status, config, total_records, processed_records, failed_records, created_at, started_at, completed_at, error_summary 
		FROM jobs WHERE id = ?`
	row := d.db.QueryRow(query, id)

	var j PipelineJob
	var startedAt, completedAt sql.NullTime
	var errorSummary sql.NullString

	err := row.Scan(&j.ID, &j.Status, &j.Config, &j.TotalRecords, &j.ProcessedRecords, &j.FailedRecords, &j.CreatedAt, &startedAt, &completedAt, &errorSummary)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job not found: %s", id)
	} else if err != nil {
		return nil, err
	}

	if startedAt.Valid {
		j.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		j.CompletedAt = &completedAt.Time
	}
	if errorSummary.Valid {
		j.ErrorSummary = &errorSummary.String
	}

	return &j, nil
}

// ListJobs retrieves all jobs.
func (d *DB) ListJobs() ([]PipelineJob, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `SELECT id, status, config, total_records, processed_records, failed_records, created_at, started_at, completed_at, error_summary 
		FROM jobs ORDER BY created_at DESC`
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []PipelineJob
	for rows.Next() {
		var j PipelineJob
		var startedAt, completedAt sql.NullTime
		var errorSummary sql.NullString

		err := rows.Scan(&j.ID, &j.Status, &j.Config, &j.TotalRecords, &j.ProcessedRecords, &j.FailedRecords, &j.CreatedAt, &startedAt, &completedAt, &errorSummary)
		if err != nil {
			return nil, err
		}

		if startedAt.Valid {
			j.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			j.CompletedAt = &completedAt.Time
		}
		if errorSummary.Valid {
			j.ErrorSummary = &errorSummary.String
		}

		list = append(list, j)
	}
	return list, nil
}

// GetJobErrors retrieves all errors related to a job ID.
func (d *DB) GetJobErrors(id string) ([]JobError, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `SELECT id, job_id, stage, raw_data, error_message, occurred_at 
		FROM job_errors WHERE job_id = ? ORDER BY occurred_at ASC`
	rows, err := d.db.Query(query, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []JobError
	for rows.Next() {
		var je JobError
		err := rows.Scan(&je.ID, &je.JobID, &je.Stage, &je.RawData, &je.ErrorMessage, &je.OccurredAt)
		if err != nil {
			return nil, err
		}
		list = append(list, je)
	}
	return list, nil
}

// GetJobResults retrieves the job results by ID.
func (d *DB) GetJobResults(id string) (*JobResults, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `SELECT job_id, results_json, export_paths, updated_at FROM job_results WHERE job_id = ?`
	row := d.db.QueryRow(query, id)

	var jr JobResults
	err := row.Scan(&jr.JobID, &jr.ResultsJSON, &jr.ExportPaths, &jr.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("results not found for job: %s", id)
	} else if err != nil {
		return nil, err
	}
	return &jr, nil
}

// DeleteJob deletes a job and cascades database details.
func (d *DB) DeleteJob(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `DELETE FROM jobs WHERE id = ?`
	_, err := d.db.Exec(query, id)
	return err
}
