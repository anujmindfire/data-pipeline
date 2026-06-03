package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"data-processing-pipeline/pkg/db"
)

// Registry tracks all running pipeline jobs in-memory for immediate, low-latency API access.
type Registry struct {
	mu   sync.RWMutex
	jobs map[string]*JobProgressTracker
}

var GlobalRegistry = &Registry{
	jobs: make(map[string]*JobProgressTracker),
}

// Register adds a new job run to the registry.
func (r *Registry) Register(jobID string, name string, cancel context.CancelFunc) *JobProgressTracker {
	r.mu.Lock()
	defer r.mu.Unlock()

	jp := &JobProgressTracker{
		JobID:          jobID,
		Name:           name,
		Status:         StatusPending,
		StartTime:      time.Now(),
		StageLatencies: make(map[string]double),
		CancelFunc:     cancel,
	}
	r.jobs[jobID] = jp
	return jp
}

// Get returns the progress status of a registered job.
func (r *Registry) Get(jobID string) (*JobProgressTracker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jp, ok := r.jobs[jobID]
	return jp, ok
}

// List returns all registered job progress states.
func (r *Registry) List() []*JobProgressTracker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*JobProgressTracker, 0, len(r.jobs))
	for _, jp := range r.jobs {
		list = append(list, jp)
	}
	return list
}

// Delete removes a job from the registry.
func (r *Registry) Delete(jobID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.jobs, jobID)
}

// JobProgressTracker represents the live status and telemetry of a pipeline job.
type JobProgressTracker struct {
	JobID            string            `json:"job_id"`
	Name             string            `json:"name"`
	Status           JobStatus         `json:"status"`
	TotalRecords     int64             `json:"total_records"`
	ProcessedRecords int64             `json:"processed_records"`
	FailedRecords    int64             `json:"failed_records"`
	ProcessingRate   float64           `json:"processing_rate"` // Records per second
	StartTime        time.Time         `json:"start_time"`
	EndTime          *time.Time        `json:"end_time,omitempty"`
	StageLatencies   map[string]double `json:"stage_latencies"` // Stage Name -> Latency in milliseconds
	CancelFunc       context.CancelFunc `json:"-"`
	mu               sync.RWMutex
}

// ToExternal converts the tracker to the public JobProgress struct.
func (jp *JobProgressTracker) ToExternal() JobProgress {
	jp.mu.RLock()
	defer jp.mu.RUnlock()

	percent := 0.0
	if jp.TotalRecords > 0 {
		percent = float64(jp.ProcessedRecords+jp.FailedRecords) * 100.0 / float64(jp.TotalRecords)
		if percent > 100 {
			percent = 100
		}
	}

	pending := jp.TotalRecords - (jp.ProcessedRecords + jp.FailedRecords)
	if pending < 0 {
		pending = 0
	}

	return JobProgress{
		JobID:            jp.JobID,
		RecordsProcessed: jp.ProcessedRecords,
		RecordsPending:   pending,
		ErrorCount:       jp.FailedRecords,
		PercentComplete:  percent,
		ProcessingRate:   jp.ProcessingRate,
		Name:             jp.Name,
		Status:           jp.Status,
		TotalRecords:     jp.TotalRecords,
		ProcessedRecords: jp.ProcessedRecords,
		FailedRecords:    jp.FailedRecords,
		StartTime:        jp.StartTime,
		EndTime:          jp.EndTime,
		StageLatencies:   jp.StageLatencies,
	}
}

// Cancel terminates a running job.
func (jp *JobProgressTracker) Cancel() {
	if jp.CancelFunc != nil {
		jp.CancelFunc()
	}
}

// Lock locks the progress metrics for write access.
func (jp *JobProgressTracker) Lock() {
	jp.mu.Lock()
}

// Unlock unlocks the progress metrics for write access.
func (jp *JobProgressTracker) Unlock() {
	jp.mu.Unlock()
}

// RLock locks the progress metrics for read access.
func (jp *JobProgressTracker) RLock() {
	jp.mu.RLock()
}

// RUnlock unlocks the progress metrics for read access.
func (jp *JobProgressTracker) RUnlock() {
	jp.mu.RUnlock()
}

// RunPipeline starts all pipeline stages concurrently, tracking metrics and syncing progress.
func RunPipeline(ctx context.Context, database *db.DB, spec *JobSpec) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register job in memory
	jp := GlobalRegistry.Register(spec.ID, spec.Name, cancel)

	jp.mu.Lock()
	jp.Status = StatusRunning
	jp.mu.Unlock()

	// Start database metadata tracking
	if err := database.StartJob(spec.ID); err != nil {
		return fmt.Errorf("failed to start database job tracking: %w", err)
	}

	// 1. Establish pipeline communication channels
	recordsCh := make(chan Record, 500)
	validatedCh := make(chan Record, 500)
	transformedCh := make(chan Record, 500)
	exportRecordsCh := make(chan Record, 500)
	resultCh := make(chan AggregatedResult, 1)
	
	// Telemetry and auditing channels
	progressCh := make(chan ProgressEvent, 500)
	errorCh := make(chan ErrorEvent, 500)
	exportDone := make(chan struct{})

	// Local counts for tracking ingestion total
	var totalIngested int64
	var activeSources sync.Map
	for _, s := range spec.Sources {
		activeSources.Store(s.ID, true)
	}

	// WaitGroups for background processes
	var bgWg sync.WaitGroup

	// 2. Start Background Error Collector
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		for errEv := range errorCh {
			// Write error to SQLite
			_ = database.InsertJobError(spec.ID, errEv.Stage, errEv.RawData, errEv.ErrorMessage)
			
			// Increment failed count if it is a record-level error
			if errEv.Stage != "export" && !strings.Contains(errEv.RawData, "setup") && !strings.Contains(errEv.RawData, "init") {
				jp.mu.Lock()
				jp.FailedRecords++
				jp.mu.Unlock()
			}
		}
	}()

	// 3. Start Background Progress Event Tracker
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		for progEv := range progressCh {
			jp.mu.Lock()
			
			// If ingestion record, count total ingested
			if progEv.Stage == "ingest" && progEv.Type == ProgressRecordProcessed {
				totalIngested += progEv.Delta
			}

			// If record successfully exported, it's counted as fully processed
			if progEv.Stage == "export" && progEv.Type == ProgressRecordProcessed {
				jp.ProcessedRecords += progEv.Delta
			}

			// If source completed, check if all sources are completed
			if progEv.Stage == "ingest" && progEv.Type == ProgressSourceCompleted {
				activeSources.Delete(progEv.SourceID)
				
				// Count remaining sources
				remainingSources := 0
				activeSources.Range(func(key, value interface{}) bool {
					remainingSources++
					return true
				})

				// Ingest stage latency
				if jp.StageLatencies["ingestion"] == 0 {
					jp.StageLatencies["ingestion"] = double(time.Since(jp.StartTime).Milliseconds())
				}

				if remainingSources == 0 {
					// All ingestion done, we know the exact total input size
					jp.TotalRecords = totalIngested
					_ = database.SetTotalRecords(spec.ID, int(totalIngested))
				}
			}

			// Record latency for stage completions in a race-free manner
			if progEv.Type == ProgressStageCompleted {
				if jp.StageLatencies[progEv.Stage] == 0 {
					jp.StageLatencies[progEv.Stage] = double(time.Since(jp.StartTime).Milliseconds())
				}
			}

			// Calculate real-time processing rate (records / second)
			elapsed := time.Since(jp.StartTime).Seconds()
			if elapsed > 0 {
				jp.ProcessingRate = float64(jp.ProcessedRecords+jp.FailedRecords) / elapsed
			}

			jp.mu.Unlock()
		}
	}()

	// 4. Start Throttled Database Syncer
	// SQLite updates progress metrics every 500ms to avoid I/O blocking
	syncStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				jp.mu.RLock()
				proc := int(jp.ProcessedRecords)
				fail := int(jp.FailedRecords)
				jp.mu.RUnlock()

				_ = database.SetJobCounts(spec.ID, proc, fail)
			case <-syncStop:
				return
			}
		}
	}()

	// 5. Start Pipeline Execution Stages
	
	// Stage 1: Ingestion
	var ingestWg sync.WaitGroup
	for _, src := range spec.Sources {
		ingestWg.Add(1)
		go func(s SourceSpec) {
			defer ingestWg.Done()
			IngestSource(ctx, spec.ID, s, recordsCh, progressCh, errorCh)
		}(src)
	}

	// Close recordsCh once all sources finish ingesting
	go func() {
		ingestWg.Wait()
		close(recordsCh)
	}()

	// Stage 2: Validation
	validationDone := StartValidationStage(ctx, spec, recordsCh, validatedCh, errorCh, progressCh)

	// Stage 3: Transformation
	transformationDone := StartTransformationStage(ctx, spec, validatedCh, transformedCh, errorCh, progressCh)

	// Stage 4: Aggregation
	aggregationDone := StartAggregationStage(ctx, spec, transformedCh, exportRecordsCh, resultCh, progressCh)

	// Stage 5: Export
	StartExportStage(ctx, spec, database, exportRecordsCh, resultCh, errorCh, progressCh, exportDone)

	// 7. Wait for Pipeline completion or Cancellation
	var finalStatus JobStatus
	var errorSummary *string

	// Wait for all stages to completely finish in order (race-free cascading shutdown)
	<-validationDone
	<-transformationDone
	<-aggregationDone
	<-exportDone

	if ctx.Err() == context.Canceled {
		finalStatus = StatusCancelled
		errSum := "Job was cancelled by user request"
		errorSummary = &errSum
		jp.mu.Lock()
		jp.Status = StatusCancelled
		jp.mu.Unlock()
	} else {
		jp.mu.RLock()
		failedCount := jp.FailedRecords
		processedCount := jp.ProcessedRecords
		jp.mu.RUnlock()

		jp.mu.Lock()
		jp.StageLatencies["export"] = double(time.Since(jp.StartTime).Milliseconds())
		
		if failedCount > 0 && processedCount == 0 {
			finalStatus = StatusFailed
			errSum := fmt.Sprintf("All records failed processing. Failed: %d", failedCount)
			errorSummary = &errSum
			jp.Status = StatusFailed
		} else {
			finalStatus = StatusCompleted
			jp.Status = StatusCompleted
		}
		jp.mu.Unlock()
	}

	// 8. Graceful Teardown of background telemetry
	close(progressCh)
	close(errorCh)
	bgWg.Wait()

	close(syncStop) // Stop tickers

	// 9. Sync final state to SQLite Database
	endTime := time.Now()
	jp.mu.Lock()
	jp.EndTime = &endTime
	proc := int(jp.ProcessedRecords)
	fail := int(jp.FailedRecords)
	jp.mu.Unlock()

	// Update the DB entry explicitly with final absolute stats
	err := database.CompleteJob(spec.ID, string(finalStatus), errorSummary)
	if err != nil {
		fmt.Printf("[Pipeline] Error finalizing job %s: %v\n", spec.ID, err)
	}

	// Force absolute counter sync
	_ = database.SetJobCounts(spec.ID, proc, fail)
	return nil
}

