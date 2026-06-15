/*
It connects to the PostgreSQL database, registers repositories and services, and runs a polling loop
to fetch and process queued "PENDING" jobs sequentially.
*/
package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"data-processing-pipeline/apps/server/service"
	"data-processing-pipeline/packages/shared/config"
	"data-processing-pipeline/packages/shared/logger"
	"data-processing-pipeline/packages/shared/models"
	"data-processing-pipeline/packages/shared/repository"

	"github.com/joho/godotenv"
)

func main() {
	bg := context.Background()
	logger.Info(bg, "PIPELINE BACKGROUND WORKER SERVICE starting")

	// 1. Load environment variables
	if err := godotenv.Load(); err != nil {
		logger.Info(bg, "No .env file loaded, relying on system environment variables")
	} else {
		logger.Info(bg, "Loaded environment variables from .env file")
	}

	// 2. Connect to GORM PostgreSQL Database
	database, err := config.ConnectDatabase()
	if err != nil {
		logger.Error(bg, "Worker failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	logger.Info(bg, "Worker connected to PostgreSQL successfully")

	// 3. Initialize repos and services
	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)
	pipelineService := service.NewPipelineService(jobRepo, errRepo, resultRepo)

	// 4. Setup cancellation context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal listener for graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		logger.Info(bg, "Worker shutdown signal received, shutting down queue loops")
		cancel()
	}()

	logger.Info(bg, "Worker starting queue poll loop, watching for PENDING jobs")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Sequential execution flag: only process one job at a time
	var activeJobCancel context.CancelFunc
	jobDone := make(chan struct{})

	// Mark activeJobCancel initially nil
	isProcessing := false

	for {
		select {
		case <-ctx.Done():
			logger.Info(bg, "Worker loop stopped, exiting")
			return
		case <-jobDone:
			isProcessing = false
			if activeJobCancel != nil {
				activeJobCancel()
			}
			activeJobCancel = nil
			logger.Info(bg, "Active job finished, ready for next PENDING job")
		case <-ticker.C:
			if isProcessing {
				continue // Skip polling if already processing a job
			}

			// Poll database for a PENDING job
			pendingJob, err := jobRepo.FindPendingJob()
			if err != nil {
				// Record not found is normal if no jobs are queued
				continue
			}

			logger.Info(bg, "Found PENDING job", "job_id", pendingJob.ID)

			// Parse Config JSON into models.JobSpec
			var spec models.JobSpec
			if err := json.Unmarshal([]byte(pendingJob.Config), &spec); err != nil {
				errMsg := "Failed to parse spec config: " + err.Error()
				logger.Error(bg, "Job spec parsing failed", "job_id", pendingJob.ID, "error", errMsg)
				_ = jobRepo.Complete(pendingJob.ID, string(models.StatusFailed), &errMsg)
				continue
			}

			// Start processing job
			isProcessing = true
			var jobCtx context.Context
			jobCtx, activeJobCancel = context.WithCancel(ctx)

			go func(id string, js *models.JobSpec, jCtx context.Context) {
				defer func() {
					jobDone <- struct{}{}
				}()

				runCtx := logger.WithCorrelationID(jCtx, id)
				logger.Info(runCtx, "Executing pipeline run", "job_id", id)
				errRun := pipelineService.RunPipeline(runCtx, js)
				if errRun != nil {
					logger.Error(runCtx, "Pipeline execution failed", "job_id", id, "error", errRun)
				} else {
					logger.Info(runCtx, "Pipeline execution completed", "job_id", id)
				}
			}(pendingJob.ID, &spec, jobCtx)
		}
	}
}
