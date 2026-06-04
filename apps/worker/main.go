/*
Package main is the entry point for the background pipeline worker microservice.

It connects to the PostgreSQL database, registers repositories and services, and runs a polling loop
to fetch and process queued "PENDING" jobs sequentially.
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"data-processing-pipeline/packages/shared/config"
	"data-processing-pipeline/packages/shared/models"
	"data-processing-pipeline/packages/shared/repository"
	"data-processing-pipeline/apps/server/service"

	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("      PIPELINE BACKGROUND WORKER SERVICE          ")
	fmt.Println("==================================================")

	// 1. Load environment variables
	if err := godotenv.Load(); err != nil {
		fmt.Println("[Info] No .env file loaded. Relying on system environment variables.")
	} else {
		fmt.Println("[Worker] Loaded environment variables from .env file")
	}

	// 2. Connect to GORM PostgreSQL Database
	database, err := config.ConnectDatabase()
	if err != nil {
		fmt.Printf("[CRITICAL] Worker failed to connect to PostgreSQL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("[Worker DB] Connected to PostgreSQL successfully")

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
		fmt.Println("\n[Worker] Shutdown signal received. Shutting down queue loops...")
		cancel()
	}()

	fmt.Println("[Worker] Starting queue poll loop. Watching for PENDING jobs...")

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
			fmt.Println("[Worker] Worker loop stopped. Exiting...")
			return
		case <-jobDone:
			isProcessing = false
			if activeJobCancel != nil {
				activeJobCancel()
			}
			activeJobCancel = nil
			fmt.Println("[Worker] Active job finished. Ready to accept next PENDING job.")
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

			fmt.Printf("[Worker] Found PENDING job: ID=%s\n", pendingJob.ID)

			// Parse Config JSON into models.JobSpec
			var spec models.JobSpec
			if err := json.Unmarshal([]byte(pendingJob.Config), &spec); err != nil {
				errMsg := fmt.Sprintf("Failed to parse spec config: %v", err)
				fmt.Printf("[Worker Error] Job %s spec parsing failed: %s\n", pendingJob.ID, errMsg)
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

				fmt.Printf("[Worker] Executing pipeline run for job ID=%s...\n", id)
				errRun := pipelineService.RunPipeline(jCtx, js)
				if errRun != nil {
					fmt.Printf("[Worker Error] Pipeline execution failed for job ID=%s: %v\n", id, errRun)
				} else {
					fmt.Printf("[Worker] Pipeline execution completed for job ID=%s\n", id)
				}
			}(pendingJob.ID, &spec, jobCtx)
		}
	}
}
