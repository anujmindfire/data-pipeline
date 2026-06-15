/*
It initializes the folders and configuration schemas, loads environment variables from the .env file,
connects to PostgreSQL via GORM, sets up repositories, services, and controllers, registers routing endpoints,
and boots up the HTTP dashboard server with listener channels for graceful shutdown signals.
*/
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"data-processing-pipeline/apps/server/controller"
	"data-processing-pipeline/apps/server/routes"
	"data-processing-pipeline/apps/server/service"
	"data-processing-pipeline/packages/shared/config"
	"data-processing-pipeline/packages/shared/logger"
	"data-processing-pipeline/packages/shared/repository"

	"github.com/joho/godotenv"
)

func main() {
	bg := context.Background()

	// 1. Ensure required directory structures exist
	if err := os.MkdirAll("data/exports", 0755); err != nil {
		logger.Error(bg, "Failed to create data/exports directory", "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll("samples", 0755); err != nil {
		logger.Error(bg, "Failed to create samples directory", "error", err)
		os.Exit(1)
	}

	// 3. Load environment variables from .env file if present
	if err := godotenv.Load(); err != nil {
		logger.Info(bg, "No .env file loaded, relying on system environment variables")
	} else {
		logger.Info(bg, "Loaded environment variables from .env file")
	}

	// 4. Connect to GORM PostgreSQL Database
	database, err := config.ConnectDatabase()
	if err != nil {
		logger.Error(bg, "Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	logger.Info(bg, "Connected to PostgreSQL and executed auto-migrations")

	// 5. Initialize clean layered architecture
	jobRepo := repository.NewPipelineJobRepository(database)
	errRepo := repository.NewJobErrorRepository(database)
	resultRepo := repository.NewJobResultsRepository(database)

	pipelineService := service.NewPipelineService(jobRepo, errRepo, resultRepo)
	ctrl := controller.NewPipelineController(pipelineService, jobRepo, errRepo, resultRepo)

	// 6. Set up multiplexer and register routes
	mux := http.NewServeMux()
	handler := routes.RegisterRoutes(mux, ctrl)

	serverAddr := "localhost:8080"
	if envAddr := os.Getenv("PIPELINE_ADDR"); envAddr != "" {
		serverAddr = envAddr
	}

	server := &http.Server{
		Addr:    serverAddr,
		Handler: handler,
	}

	// 7. Run Server in background
	go func() {
		logger.Info(bg, "Server listening", "addr", "http://"+serverAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(bg, "HTTP Server crashed", "error", err)
			os.Exit(1)
		}
	}()

	// 8. Graceful Shutdown listener
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	logger.Info(bg, "Shutdown signal received, stopping services gracefully")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn(bg, "Failed to stop HTTP Server cleanly", "error", err)
	}

	logger.Info(bg, "Pipeline Service successfully stopped")
}
