/*
It initializes the folders and configuration schemas, loads environment variables from the .env file,
connects to PostgreSQL via GORM, sets up repositories, services, and controllers, registers routing endpoints,
and boots up the HTTP dashboard server with listener channels for graceful shutdown signals.
*/
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"data-processing-pipeline/apps/server/controller"
	"data-processing-pipeline/apps/server/routes"
	"data-processing-pipeline/apps/server/service"
	"data-processing-pipeline/packages/shared/config"
	"data-processing-pipeline/packages/shared/repository"

	"github.com/joho/godotenv"
)

func main() {
	// 1. Ensure required directory structures exist
	_ = os.MkdirAll("data/exports", 0755)
	_ = os.MkdirAll("samples", 0755)

	// 2. Auto-generate sample biometric dataset if missing
	ensureBiometricSample()

	// 3. Load environment variables from .env file if present
	if err := godotenv.Load(); err != nil {
		fmt.Println("[Info] No .env file loaded. Relying on system environment variables.")
	} else {
		fmt.Println("[Main] Loaded environment variables from .env file")
	}

	// 4. Connect to GORM PostgreSQL Database
	database, err := config.ConnectDatabase()
	if err != nil {
		fmt.Printf("[CRITICAL] Failed to connect to PostgreSQL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("[DB] Successfully connected to PostgreSQL and executed auto-migrations")

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
		fmt.Printf("[API] Server listening on http://%s\n", serverAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[CRITICAL] HTTP Server crashed: %v\n", err)
			os.Exit(1)
		}
	}()

	// 8. Graceful Shutdown listener
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	fmt.Println("\n[Main] Shutdown signal received. Stopping services gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("[Warning] Failed to stop HTTP Server cleanly: %v\n", err)
	}

	fmt.Println("[Main] Pipeline Service successfully stopped. Goodbye!")
}

func ensureBiometricSample() {
	path := filepath.Join("samples", "biometrics_sample.csv")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		content := `Index,Height,Weight
1,65.78,112.99
2,71.52,136.49
3,69.40,153.03
4,68.22,142.34
5,67.79,144.30
6,68.70,123.30
7,69.80,141.49
8,70.01,136.46
9,67.90,112.37
10,66.78,120.67
11,72.31,145.39
12,68.90,131.02
13,67.12,122.88
14,70.44,142.34
15,69.11,133.00
`
		err := os.WriteFile(path, []byte(content), 0644)
		if err != nil {
			fmt.Printf("[Error] Failed to generate biometrics sample CSV: %v\n", err)
		} else {
			fmt.Println("[Main] Generated local sample biometrics CSV in " + path)
		}
	}
}
