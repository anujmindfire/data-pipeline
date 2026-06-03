/*
Package routes manages the API endpoint routes and custom middleware declarations.

RegisterRoutes attaches controller functions to the ServeMux for routing, configures static file directories
for the dashboard SPA frontend application, and applies the CORS/Content-Type middleware handler.
*/
package routes

import (
	"net/http"
	"strings"
	"data-processing-pipeline/pkg/controller"
)

// RegisterRoutes registers all pipeline API endpoints and static SPA dashboard routes.
func RegisterRoutes(mux *http.ServeMux, ctrl *controller.PipelineController) http.Handler {
	// Static SPA Dashboard routes
	fs := http.FileServer(http.Dir("./web"))
	mux.Handle("GET /", fs)
	// Make sure sub-resources are also served
	mux.Handle("GET /index.html", fs)
	mux.Handle("GET /index.css", fs)
	mux.Handle("GET /index.js", fs)

	// API REST Endpoints
	mux.HandleFunc("POST /api/v1/pipelines", ctrl.CreatePipeline)
	mux.HandleFunc("GET /api/v1/pipelines", ctrl.ListPipelines)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", ctrl.GetPipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/progress", ctrl.GetProgress)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/results", ctrl.GetResults)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/errors", ctrl.GetErrors)
	mux.HandleFunc("PATCH /api/v1/pipelines/{id}/cancel", ctrl.CancelPipeline)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", ctrl.DeletePipeline)
	
	// Prometheus metrics endpoint
	mux.HandleFunc("GET /metrics", ctrl.GetMetrics)

	return corsMiddleware(mux)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if !strings.HasPrefix(r.URL.Path, "/metrics") && !strings.HasPrefix(r.URL.Path, "/index") && r.URL.Path != "/" {
			w.Header().Set("Content-Type", "application/json")
		}

		next.ServeHTTP(w, r)
	})
}
