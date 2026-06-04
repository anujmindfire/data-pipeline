/*
Package routes manages the API endpoint routes and custom middleware declarations.

RegisterRoutes attaches controller functions to the ServeMux for routing, configures static file directories
for the dashboard SPA frontend application, and applies the CORS/Content-Type middleware handler.
*/
package routes

import (
	"net/http"
	"os"
	"strings"

	"data-processing-pipeline/apps/server/controller"
	"data-processing-pipeline/packages/shared/utils"
)

// getStaticDir dynamically finds the path to the frontend assets.
func getStaticDir() string {
	if dir := os.Getenv("STATIC_DIR"); dir != "" {
		return dir
	}
	candidates := []string{
		"./apps/web",
		"./apps/dashboard/web",
		"./apps/dashboard",
		"./web",
		"../web",
		"../dashboard/web",
		"../dashboard",
		"../../apps/web",
		"../../apps/dashboard/web",
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c
		}
	}
	return "./web"
}

// RegisterRoutes registers all pipeline API endpoints and static SPA dashboard routes.
func RegisterRoutes(mux *http.ServeMux, ctrl *controller.PipelineController) http.Handler {
	// Static SPA Dashboard routes
	staticDir := getStaticDir()
	fs := http.FileServer(http.Dir(staticDir))
	mux.Handle("GET /", fs)
	// Make sure sub-resources are also served
	mux.Handle("GET /index.html", fs)
	mux.Handle("GET /style.css", fs)
	mux.Handle("GET /app.js", fs)

	// API REST Endpoints
	mux.HandleFunc(utils.RouteCreatePipeline, ctrl.CreatePipeline)
	mux.HandleFunc(utils.RouteListPipelines, ctrl.ListPipelines)
	mux.HandleFunc(utils.RouteGetPipeline, ctrl.GetPipeline)
	mux.HandleFunc(utils.RouteGetProgress, ctrl.GetProgress)
	mux.HandleFunc(utils.RouteGetResults, ctrl.GetResults)
	mux.HandleFunc(utils.RouteGetErrors, ctrl.GetErrors)
	mux.HandleFunc(utils.RouteCancelPipeline, ctrl.CancelPipeline)
	mux.HandleFunc(utils.RouteDeletePipeline, ctrl.DeletePipeline)
	
	// Prometheus metrics endpoint
	mux.HandleFunc(utils.RouteMetrics, ctrl.GetMetrics)

	// Swagger specification endpoint
	mux.HandleFunc("GET /swagger.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.ServeFile(w, r, "./swagger.json")
	})

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
