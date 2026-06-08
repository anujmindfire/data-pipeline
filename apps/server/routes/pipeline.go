/*
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
		"./web",
		"../web",
		"../../apps/web",
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

	// Swagger UI HTML Documentation endpoint
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Processing Pipeline - API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css">
    <style>
        body {
            margin: 0;
            padding: 0;
            background-color: #1a1a1a;
        }
        /* Custom dark theme inversion to match visual dashboard */
        .swagger-ui {
            filter: invert(88%) hue-rotate(180deg);
        }
        .swagger-ui .topbar {
            display: none;
        }
        .swagger-ui .info .title {
            color: #ffffff;
        }
        .swagger-ui .info p, .swagger-ui .info li, .swagger-ui .info td {
            color: #cccccc;
        }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js"></script>
    <script>
        window.onload = function() {
            SwaggerUIBundle({
                url: "/swagger.json",
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [
                    SwaggerUIBundle.presets.apis
                ],
                layout: "BaseLayout"
            });
        };
    </script>
</body>
</html>`
		w.Write([]byte(html))
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
