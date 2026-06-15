/*
RegisterRoutes attaches controller functions to the ServeMux for routing, configures static file directories
for the dashboard SPA frontend application, and applies the CORS/Content-Type middleware handler.
*/
package routes

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"data-processing-pipeline/apps/server/controller"
	"data-processing-pipeline/packages/shared/utils"
)

// getStaticDir retrieves the path to the frontend assets from the environment variable.
func getStaticDir() string {
	dir := os.Getenv("STATIC_DIR")
	if dir == "" {
		dir = "./apps/web"
	}
	// Verify if the static directory exists on the filesystem
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		fmt.Printf("[Warning] Static directory %q does not exist or is not a directory. SPA frontend might not be served correctly.\n", dir)
	}
	return dir
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
	mux.Handle(utils.RouteCreatePipeline, authMiddleware(http.HandlerFunc(ctrl.CreatePipeline)))
	mux.HandleFunc(utils.RouteListPipelines, ctrl.ListPipelines)
	mux.HandleFunc(utils.RouteGetPipeline, ctrl.GetPipeline)
	mux.HandleFunc(utils.RouteGetProgress, ctrl.GetProgress)
	mux.HandleFunc(utils.RouteGetResults, ctrl.GetResults)
	mux.HandleFunc(utils.RouteGetErrors, ctrl.GetErrors)
	mux.Handle(utils.RouteCancelPipeline, authMiddleware(http.HandlerFunc(ctrl.CancelPipeline)))
	mux.Handle(utils.RouteDeletePipeline, authMiddleware(http.HandlerFunc(ctrl.DeletePipeline)))
	
	// Prometheus metrics endpoint
	mux.HandleFunc(utils.RouteMetrics, ctrl.GetMetrics)

	// Swagger specification endpoint
	mux.HandleFunc("GET /swagger.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		http.ServeFile(w, r, "./swagger.json")
	})

	// Swagger UI HTML Documentation endpoint
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
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
                url: "/swagger.json?t=" + new Date().getTime(),
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

	return rateLimitMiddleware(corsMiddleware(mux))
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Content-Type", "application/json")
		}

		next.ServeHTTP(w, r)
	})
}

type ipLimiter struct {
	tokens    float64
	lastCheck time.Time
}

var (
	limiters   = make(map[string]*ipLimiter)
	limitMutex sync.Mutex
)

func rateLimitMiddleware(next http.Handler) http.Handler {
	const (
		maxTokens  = 10.0
		refillRate = 2.0
	)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := r.RemoteAddr
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			ip = strings.Split(xff, ",")[0]
		} else if xri := r.Header.Get("X-Real-IP"); xri != "" {
			ip = xri
		}
		if idx := strings.LastIndex(ip, ":"); idx != -1 {
			ip = ip[:idx]
		}

		limitMutex.Lock()
		lim, exists := limiters[ip]
		now := time.Now()
		if !exists {
			lim = &ipLimiter{
				tokens:    maxTokens,
				lastCheck: now,
			}
			limiters[ip] = lim
		} else {
			elapsed := now.Sub(lim.lastCheck).Seconds()
			lim.tokens += elapsed * refillRate
			if lim.tokens > maxTokens {
				lim.tokens = maxTokens
			}
			lim.lastCheck = now
		}

		if lim.tokens < 1.0 {
			limitMutex.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"Rate limit exceeded. Please try again later."}`))
			return
		}

		lim.tokens -= 1.0
		limitMutex.Unlock()

		next.ServeHTTP(w, r)
	})
}

func ValidateJWT(tokenStr string, secret []byte) bool {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return false
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expectedSignature := mac.Sum(nil)

	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}

	if !hmac.Equal(signature, expectedSignature) {
		return false
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}

	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return false
	}

	if claims.Exp != 0 && time.Now().Unix() > claims.Exp {
		return false
	}

	return true
}

func authMiddleware(next http.Handler) http.Handler {
	apiKeyEnv := os.Getenv("API_KEY")
	if apiKeyEnv == "" {
		apiKeyEnv = "dev-api-key-99"
	}
	jwtSecretEnv := os.Getenv("JWT_SECRET")
	if jwtSecretEnv == "" {
		jwtSecretEnv = "dev-jwt-secret-99"
	}
	jwtSecret := []byte(jwtSecretEnv)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqKey := r.Header.Get("X-API-Key")
		if reqKey != "" && reqKey == apiKeyEnv {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			if ValidateJWT(tokenStr, jwtSecret) {
				next.ServeHTTP(w, r)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"Unauthorized. Missing or invalid X-API-Key or JWT Authorization header."}`))
	})
}
