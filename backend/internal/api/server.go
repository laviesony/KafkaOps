package api

import (
	"context"
	"net/http"

	"github.com/rs/cors"
	"github.com/yourorg/kafkaops/internal/decode"
	"github.com/yourorg/kafkaops/internal/kafka"
	"github.com/yourorg/kafkaops/internal/store"
)

// ServerDeps contains all dependencies for the API server.
type ServerDeps struct {
	Store    *store.MessageStore
	Consumer *kafka.Consumer
	Producer *kafka.Producer
	Decoder  *decode.Decoder
	Addr     string
}

// Server wraps the HTTP server with KafkaOps-specific configuration.
type Server struct {
	httpServer *http.Server
	deps       ServerDeps
}

// NewServer creates a new API server with all routes configured.
func NewServer(deps ServerDeps) *Server {
	mux := http.NewServeMux()

	// Create handlers
	handlers := NewHandlers(deps.Store, deps.Producer, deps.Decoder)

	// Create bulk handlers (feature-flagged)
	var bulkHandlers *BulkHandlers
	if isProFeatureEnabled() {
		bulkHandlers = NewBulkHandlers(deps.Store, deps.Producer, deps.Decoder)
	}

	// Register routes
	RegisterRoutes(mux, handlers, bulkHandlers)

	// Configure CORS for local frontend development
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:5173"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	}).Handler(mux)

	return &Server{
		httpServer: &http.Server{
			Addr:    deps.Addr,
			Handler: corsHandler,
		},
		deps: deps,
	}
}

// Start begins serving HTTP requests.
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// isProFeatureEnabled checks if PRO features are enabled.
// This is a placeholder - implement license checking as needed.
func isProFeatureEnabled() bool {
	// TODO: Implement proper license/feature flag checking
	// For now, PRO features are disabled by default
	return false
}
