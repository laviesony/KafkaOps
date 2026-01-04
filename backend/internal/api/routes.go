package api

import "net/http"

// RegisterRoutes configures all API routes on the given mux.
func RegisterRoutes(mux *http.ServeMux, handlers *Handlers, bulkHandlers *BulkHandlers) {
	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Messages API
	mux.HandleFunc("/api/messages", handlers.GetMessagesHandler)
	mux.HandleFunc("/api/messages/", func(w http.ResponseWriter, r *http.Request) {
		// Route based on path suffix
		if len(r.URL.Path) > len("/api/messages/") {
			// Check if it's a replay request
			if r.Method == http.MethodPost && (len(r.URL.Path) > 0 && r.URL.Path[len(r.URL.Path)-1] != '/') {
				// POST /api/messages/{id}/replay
				handlers.ReplayMessageHandler(w, r)
				return
			}
			// GET /api/messages/{id}
			handlers.GetMessageHandler(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Bulk operations (PRO feature)
	if bulkHandlers != nil {
		mux.HandleFunc("/api/bulk/preview", bulkHandlers.PreviewHandler)
		mux.HandleFunc("/api/bulk/execute", bulkHandlers.ExecuteHandler)
	}
}
