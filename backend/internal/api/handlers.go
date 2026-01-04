package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/yourorg/kafkaops/internal/decode"
	"github.com/yourorg/kafkaops/internal/kafka"
	"github.com/yourorg/kafkaops/internal/store"
)

// Handlers holds all HTTP handler dependencies.
type Handlers struct {
	store    *store.MessageStore
	producer *kafka.Producer
	decoder  *decode.Decoder
}

// NewHandlers creates handlers with injected dependencies.
func NewHandlers(store *store.MessageStore, producer *kafka.Producer, decoder *decode.Decoder) *Handlers {
	return &Handlers{
		store:    store,
		producer: producer,
		decoder:  decoder,
	}
}

// APIResponse is a standard response wrapper.
type APIResponse struct {
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
	Total   int    `json:"total,omitempty"`
	Page    int    `json:"page,omitempty"`
	PerPage int    `json:"perPage,omitempty"`
}

// GetMessagesHandler returns paginated messages.
// GET /api/messages?page=1&limit=50&topic=xxx
func (h *Handlers) GetMessagesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Parse query params
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}
	topic := r.URL.Query().Get("topic")

	// Query messages
	messages, total, err := h.store.QueryMessages(store.QueryMessagesParams{
		Topic:  topic,
		Limit:  limit,
		Offset: (page - 1) * limit,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, APIResponse{
		Data:    messages,
		Total:   total,
		Page:    page,
		PerPage: limit,
	})
}

// GetMessageHandler returns a single message by ID.
// GET /api/messages/{id}
func (h *Handlers) GetMessageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract ID from path
	id, err := h.extractIDFromPath(r.URL.Path, "/api/messages/")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid message ID")
		return
	}

	msg, err := h.store.GetMessageByID(id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if msg == nil {
		h.writeError(w, http.StatusNotFound, "message not found")
		return
	}

	h.writeJSON(w, http.StatusOK, APIResponse{Data: msg})
}

// ReplayRequest is the request body for replaying a message.
type ReplayRequest struct {
	Payload json.RawMessage `json:"payload"`
	Topic   string          `json:"topic,omitempty"` // Optional override
}

// ReplayResponse contains the result of a replay operation.
type ReplayResponse struct {
	Success   bool   `json:"success"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// ReplayMessageHandler replays a single message.
// POST /api/messages/{id}/replay
func (h *Handlers) ReplayMessageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract ID from path (path: /api/messages/{id}/replay)
	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/messages/"), "/")
	if len(pathParts) < 1 {
		h.writeError(w, http.StatusBadRequest, "invalid path")
		return
	}
	id, err := strconv.ParseInt(pathParts[0], 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid message ID")
		return
	}

	// Get original message
	msg, err := h.store.GetMessageByID(id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if msg == nil {
		h.writeError(w, http.StatusNotFound, "message not found")
		return
	}

	// Parse request body
	var req ReplayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Use provided payload or original
	payload := msg.Value
	if len(req.Payload) > 0 {
		payload = req.Payload
	}

	// Determine target topic
	topic := req.Topic
	if topic == "" {
		topic = msg.Headers["X-Original-Topic"]
	}
	if topic == "" {
		topic = msg.Topic // Fallback to DLQ topic (unusual but valid)
	}

	// Replay the message
	result, err := h.producer.Replay(r.Context(), kafka.ReplayRequest{
		Topic:     topic,
		Key:       msg.Key,
		Value:     payload,
		Headers:   msg.Headers,
		Partition: -1, // Let Kafka choose partition
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "replay failed: "+err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, APIResponse{
		Data: ReplayResponse{
			Success:   true,
			Topic:     result.Topic,
			Partition: result.Partition,
			Offset:    result.Offset,
		},
	})
}

// extractIDFromPath extracts a numeric ID from a path.
func (h *Handlers) extractIDFromPath(path, prefix string) (int64, error) {
	idStr := strings.TrimPrefix(path, prefix)
	// Remove trailing path components
	if idx := strings.Index(idStr, "/"); idx > 0 {
		idStr = idStr[:idx]
	}
	return strconv.ParseInt(idStr, 10, 64)
}

func (h *Handlers) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *Handlers) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, APIResponse{Error: message})
}
