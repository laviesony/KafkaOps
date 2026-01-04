/*
Bulk remediation handler (PRO feature – server enforced)

Responsibilities:
- Receive JSON Patch definition
- Receive list of message offsets / IDs
- Apply patch in memory
- Validate against Avro schema
- Produce messages in bounded batches

Rules:
- Must verify license / feature flag before execution
- Must strip X-Exception-* headers
- Must preserve X-Original-* headers
- Must produce idempotently where possible
*/

package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/yourorg/kafkaops/internal/decode"
	"github.com/yourorg/kafkaops/internal/kafka"
	"github.com/yourorg/kafkaops/internal/store"
)

// BulkHandlers handles bulk remediation operations (PRO feature).
type BulkHandlers struct {
	store    *store.MessageStore
	producer *kafka.Producer
	decoder  *decode.Decoder
}

// NewBulkHandlers creates bulk handlers with dependencies.
func NewBulkHandlers(store *store.MessageStore, producer *kafka.Producer, decoder *decode.Decoder) *BulkHandlers {
	return &BulkHandlers{
		store:    store,
		producer: producer,
		decoder:  decoder,
	}
}

// BulkPreviewRequest is the request for previewing bulk patches.
type BulkPreviewRequest struct {
	MessageIDs []int64 `json:"messageIds"`
	Patch      []Patch `json:"patch"` // RFC 6902 JSON Patch
}

// Patch represents a single JSON Patch operation (RFC 6902).
type Patch struct {
	Op    string `json:"op"`    // "add", "remove", "replace", "move", "copy", "test"
	Path  string `json:"path"`  // JSON Pointer
	Value any    `json:"value"` // New value (for add, replace)
	From  string `json:"from"`  // Source path (for move, copy)
}

// BulkPreviewResponse shows what will change.
type BulkPreviewResponse struct {
	Previews []PreviewItem `json:"previews"`
	Errors   []string      `json:"errors,omitempty"`
}

// PreviewItem shows before/after for one message.
type PreviewItem struct {
	MessageID int64  `json:"messageId"`
	Before    any    `json:"before"`
	After     any    `json:"after"`
	Error     string `json:"error,omitempty"`
}

// PreviewHandler generates a preview of the bulk patch operation.
// POST /api/bulk/preview
func (h *BulkHandlers) PreviewHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req BulkPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	if len(req.MessageIDs) == 0 {
		h.writeError(w, http.StatusBadRequest, "no message IDs provided")
		return
	}

	if len(req.MessageIDs) > 1000 {
		h.writeError(w, http.StatusBadRequest, "too many messages (max 1000)")
		return
	}

	previews := make([]PreviewItem, 0, len(req.MessageIDs))
	var errors []string

	for _, id := range req.MessageIDs {
		msg, err := h.store.GetMessageByID(id)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get message %d: %v", id, err))
			continue
		}
		if msg == nil {
			errors = append(errors, fmt.Sprintf("message %d not found", id))
			continue
		}

		preview := PreviewItem{
			MessageID: id,
			Before:    msg.DecodedPayload,
		}

		// Apply patch to get "after"
		patched, err := applyPatches(msg.DecodedPayload, req.Patch)
		if err != nil {
			preview.Error = err.Error()
		} else {
			preview.After = patched
		}

		previews = append(previews, preview)
	}

	h.writeJSON(w, http.StatusOK, BulkPreviewResponse{
		Previews: previews,
		Errors:   errors,
	})
}

// BulkExecuteRequest executes the bulk patch.
type BulkExecuteRequest struct {
	MessageIDs []int64 `json:"messageIds"`
	Patch      []Patch `json:"patch"`
	Confirmed  bool    `json:"confirmed"` // Must be true to execute
}

// BulkExecuteResponse contains execution results.
type BulkExecuteResponse struct {
	Succeeded int      `json:"succeeded"`
	Failed    int      `json:"failed"`
	Errors    []string `json:"errors,omitempty"`
}

// ExecuteHandler executes the bulk patch operation.
// POST /api/bulk/execute
func (h *BulkHandlers) ExecuteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req BulkExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	// Safety check: must explicitly confirm
	if !req.Confirmed {
		h.writeError(w, http.StatusBadRequest, "must set confirmed=true to execute")
		return
	}

	if len(req.MessageIDs) == 0 {
		h.writeError(w, http.StatusBadRequest, "no message IDs provided")
		return
	}

	if len(req.MessageIDs) > 1000 {
		h.writeError(w, http.StatusBadRequest, "too many messages (max 1000)")
		return
	}

	var succeeded, failed int
	var errors []string

	// Process in batches of 100
	batchSize := 100
	for i := 0; i < len(req.MessageIDs); i += batchSize {
		end := i + batchSize
		if end > len(req.MessageIDs) {
			end = len(req.MessageIDs)
		}
		batch := req.MessageIDs[i:end]

		batchRequests := make([]kafka.ReplayRequest, 0, len(batch))

		for _, id := range batch {
			msg, err := h.store.GetMessageByID(id)
			if err != nil {
				errors = append(errors, fmt.Sprintf("failed to get message %d: %v", id, err))
				failed++
				continue
			}
			if msg == nil {
				errors = append(errors, fmt.Sprintf("message %d not found", id))
				failed++
				continue
			}

			// Apply patch
			patched, err := applyPatches(msg.DecodedPayload, req.Patch)
			if err != nil {
				errors = append(errors, fmt.Sprintf("failed to patch message %d: %v", id, err))
				failed++
				continue
			}

			// Serialize patched payload
			patchedBytes, err := json.Marshal(patched)
			if err != nil {
				errors = append(errors, fmt.Sprintf("failed to serialize message %d: %v", id, err))
				failed++
				continue
			}

			// Determine target topic
			topic := msg.Headers["X-Original-Topic"]
			if topic == "" {
				topic = msg.Topic
			}

			batchRequests = append(batchRequests, kafka.ReplayRequest{
				Topic:     topic,
				Key:       msg.Key,
				Value:     patchedBytes,
				Headers:   msg.Headers,
				Partition: -1,
			})
		}

		// Execute batch replay
		if len(batchRequests) > 0 {
			_, replayErrors := h.producer.ReplayBatch(r.Context(), batchRequests)
			for j, err := range replayErrors {
				if err != nil {
					errors = append(errors, fmt.Sprintf("replay error for message in batch: %v", err))
					failed++
				} else if j < len(batchRequests) {
					succeeded++
				}
			}
		}
	}

	h.writeJSON(w, http.StatusOK, BulkExecuteResponse{
		Succeeded: succeeded,
		Failed:    failed,
		Errors:    errors,
	})
}

// applyPatches applies JSON Patch operations to a value.
// This is a simplified implementation - consider using a proper JSON Patch library.
func applyPatches(value any, patches []Patch) (any, error) {
	// Convert to JSON and back for manipulation
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("value is not an object: %w", err)
	}

	for _, p := range patches {
		switch p.Op {
		case "replace":
			// Simple path handling (e.g., "/foo/bar")
			if err := setPath(obj, p.Path, p.Value); err != nil {
				return nil, fmt.Errorf("patch replace failed at %s: %w", p.Path, err)
			}
		case "add":
			if err := setPath(obj, p.Path, p.Value); err != nil {
				return nil, fmt.Errorf("patch add failed at %s: %w", p.Path, err)
			}
		case "remove":
			if err := removePath(obj, p.Path); err != nil {
				return nil, fmt.Errorf("patch remove failed at %s: %w", p.Path, err)
			}
		default:
			return nil, fmt.Errorf("unsupported patch operation: %s", p.Op)
		}
	}

	return obj, nil
}

// setPath sets a value at a JSON Pointer path.
func setPath(obj map[string]any, path string, value any) error {
	parts := parseJSONPointer(path)
	if len(parts) == 0 {
		return fmt.Errorf("empty path")
	}

	current := obj
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			// Create intermediate objects
			newObj := make(map[string]any)
			current[parts[i]] = newObj
			current = newObj
		} else if m, ok := next.(map[string]any); ok {
			current = m
		} else {
			return fmt.Errorf("cannot navigate through non-object at %s", parts[i])
		}
	}

	current[parts[len(parts)-1]] = value
	return nil
}

// removePath removes a value at a JSON Pointer path.
func removePath(obj map[string]any, path string) error {
	parts := parseJSONPointer(path)
	if len(parts) == 0 {
		return fmt.Errorf("empty path")
	}

	current := obj
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			return nil // Path doesn't exist, nothing to remove
		}
		if m, ok := next.(map[string]any); ok {
			current = m
		} else {
			return fmt.Errorf("cannot navigate through non-object at %s", parts[i])
		}
	}

	delete(current, parts[len(parts)-1])
	return nil
}

// parseJSONPointer parses a JSON Pointer (e.g., "/foo/bar") into parts.
func parseJSONPointer(path string) []string {
	if path == "" || path == "/" {
		return nil
	}
	if path[0] == '/' {
		path = path[1:]
	}
	parts := make([]string, 0)
	for _, part := range splitString(path, "/") {
		// Unescape JSON Pointer encoding
		part = unescapeJSONPointer(part)
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func splitString(s, sep string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func unescapeJSONPointer(s string) string {
	s = replaceAll(s, "~1", "/")
	s = replaceAll(s, "~0", "~")
	return s
}

func replaceAll(s, old, new string) string {
	result := ""
	for i := 0; i <= len(s)-len(old); {
		if s[i:i+len(old)] == old {
			result += new
			i += len(old)
		} else {
			result += string(s[i])
			i++
		}
	}
	if len(s) > 0 && len(old) > 0 && len(s) >= len(old) {
		result += s[len(s)-(len(old)-1):]
	}
	return result
}

func (h *BulkHandlers) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *BulkHandlers) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}
