package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Registry is a read-only client for Confluent Schema Registry.
type Registry struct {
	baseURL    string
	httpClient *http.Client
	cache      *Cache
}

// schemaResponse represents the Schema Registry API response.
type schemaResponse struct {
	Schema string `json:"schema"`
}

// NewRegistry creates a new Schema Registry client.
func NewRegistry(baseURL string) *Registry {
	return &Registry{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		cache: NewCache(),
	}
}

// FetchSchemaByID retrieves a schema by its ID.
// Results are cached to minimize network calls.
func (r *Registry) FetchSchemaByID(id uint32) (string, error) {
	// Check cache first
	if cached, ok := r.cache.Get(id); ok {
		return cached, nil
	}

	// Schema Registry disabled
	if r.baseURL == "" {
		return "", fmt.Errorf("schema registry not configured")
	}

	// Fetch from registry
	url := fmt.Sprintf("%s/schemas/ids/%d", r.baseURL, id)

	resp, err := r.httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to fetch schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("schema not found: id=%d", id)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("schema registry error (status=%d): %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	var schemaResp schemaResponse
	if err := json.Unmarshal(body, &schemaResp); err != nil {
		return "", fmt.Errorf("failed to parse schema response: %w", err)
	}

	// Cache the schema
	r.cache.Put(id, schemaResp.Schema)

	return schemaResp.Schema, nil
}

// GetSchemaByID is an alias for FetchSchemaByID for interface compatibility.
func (r *Registry) GetSchemaByID(id uint32) (string, error) {
	return r.FetchSchemaByID(id)
}

// IsConfigured returns true if Schema Registry is configured.
func (r *Registry) IsConfigured() bool {
	return r.baseURL != ""
}
