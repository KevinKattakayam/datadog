// Package model defines the core data structures for the observability pipeline.
package model

// Metric represents a single metric data point ingested by the pipeline.
type Metric struct {
	Name      string            `json:"name"      binding:"required"`
	Value     float64           `json:"value"     binding:"required"`
	Unit      string            `json:"unit,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp int64             `json:"timestamp" binding:"required"`
	Host      string            `json:"host"      binding:"required"`
}

// BatchRequest represents a batch of metrics submitted in a single request.
type BatchRequest struct {
	Metrics []Metric `json:"metrics" binding:"required,min=1,max=1000"`
}

// IngestResponse is returned after successful metric ingestion.
type IngestResponse struct {
	Status   string `json:"status"`
	Accepted int    `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

// ErrorResponse is returned when a request fails validation or processing.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

// HealthResponse is returned by the health check endpoint.
type HealthResponse struct {
	Status     string            `json:"status"`
	Version    string            `json:"version"`
	Components map[string]string `json:"components"`
}
