// Package model defines the core data structures for the observability pipeline.
package model

import (
	"encoding/json"
	"fmt"
)

// Metric represents a single metric data point ingested by the pipeline.
type Metric struct {
	Name      string            `json:"name"      binding:"required"`
	Value     float64           `json:"value"`
	Unit      string            `json:"unit,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp int64             `json:"timestamp" binding:"required"`
	Host      string            `json:"host"      binding:"required"`
	TenantID  string            `json:"tenant_id,omitempty"`
}

// UnmarshalJSON keeps an explicitly supplied zero distinct from an omitted
// value. Gin's `required` rule considers float64(0) empty, so the rule cannot
// model this API contract on a scalar field.
func (m *Metric) UnmarshalJSON(data []byte) error {
	type metricJSON struct {
		Name      string            `json:"name"`
		Value     *float64          `json:"value"`
		Unit      string            `json:"unit"`
		Tags      map[string]string `json:"tags"`
		Timestamp int64             `json:"timestamp"`
		Host      string            `json:"host"`
		TenantID  string            `json:"tenant_id"`
	}

	var decoded metricJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Value == nil {
		return fmt.Errorf("value is required")
	}

	*m = Metric{
		Name:      decoded.Name,
		Value:     *decoded.Value,
		Unit:      decoded.Unit,
		Tags:      decoded.Tags,
		Timestamp: decoded.Timestamp,
		Host:      decoded.Host,
		TenantID:  decoded.TenantID,
	}
	return nil
}

// BatchRequest represents a batch of metrics submitted in a single request.
type BatchRequest struct {
	Metrics []Metric `json:"metrics" binding:"required,min=1,max=1000"`
}

// IngestResponse is returned after successful metric ingestion.
type IngestResponse struct {
	Status   string `json:"status"`
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected,omitempty"`
	Skipped  int    `json:"skipped,omitempty"`
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
