package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
)

func TestIngestHandler_ValidMetric(t *testing.T) {
	// Create a mock producer
	mp := &mockProducer{}
	h := NewIngestHandler(mp, nil)

	metric := model.Metric{
		Name:      "api.request.duration_ms",
		Value:     142.7,
		Unit:      "ms",
		Tags:      map[string]string{"service": "checkout"},
		Timestamp: time.Now().Unix(),
		Host:      "prod-api-01",
	}

	body, _ := json.Marshal(metric)
	req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.router().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", w.Code)
	}

	var resp model.IngestResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Accepted != 1 {
		t.Errorf("expected accepted=1, got %d", resp.Accepted)
	}
}

func TestIngestHandler_InvalidMetric_MissingName(t *testing.T) {
	mp := &mockProducer{}
	h := NewIngestHandler(mp, nil)

	metric := map[string]interface{}{
		"value":     142.7,
		"unit":      "ms",
		"timestamp": time.Now().Unix(),
		"host":      "prod-api-01",
	}

	body, _ := json.Marshal(metric)
	req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestIngestHandler_BatchEndpoint(t *testing.T) {
	mp := &mockProducer{}
	h := NewIngestHandler(mp, nil)

	batch := model.BatchRequest{
		Metrics: []model.Metric{
			{Name: "cpu.usage", Value: 67.3, Unit: "percent", Tags: map[string]string{"service": "api"}, Timestamp: time.Now().Unix(), Host: "prod-01"},
			{Name: "mem.usage", Value: 2147483648, Unit: "bytes", Tags: map[string]string{"service": "api"}, Timestamp: time.Now().Unix(), Host: "prod-01"},
		},
	}

	body, _ := json.Marshal(batch)
	req := httptest.NewRequest("POST", "/ingest/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.router().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", w.Code)
	}
}

func TestIngestHandler_EmptyBatch(t *testing.T) {
	mp := &mockProducer{}
	h := NewIngestHandler(mp, nil)

	batch := model.BatchRequest{Metrics: []model.Metric{}}

	body, _ := json.Marshal(batch)
	req := httptest.NewRequest("POST", "/ingest/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for empty batch, got %d", w.Code)
	}
}

func TestIngestHandler_BatchTooLarge(t *testing.T) {
	mp := &mockProducer{}
	h := NewIngestHandler(mp, nil)

	metrics := make([]model.Metric, 1001)
	for i := range metrics {
		metrics[i] = model.Metric{
			Name: "test.metric", Value: float64(i), Unit: "count",
			Tags: map[string]string{"service": "test"}, Timestamp: time.Now().Unix(), Host: "test",
		}
	}
	batch := model.BatchRequest{Metrics: metrics}

	body, _ := json.Marshal(batch)
	req := httptest.NewRequest("POST", "/ingest/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for oversized batch, got %d", w.Code)
	}
}

// mockProducer satisfies the producer interface for testing
type mockProducer struct {
	published []model.Metric
}

func (m *mockProducer) Publish(ctx interface{}, metric *model.Metric) error {
	m.published = append(m.published, *metric)
	return nil
}

func (m *mockProducer) PublishBatch(ctx interface{}, metrics []model.Metric) (int, error) {
	for _, met := range metrics {
		m.published = append(m.published, met)
	}
	return len(metrics), nil
}
