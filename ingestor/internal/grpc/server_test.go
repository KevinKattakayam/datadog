package grpcserver

import (
	"context"
	"testing"
	"time"


	"log/slog"
	"os"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
)

// mockProducer implements KafkaPublisher for testing.
type mockProducer struct {
	published []model.Metric
	failNext  bool
}

func (m *mockProducer) Publish(_ context.Context, metric model.Metric) error {
	if m.failNext {
		return context.DeadlineExceeded
	}
	m.published = append(m.published, metric)
	return nil
}

func newTestServer() (*MetricServer, *mockProducer) {
	mp := &mockProducer{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	server := NewMetricServer("50051", mp, logger)
	return server, mp
}

func TestIngestMetric_Valid(t *testing.T) {
	server, mp := newTestServer()
	ctx := context.Background()

	resp, err := server.IngestMetric(ctx, &GRPCMetric{
		Name:      "cpu.usage",
		Value:     85.5,
		Unit:      "percent",
		Tags:      map[string]string{"service": "api"},
		Timestamp: time.Now().Unix(),
		Host:      "host-01",
		TraceID:   "abc123",
	})

	if err != nil {
		t.Fatalf("IngestMetric returned error: %v", err)
	}
	if resp.Status != "accepted" {
		t.Errorf("expected accepted, got %s: %s", resp.Status, resp.Message)
	}
	if resp.TraceID != "abc123" {
		t.Errorf("expected trace_id abc123, got %s", resp.TraceID)
	}
	if len(mp.published) != 1 {
		t.Errorf("expected 1 published metric, got %d", len(mp.published))
	}
}

func TestIngestMetric_NilPayload(t *testing.T) {
	server, _ := newTestServer()

	resp, _ := server.IngestMetric(context.Background(), nil)
	if resp.Status != "rejected" {
		t.Errorf("expected rejected for nil payload, got %s", resp.Status)
	}
}

func TestIngestMetric_InvalidName(t *testing.T) {
	server, _ := newTestServer()

	resp, _ := server.IngestMetric(context.Background(), &GRPCMetric{
		Name:      "",
		Value:     42.0,
		Timestamp: time.Now().Unix(),
		Host:      "host-01",
	})

	if resp.Status != "rejected" {
		t.Errorf("expected rejected for empty name, got %s", resp.Status)
	}
}

func TestIngestMetricBatch_Valid(t *testing.T) {
	server, mp := newTestServer()
	ctx := context.Background()

	metrics := []*GRPCMetric{
		{Name: "metric.a", Value: 1.0, Timestamp: time.Now().Unix(), Host: "h1"},
		{Name: "metric.b", Value: 2.0, Timestamp: time.Now().Unix(), Host: "h2"},
		{Name: "metric.c", Value: 3.0, Timestamp: time.Now().Unix(), Host: "h3"},
	}

	resp, err := server.IngestMetricBatch(ctx, metrics)
	if err != nil {
		t.Fatalf("IngestMetricBatch returned error: %v", err)
	}
	if resp.Status != "accepted" {
		t.Errorf("expected accepted, got %s", resp.Status)
	}
	if resp.Accepted != 3 {
		t.Errorf("expected 3 accepted, got %d", resp.Accepted)
	}
	if len(mp.published) != 3 {
		t.Errorf("expected 3 published, got %d", len(mp.published))
	}
}

func TestIngestMetricBatch_Empty(t *testing.T) {
	server, _ := newTestServer()

	resp, _ := server.IngestMetricBatch(context.Background(), []*GRPCMetric{})
	if resp.Status != "rejected" {
		t.Errorf("expected rejected for empty batch, got %s", resp.Status)
	}
}

func TestIngestMetricBatch_TooLarge(t *testing.T) {
	server, _ := newTestServer()

	metrics := make([]*GRPCMetric, 1001)
	for i := range metrics {
		metrics[i] = &GRPCMetric{Name: "m", Value: 1.0, Timestamp: time.Now().Unix(), Host: "h"}
	}

	resp, _ := server.IngestMetricBatch(context.Background(), metrics)
	if resp.Status != "rejected" {
		t.Errorf("expected rejected for oversized batch, got %s", resp.Status)
	}
}

func TestIngestMetricBatch_PartialFailure(t *testing.T) {
	server, _ := newTestServer()
	ctx := context.Background()

	metrics := []*GRPCMetric{
		{Name: "valid.metric", Value: 1.0, Timestamp: time.Now().Unix(), Host: "h1"},
		{Name: "", Value: 2.0, Timestamp: time.Now().Unix(), Host: "h2"}, // Invalid: empty name
		{Name: "another.valid", Value: 3.0, Timestamp: time.Now().Unix(), Host: "h3"},
	}

	resp, _ := server.IngestMetricBatch(ctx, metrics)
	if resp.Status != "partial" {
		t.Errorf("expected partial, got %s", resp.Status)
	}
	if resp.Accepted != 2 {
		t.Errorf("expected 2 accepted, got %d", resp.Accepted)
	}
	if resp.Rejected != 1 {
		t.Errorf("expected 1 rejected, got %d", resp.Rejected)
	}
}

func TestStats(t *testing.T) {
	server, _ := newTestServer()
	ctx := context.Background()

	server.IngestMetric(ctx, &GRPCMetric{Name: "m1", Value: 1.0, Timestamp: time.Now().Unix(), Host: "h1"})
	server.IngestMetric(ctx, &GRPCMetric{Name: "", Value: 2.0, Timestamp: time.Now().Unix(), Host: "h2"}) // rejected

	stats := server.Stats()
	if stats["accepted"] != 1 {
		t.Errorf("expected 1 accepted, got %d", stats["accepted"])
	}
	if stats["rejected"] != 1 {
		t.Errorf("expected 1 rejected, got %d", stats["rejected"])
	}
}
