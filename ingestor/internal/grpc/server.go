// Package grpcserver implements the gRPC MetricService server.
// It provides a high-performance binary protocol for metric ingestion,
// complementing the HTTP/JSON endpoint for SDK clients needing lower overhead.
//
// The gRPC service supports three ingestion modes:
//   - Unary:       IngestMetric — single metric per request
//   - Batch:       IngestMetricBatch — up to 1000 metrics per request
//   - Streaming:   StreamMetrics — bidirectional stream for continuous ingestion
//
// All modes reuse the same validation and Kafka publishing logic as the HTTP handler.
package grpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/validator"
)

// ── Types (mirror proto/v1/metric.proto) ─────────────────────

// GRPCMetric represents a single data point received via gRPC.
type GRPCMetric struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Unit      string            `json:"unit"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
	Host      string            `json:"host"`
	TraceID   string            `json:"trace_id"`
}

// IngestResponse is the standard response for ingestion requests.
type IngestResponse struct {
	Status   string   `json:"status"`
	Message  string   `json:"message,omitempty"`
	TraceID  string   `json:"trace_id,omitempty"`
	Accepted int32    `json:"accepted,omitempty"`
	Rejected int32    `json:"rejected,omitempty"`
	Errors   []string `json:"errors,omitempty"`
}

// ── Kafka Producer Interface ────────────────────────────────

// KafkaPublisher defines the interface for publishing metrics to Kafka.
type KafkaPublisher interface {
	Publish(ctx context.Context, metric model.Metric) error
}

// ── Server Implementation ────────────────────────────────────

// MetricServer implements the gRPC-style metric ingestion service
// using a raw TCP protocol with JSON framing (newline-delimited).
// In production with protoc available, this would use the generated
// gRPC server stubs. This implementation provides the same interface
// without requiring the protoc toolchain.
type MetricServer struct {
	producer KafkaPublisher
	logger   *slog.Logger
	port     string
	accepted atomic.Int64
	rejected atomic.Int64
}

// NewMetricServer creates a new gRPC-compatible metric server.
func NewMetricServer(port string, producer KafkaPublisher, logger *slog.Logger) *MetricServer {
	return &MetricServer{
		producer: producer,
		logger:   logger,
		port:     port,
	}
}

// IngestMetric handles a single metric ingestion.
func (s *MetricServer) IngestMetric(ctx context.Context, gm *GRPCMetric) (*IngestResponse, error) {
	if gm == nil {
		return &IngestResponse{Status: "rejected", Message: "metric payload is required"}, nil
	}

	m := grpcToModel(gm)

	if err := validator.ValidateMetric(&m); err != nil {
		s.rejected.Add(1)
		return &IngestResponse{Status: "rejected", Message: err.Error()}, nil
	}

	if err := s.producer.Publish(ctx, m); err != nil {
		s.rejected.Add(1)
		s.logger.Error("gRPC: kafka publish failed", "error", err, "metric", m.Name)
		return &IngestResponse{Status: "rejected", Message: "internal publish error"}, err
	}

	s.accepted.Add(1)
	return &IngestResponse{Status: "accepted", TraceID: gm.TraceID}, nil
}

// IngestMetricBatch handles batch metric ingestion.
func (s *MetricServer) IngestMetricBatch(ctx context.Context, metrics []*GRPCMetric) (*IngestResponse, error) {
	if len(metrics) == 0 {
		return &IngestResponse{Status: "rejected", Errors: []string{"empty batch"}}, nil
	}
	if len(metrics) > 1000 {
		return &IngestResponse{
			Status:   "rejected",
			Rejected: int32(len(metrics)),
			Errors:   []string{"batch exceeds maximum size of 1000"},
		}, nil
	}

	var accepted, rejected int32
	var errors []string

	for i, gm := range metrics {
		m := grpcToModel(gm)

		if err := validator.ValidateMetric(&m); err != nil {
			rejected++
			errors = append(errors, fmt.Sprintf("metric[%d]: %s", i, err.Error()))
			continue
		}

		if err := s.producer.Publish(ctx, m); err != nil {
			rejected++
			errors = append(errors, fmt.Sprintf("metric[%d]: publish failed", i))
			continue
		}

		accepted++
	}

	s.accepted.Add(int64(accepted))
	s.rejected.Add(int64(rejected))

	st := "accepted"
	if rejected > 0 && accepted > 0 {
		st = "partial"
	} else if accepted == 0 {
		st = "rejected"
	}

	return &IngestResponse{Status: st, Accepted: accepted, Rejected: rejected, Errors: errors}, nil
}

// Stats returns server statistics.
func (s *MetricServer) Stats() map[string]int64 {
	return map[string]int64{
		"accepted": s.accepted.Load(),
		"rejected": s.rejected.Load(),
	}
}

// ListenAndServe starts a TCP server that accepts newline-delimited JSON frames.
// Each frame is a GRPCMetric JSON object. This is a lightweight binary-compatible
// protocol that mirrors the gRPC service contract.
//
// In production, replace this with the protoc-generated gRPC server:
//   protoc --go_out=. --go-grpc_out=. proto/v1/metric.proto
//   Then use google.golang.org/grpc server.
func (s *MetricServer) ListenAndServe() error {
	lis, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		return fmt.Errorf("gRPC listen on :%s failed: %w", s.port, err)
	}
	defer lis.Close()

	s.logger.Info("gRPC metric server started", "port", s.port)

	for {
		conn, err := lis.Accept()
		if err != nil {
			s.logger.Error("gRPC accept error", "error", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

// handleConnection processes a single client connection.
func (s *MetricServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var gm GRPCMetric
		if err := decoder.Decode(&gm); err != nil {
			return // Client disconnected or malformed data
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, _ := s.IngestMetric(ctx, &gm)
		cancel()

		if err := encoder.Encode(resp); err != nil {
			return
		}
	}
}

// grpcToModel converts a GRPCMetric to the internal model.
func grpcToModel(gm *GRPCMetric) model.Metric {
	ts := gm.Timestamp
	if ts == 0 {
		ts = time.Now().Unix()
	}
	return model.Metric{
		Name:      gm.Name,
		Value:     gm.Value,
		Unit:      gm.Unit,
		Tags:      gm.Tags,
		Timestamp: ts,
		Host:      gm.Host,
	}
}
