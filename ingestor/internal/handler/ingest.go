// Package handler implements HTTP request handlers for the ingestor API.
package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/middleware"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/producer"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/validator"
)

// IngestHandler handles metric ingestion requests.
type IngestHandler struct {
	producer *producer.KafkaProducer
	logger   *slog.Logger
}

// NewIngestHandler creates a new IngestHandler.
func NewIngestHandler(p *producer.KafkaProducer, logger *slog.Logger) *IngestHandler {
	return &IngestHandler{
		producer: p,
		logger:   logger,
	}
}

// IngestSingle handles POST /ingest — single metric ingestion.
func (h *IngestHandler) IngestSingle(c *gin.Context) {
	var metric model.Metric
	if err := c.ShouldBindJSON(&metric); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "invalid request body",
			Code:    http.StatusBadRequest,
			Details: err.Error(),
		})
		return
	}

	// Validate metric
	if err := validator.ValidateMetric(&metric); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "validation failed",
			Code:    http.StatusBadRequest,
			Details: err.Error(),
		})
		return
	}

	tenantID := middleware.GetTenantID(c)

	// Publish to Kafka — synchronous, returns error if not acked
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := h.producer.Publish(ctx, tenantID, &metric); err != nil {
		h.logger.Error("publish failed", "error", err, "metric", metric.Name, "tenant", tenantID)
		c.Header("Retry-After", "5")
		c.JSON(http.StatusServiceUnavailable, model.ErrorResponse{
			Error: "upstream unavailable; retry with backoff",
			Code:  http.StatusServiceUnavailable,
		})
		return
	}
	middleware.RecordKafkaPublishDuration(time.Since(start))
	middleware.RecordBatchSize(1)

	c.JSON(http.StatusAccepted, model.IngestResponse{
		Status:   "accepted",
		Accepted: 1,
	})
}

// IngestBatch handles POST /ingest/batch — batch metric ingestion.
func (h *IngestHandler) IngestBatch(c *gin.Context) {
	var batch model.BatchRequest
	if err := c.ShouldBindJSON(&batch); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "invalid batch request body",
			Code:    http.StatusBadRequest,
			Details: err.Error(),
		})
		return
	}

	// Validate all metrics
	validMetrics := make([]model.Metric, 0, len(batch.Metrics))
	for i, m := range batch.Metrics {
		if err := validator.ValidateMetric(&m); err != nil {
			h.logger.Warn("skipping invalid metric in batch",
				"index", i,
				"error", err,
				"metric_name", m.Name,
			)
			continue
		}
		validMetrics = append(validMetrics, m)
	}

	if len(validMetrics) == 0 {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error: "no valid metrics in batch",
			Code:  http.StatusBadRequest,
		})
		return
	}

	tenantID := middleware.GetTenantID(c)

	// Publish batch to Kafka — synchronous
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	acked, err := h.producer.PublishBatch(ctx, tenantID, validMetrics)
	middleware.RecordKafkaPublishDuration(time.Since(start))
	middleware.RecordBatchSize(float64(len(validMetrics)))

	skipped := len(batch.Metrics) - len(validMetrics)

	switch {
	case err != nil && acked == 0:
		// Total failure — nothing reached Kafka
		h.logger.Error("batch publish failed", "error", err, "tenant", tenantID)
		c.Header("Retry-After", "5")
		c.JSON(http.StatusServiceUnavailable, model.ErrorResponse{
			Error: "upstream unavailable; retry with backoff",
			Code:  http.StatusServiceUnavailable,
		})
	case err != nil:
		// Partial failure — some records acked, some not
		c.JSON(http.StatusMultiStatus, model.IngestResponse{
			Status:   "partial",
			Accepted: acked,
			Rejected: len(validMetrics) - acked,
			Skipped:  skipped,
			Message:  "resend the rejected records",
		})
	default:
		// Full success
		c.JSON(http.StatusAccepted, model.IngestResponse{
			Status:   "accepted",
			Accepted: acked,
			Skipped:  skipped,
		})
	}
}
