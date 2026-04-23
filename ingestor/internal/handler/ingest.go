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

	// Publish to Kafka
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := h.producer.Publish(ctx, &metric); err != nil {
		h.logger.Error("publish failed", "error", err, "metric", metric.Name)
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{
			Error: "failed to publish metric",
			Code:  http.StatusInternalServerError,
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

	// Publish batch to Kafka
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	published, err := h.producer.PublishBatch(ctx, validMetrics)
	if err != nil {
		h.logger.Error("batch publish failed", "error", err)
	}
	middleware.RecordKafkaPublishDuration(time.Since(start))
	middleware.RecordBatchSize(float64(len(validMetrics)))

	c.JSON(http.StatusAccepted, model.IngestResponse{
		Status:   "accepted",
		Accepted: published,
		Message:  "",
	})
}
