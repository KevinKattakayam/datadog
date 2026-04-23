package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/producer"
)

const version = "1.0.0"

// HealthHandler handles health check endpoints.
type HealthHandler struct {
	producer *producer.KafkaProducer
	logger   *slog.Logger
}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler(p *producer.KafkaProducer, logger *slog.Logger) *HealthHandler {
	return &HealthHandler{
		producer: p,
		logger:   logger,
	}
}

// Health handles GET /health — liveness probe.
func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, model.HealthResponse{
		Status:  "healthy",
		Version: version,
		Components: map[string]string{
			"server": "up",
		},
	})
}

// Ready handles GET /ready — readiness probe (checks Kafka).
func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	components := map[string]string{
		"server": "up",
	}

	kafkaHealthy := h.producer.IsHealthy(ctx)
	if kafkaHealthy {
		components["kafka"] = "up"
	} else {
		components["kafka"] = "down"
	}

	if !kafkaHealthy {
		c.JSON(http.StatusServiceUnavailable, model.HealthResponse{
			Status:     "degraded",
			Version:    version,
			Components: components,
		})
		return
	}

	c.JSON(http.StatusOK, model.HealthResponse{
		Status:     "ready",
		Version:    version,
		Components: components,
	})
}
