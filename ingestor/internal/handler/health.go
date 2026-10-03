package handler

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/KevinKattakayam/datadog/ingestor/internal/model"
	"github.com/KevinKattakayam/datadog/ingestor/internal/producer"
)

const version = "2.0.0"

// HealthHandler handles health check endpoints.
type HealthHandler struct {
	producer *producer.KafkaProducer
	logger   *slog.Logger
	draining atomic.Bool
}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler(p *producer.KafkaProducer, logger *slog.Logger) *HealthHandler {
	return &HealthHandler{
		producer: p,
		logger:   logger,
	}
}

// SetDraining marks the handler as draining — readiness will fail so
// the endpoints controller pulls this pod out of the Service before
// it stops accepting.
func (h *HealthHandler) SetDraining(draining bool) {
	h.draining.Store(draining)
	h.logger.Info("draining state changed", "draining", draining)
}

// Health handles GET /health — liveness probe.
// Always returns 200 if the process is running.
func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, model.HealthResponse{
		Status:  "healthy",
		Version: version,
		Components: map[string]string{
			"server": "up",
		},
	})
}

// Ready handles GET /ready — readiness probe (checks Kafka + drain state).
func (h *HealthHandler) Ready(c *gin.Context) {
	// Draining: fail readiness so the endpoints controller removes us
	if h.draining.Load() {
		c.JSON(http.StatusServiceUnavailable, model.HealthResponse{
			Status:  "draining",
			Version: version,
			Components: map[string]string{
				"server": "draining",
			},
		})
		return
	}

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
