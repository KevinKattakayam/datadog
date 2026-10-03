// Package handler implements HTTP request handlers for the ingestor API.
package handler

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/KevinKattakayam/datadog/ingestor/internal/middleware"
	"github.com/KevinKattakayam/datadog/ingestor/internal/model"
	"github.com/KevinKattakayam/datadog/ingestor/internal/validator"
)

// Publisher is the durable write the handler depends on. The production
// implementation is producer.KafkaProducer; tests substitute a fake so the
// real status-code logic is exercised without a broker.
type Publisher interface {
	// Publish returns nil only once every in-sync replica acknowledged.
	Publish(ctx context.Context, tenantID string, m *model.Metric) error
	// PublishBatch returns one result per metric, in order (nil = acked).
	// The error is non-nil only if nothing could be attempted.
	PublishBatch(ctx context.Context, tenantID string, metrics []model.Metric) ([]error, error)
}

// Quota charges a tenant per metric. middleware.TenantRateLimiter satisfies it.
type Quota interface {
	AllowN(tenantID string, n int) (bool, time.Duration)
}

// IngestHandler handles metric ingestion requests.
type IngestHandler struct {
	producer Publisher
	quota    Quota // nil disables the per-metric quota
	logger   *slog.Logger
}

// NewIngestHandler creates a new IngestHandler. quota may be nil.
func NewIngestHandler(p Publisher, quota Quota, logger *slog.Logger) *IngestHandler {
	return &IngestHandler{producer: p, quota: quota, logger: logger}
}

// IngestSingle handles POST /ingest — single metric ingestion.
func (h *IngestHandler) IngestSingle(c *gin.Context) {
	var metric model.Metric
	if err := c.ShouldBindJSON(&metric); err != nil {
		h.bindError(c, err)
		return
	}

	if err := validator.ValidateMetric(&metric); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "validation failed",
			Code:    http.StatusBadRequest,
			Details: err.Error(),
		})
		return
	}

	tenantID := middleware.GetTenantID(c)
	if !h.admit(c, tenantID, 1) {
		return
	}

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

	c.JSON(http.StatusAccepted, model.IngestResponse{Status: "accepted", Accepted: 1})
}

// IngestBatch handles POST /ingest/batch.
//
// Status codes:
//   - 202 every metric acknowledged by Kafka.
//   - 207 some accepted; `errors` lists the rest by request index, with
//     retryable=true for Kafka failures (resend) and false for validation
//     failures (fix first).
//   - 400 nothing in the batch was valid.
//   - 429 the tenant's metrics-per-second quota cannot admit this batch.
//   - 503 valid metrics, but Kafka acknowledged none of them.
func (h *IngestHandler) IngestBatch(c *gin.Context) {
	var batch model.BatchRequest
	if err := c.ShouldBindJSON(&batch); err != nil {
		h.bindError(c, err)
		return
	}

	var itemErrs []model.ItemError
	valid := make([]model.Metric, 0, len(batch.Metrics))
	validIndex := make([]int, 0, len(batch.Metrics)) // valid[i] came from request index validIndex[i]
	for i := range batch.Metrics {
		m := batch.Metrics[i]
		if err := validator.ValidateMetric(&m); err != nil {
			itemErrs = append(itemErrs, model.ItemError{Index: i, Error: err.Error(), Retryable: false})
			continue
		}
		valid = append(valid, m)
		validIndex = append(validIndex, i)
	}
	skipped := len(batch.Metrics) - len(valid)

	if len(valid) == 0 {
		c.JSON(http.StatusBadRequest, model.IngestResponse{
			Status:  "rejected",
			Skipped: skipped,
			Message: "no valid metrics in batch",
			Errors:  itemErrs,
		})
		return
	}

	tenantID := middleware.GetTenantID(c)
	if !h.admit(c, tenantID, len(valid)) {
		return
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	results, err := h.producer.PublishBatch(ctx, tenantID, valid)
	middleware.RecordKafkaPublishDuration(time.Since(start))
	middleware.RecordBatchSize(float64(len(valid)))
	if err != nil {
		h.logger.Error("batch could not be attempted", "error", err, "tenant", tenantID)
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{
			Error: "internal error encoding batch",
			Code:  http.StatusInternalServerError,
		})
		return
	}

	accepted, rejected := 0, 0
	for i, perr := range results {
		if perr == nil {
			accepted++
			continue
		}
		rejected++
		itemErrs = append(itemErrs, model.ItemError{
			Index:     validIndex[i],
			Error:     fmt.Sprintf("not acknowledged by kafka: %v", perr),
			Retryable: true,
		})
	}

	resp := model.IngestResponse{
		Accepted: accepted,
		Rejected: rejected,
		Skipped:  skipped,
		Errors:   sortByIndex(itemErrs),
	}

	switch {
	case accepted == 0:
		h.logger.Error("batch publish failed", "tenant", tenantID, "rejected", rejected)
		resp.Status = "unavailable"
		resp.Message = "upstream unavailable; retry the retryable items with backoff"
		c.Header("Retry-After", "5")
		c.JSON(http.StatusServiceUnavailable, resp)
	case rejected > 0 || skipped > 0:
		resp.Status = "partial"
		resp.Message = "resend items marked retryable; fix the others"
		c.JSON(http.StatusMultiStatus, resp)
	default:
		resp.Status = "accepted"
		c.JSON(http.StatusAccepted, resp)
	}
}

// admit charges n metrics to the tenant's quota, writing a 429 on refusal.
func (h *IngestHandler) admit(c *gin.Context, tenantID string, n int) bool {
	if h.quota == nil {
		return true
	}
	ok, wait := h.quota.AllowN(tenantID, n)
	if ok {
		return true
	}
	middleware.RecordRateLimited("metrics")
	msg := "tenant metrics-per-second quota exceeded"
	if wait < 0 {
		msg = "batch larger than the tenant's burst allowance; split it"
	} else {
		middleware.SetRetryAfter(c, wait)
	}
	c.JSON(http.StatusTooManyRequests, model.ErrorResponse{
		Error: msg,
		Code:  http.StatusTooManyRequests,
	})
	return false
}

func (h *IngestHandler) bindError(c *gin.Context, err error) {
	if middleware.IsBodyTooLarge(err) {
		c.JSON(http.StatusRequestEntityTooLarge, model.ErrorResponse{
			Error: "request body too large",
			Code:  http.StatusRequestEntityTooLarge,
		})
		return
	}
	c.JSON(http.StatusBadRequest, model.ErrorResponse{
		Error:   "invalid request body",
		Code:    http.StatusBadRequest,
		Details: err.Error(),
	})
}

// sortByIndex orders item errors by request index. Validation errors are
// collected first and Kafka errors second, so the merged list needs sorting
// for a predictable response. Insertion sort: lists are at most 1000 long and
// already two sorted runs.
func sortByIndex(errs []model.ItemError) []model.ItemError {
	for i := 1; i < len(errs); i++ {
		for j := i; j > 0 && errs[j].Index < errs[j-1].Index; j-- {
			errs[j], errs[j-1] = errs[j-1], errs[j]
		}
	}
	return errs
}
