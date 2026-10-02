package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/validator"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// TestValidateMetricDirect tests the validation logic directly (no Kafka needed).
func TestValidateMetricDirect_Valid(t *testing.T) {
	m := &model.Metric{
		Name:      "api.request.duration_ms",
		Value:     142.7,
		Unit:      "ms",
		Tags:      map[string]string{"service": "checkout"},
		Timestamp: time.Now().Unix(),
		Host:      "prod-api-01",
	}
	if err := validator.ValidateMetric(m); err != nil {
		t.Errorf("expected valid metric, got error: %v", err)
	}
}

func TestValidateMetricDirect_MissingName(t *testing.T) {
	m := &model.Metric{
		Value:     142.7,
		Timestamp: time.Now().Unix(),
		Host:      "prod-api-01",
	}
	if err := validator.ValidateMetric(m); err == nil {
		t.Error("expected error for missing name")
	}
}

func TestValidateMetricDirect_MissingHost(t *testing.T) {
	m := &model.Metric{
		Name:      "test.metric",
		Value:     42.0,
		Timestamp: time.Now().Unix(),
	}
	if err := validator.ValidateMetric(m); err == nil {
		t.Error("expected error for missing host")
	}
}

// TestIngestSingle_InvalidJSON tests that invalid JSON is rejected with 400.
func TestIngestSingle_InvalidJSON(t *testing.T) {
	router := gin.New()
	// Use a simple handler that just parses JSON and validates — no Kafka needed
	router.POST("/ingest", func(c *gin.Context) {
		var metric model.Metric
		if err := c.ShouldBindJSON(&metric); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{
				Error: "invalid request body",
				Code:  http.StatusBadRequest,
			})
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
		c.JSON(http.StatusAccepted, model.IngestResponse{Status: "accepted", Accepted: 1})
	})

	req := httptest.NewRequest("POST", "/ingest", bytes.NewReader([]byte(`{bad json}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// TestIngestSingle_MissingName tests validation rejects missing name.
func TestIngestSingle_MissingName(t *testing.T) {
	router := gin.New()
	router.POST("/ingest", func(c *gin.Context) {
		var metric model.Metric
		if err := c.ShouldBindJSON(&metric); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "invalid", Code: 400})
			return
		}
		if err := validator.ValidateMetric(&metric); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "validation", Code: 400, Details: err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, model.IngestResponse{Status: "accepted", Accepted: 1})
	})

	body, _ := json.Marshal(map[string]interface{}{
		"value": 142.7, "unit": "ms", "timestamp": time.Now().Unix(), "host": "test",
	})
	req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// TestIngestSingle_ValidMetric tests that a valid metric is accepted.
func TestIngestSingle_ValidMetric(t *testing.T) {
	router := gin.New()
	router.POST("/ingest", func(c *gin.Context) {
		var metric model.Metric
		if err := c.ShouldBindJSON(&metric); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "invalid", Code: 400})
			return
		}
		if err := validator.ValidateMetric(&metric); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "validation", Code: 400, Details: err.Error()})
			return
		}
		// Simulate accepted (no Kafka in tests)
		c.JSON(http.StatusAccepted, model.IngestResponse{Status: "accepted", Accepted: 1})
	})

	metric := model.Metric{
		Name: "api.request.duration_ms", Value: 142.7, Unit: "ms",
		Tags:      map[string]string{"service": "checkout"},
		Timestamp: time.Now().Unix(), Host: "prod-api-01",
	}
	body, _ := json.Marshal(metric)
	req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d", w.Code)
	}

	var resp model.IngestResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Accepted != 1 {
		t.Errorf("expected accepted=1, got %d", resp.Accepted)
	}
}

// TestIngestBatch_EmptyBatch tests that empty batch is rejected.
func TestIngestBatch_EmptyBatch(t *testing.T) {
	router := gin.New()
	router.POST("/ingest/batch", func(c *gin.Context) {
		var batch model.BatchRequest
		if err := c.ShouldBindJSON(&batch); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "invalid", Code: 400})
			return
		}
		if len(batch.Metrics) == 0 {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "empty batch", Code: 400})
			return
		}
		c.JSON(http.StatusAccepted, model.IngestResponse{Status: "accepted", Accepted: len(batch.Metrics)})
	})

	body, _ := json.Marshal(model.BatchRequest{Metrics: []model.Metric{}})
	req := httptest.NewRequest("POST", "/ingest/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// TestIngestBatch_ValidBatch tests batch ingestion with valid metrics.
func TestIngestBatch_ValidBatch(t *testing.T) {
	router := gin.New()
	router.POST("/ingest/batch", func(c *gin.Context) {
		var batch model.BatchRequest
		if err := c.ShouldBindJSON(&batch); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "invalid", Code: 400})
			return
		}
		valid := 0
		for _, m := range batch.Metrics {
			if err := validator.ValidateMetric(&m); err == nil {
				valid++
			}
		}
		if valid == 0 {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "no valid", Code: 400})
			return
		}
		c.JSON(http.StatusAccepted, model.IngestResponse{Status: "accepted", Accepted: valid})
	})

	batch := model.BatchRequest{
		Metrics: []model.Metric{
			{Name: "cpu.usage", Value: 67.3, Unit: "percent", Tags: map[string]string{"svc": "api"}, Timestamp: time.Now().Unix(), Host: "prod-01"},
			{Name: "mem.usage", Value: 2048, Unit: "bytes", Tags: map[string]string{"svc": "api"}, Timestamp: time.Now().Unix(), Host: "prod-01"},
			{Name: "disk.io", Value: 1024, Unit: "bytes", Tags: map[string]string{"svc": "api"}, Timestamp: time.Now().Unix(), Host: "prod-01"},
		},
	}
	body, _ := json.Marshal(batch)
	req := httptest.NewRequest("POST", "/ingest/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d", w.Code)
	}

	var resp model.IngestResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Accepted != 3 {
		t.Errorf("expected accepted=3, got %d", resp.Accepted)
	}
}

// TestIngestBindingAcceptsExplicitZeroValues distinguishes an omitted value
// from a valid metric whose value is zero.  Gin's `required` validator treats
// a float64 zero value as missing, so this must exercise JSON binding rather
// than only the domain validator.
func TestIngestBindingAcceptsExplicitZeroValues(t *testing.T) {
	timestamp := time.Now().Unix()
	cases := []struct {
		name string
		path string
		body string
	}{
		{
			name: "single integer zero",
			path: "/ingest",
			body: fmt.Sprintf(`{"name":"zero.metric","value":0,"timestamp":%d,"host":"test"}`, timestamp),
		},
		{
			name: "single decimal zero",
			path: "/ingest",
			body: fmt.Sprintf(`{"name":"zero.metric","value":0.0,"timestamp":%d,"host":"test"}`, timestamp),
		},
		{
			name: "batch integer and decimal zero",
			path: "/ingest/batch",
			body: fmt.Sprintf(`{"metrics":[{"name":"zero.integer","value":0,"timestamp":%d,"host":"test"},{"name":"zero.decimal","value":0.0,"timestamp":%d,"host":"test"}]}`, timestamp, timestamp),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.POST("/ingest", func(c *gin.Context) {
				var metric model.Metric
				if err := c.ShouldBindJSON(&metric); err != nil {
					c.Status(http.StatusBadRequest)
					return
				}
				if err := validator.ValidateMetric(&metric); err != nil {
					c.Status(http.StatusBadRequest)
					return
				}
				c.Status(http.StatusAccepted)
			})
			router.POST("/ingest/batch", func(c *gin.Context) {
				var batch model.BatchRequest
				if err := c.ShouldBindJSON(&batch); err != nil {
					c.Status(http.StatusBadRequest)
					return
				}
				for i := range batch.Metrics {
					if err := validator.ValidateMetric(&batch.Metrics[i]); err != nil {
						c.Status(http.StatusBadRequest)
						return
					}
				}
				c.Status(http.StatusAccepted)
			})

			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusAccepted {
				t.Fatalf("explicit zero value was rejected: status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestIngestBindingRejectsMissingValue(t *testing.T) {
	router := gin.New()
	router.POST("/ingest", func(c *gin.Context) {
		var metric model.Metric
		if err := c.ShouldBindJSON(&metric); err != nil {
			c.Status(http.StatusBadRequest)
			return
		}
		c.Status(http.StatusAccepted)
	})

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewBufferString(fmt.Sprintf(`{"name":"missing.value","timestamp":%d,"host":"test"}`, time.Now().Unix())))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing value was accepted: status=%d", w.Code)
	}
}
