package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ingestor_requests_total",
			Help: "Total HTTP requests received by the ingestor.",
		},
		[]string{"method", "path", "status_code"},
	)

	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ingestor_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"method", "path"},
	)

	batchSizeHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ingestor_batch_size_histogram",
			Help:    "Distribution of batch sizes received.",
			Buckets: []float64{1, 10, 50, 100, 250, 500, 1000},
		},
	)

	kafkaPublishDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ingestor_kafka_publish_duration_seconds",
			Help:    "Time to publish a batch to Kafka.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
	)
)

// RecordBatchSize records the size of an ingested batch.
func RecordBatchSize(size float64) {
	batchSizeHistogram.Observe(size)
}

// RecordKafkaPublishDuration records the time taken to publish to Kafka.
func RecordKafkaPublishDuration(d time.Duration) {
	kafkaPublishDuration.Observe(d.Seconds())
}

// PrometheusMetrics returns a gin middleware that records request metrics.
func PrometheusMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		if path == "" {
			path = c.Request.URL.Path
		}

		c.Next()

		status := strconv.Itoa(c.Writer.Status())
		duration := time.Since(start).Seconds()

		requestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		requestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}
