// Package obsdk provides a Go SDK for instrumenting applications
// to send metrics to the Observability Pipeline ingestor.
//
// Usage:
//
//	client := obsdk.New("http://localhost:8080", obsdk.WithHost("my-service-01"))
//	defer client.Close()
//
//	client.Gauge("cpu.usage.percent", 67.3, obsdk.Tags{"service": "api"})
//	client.Counter("api.requests.total", 1, obsdk.Tags{"endpoint": "/cart"})
//	client.Histogram("api.latency.ms", 142.7, obsdk.Tags{"method": "GET"})
package obsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// Tags is a convenience type for metric tags.
type Tags map[string]string

// Metric represents a single metric data point.
type Metric struct {
	Name      string `json:"name"`
	Value     float64 `json:"value"`
	Unit      string  `json:"unit"`
	Tags      Tags    `json:"tags"`
	Timestamp int64   `json:"timestamp"`
	Host      string  `json:"host"`
}

// BatchRequest is the payload for the batch ingest endpoint.
type BatchRequest struct {
	Metrics []Metric `json:"metrics"`
}

// Client is the SDK client for the observability pipeline.
type Client struct {
	endpoint   string
	host       string
	httpClient *http.Client
	batchSize  int
	flushMs    int

	mu      sync.Mutex
	buffer  []Metric
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// Option configures the SDK client.
type Option func(*Client)

// WithHost sets the host identifier for all metrics.
func WithHost(host string) Option {
	return func(c *Client) { c.host = host }
}

// WithBatchSize sets the batch size before auto-flush.
func WithBatchSize(size int) Option {
	return func(c *Client) { c.batchSize = size }
}

// WithFlushInterval sets the auto-flush interval in milliseconds.
func WithFlushInterval(ms int) Option {
	return func(c *Client) { c.flushMs = ms }
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) { c.httpClient = client }
}

// New creates a new SDK client.
func New(endpoint string, opts ...Option) *Client {
	hostname, _ := os.Hostname()
	c := &Client{
		endpoint:   endpoint,
		host:       hostname,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		batchSize:  100,
		flushMs:    1000,
		buffer:     make([]Metric, 0, 100),
		closeCh:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}

	// Start background flusher
	c.wg.Add(1)
	go c.flusher()

	return c
}

// Gauge records a gauge metric (current value).
func (c *Client) Gauge(name string, value float64, tags Tags) {
	c.record(name, value, "gauge", tags)
}

// Counter records a counter metric (incremental).
func (c *Client) Counter(name string, value float64, tags Tags) {
	c.record(name, value, "count", tags)
}

// Histogram records a histogram/distribution metric.
func (c *Client) Histogram(name string, value float64, tags Tags) {
	c.record(name, value, "ms", tags)
}

// Timer records a duration metric.
func (c *Client) Timer(name string, duration time.Duration, tags Tags) {
	c.record(name, float64(duration.Milliseconds()), "ms", tags)
}

// TimerFunc measures and records the duration of a function call.
func (c *Client) TimerFunc(name string, tags Tags, fn func()) {
	start := time.Now()
	fn()
	c.Timer(name, time.Since(start), tags)
}

func (c *Client) record(name string, value float64, unit string, tags Tags) {
	m := Metric{
		Name:      name,
		Value:     value,
		Unit:      unit,
		Tags:      tags,
		Timestamp: time.Now().Unix(),
		Host:      c.host,
	}

	c.mu.Lock()
	c.buffer = append(c.buffer, m)
	shouldFlush := len(c.buffer) >= c.batchSize
	c.mu.Unlock()

	if shouldFlush {
		go c.flush()
	}
}

func (c *Client) flusher() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(c.flushMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.flush()
		case <-c.closeCh:
			c.flush() // Final flush
			return
		}
	}
}

func (c *Client) flush() {
	c.mu.Lock()
	if len(c.buffer) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.buffer
	c.buffer = make([]Metric, 0, c.batchSize)
	c.mu.Unlock()

	body := BatchRequest{Metrics: batch}
	data, err := json.Marshal(body)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/ingest/batch", bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

// Flush forces any buffered metrics to be sent immediately.
func (c *Client) Flush() {
	c.flush()
}

// Close gracefully shuts down the client, flushing remaining metrics.
func (c *Client) Close() error {
	close(c.closeCh)
	c.wg.Wait()
	return nil
}

// Healthy checks if the ingestor is reachable.
func (c *Client) Healthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/health", nil)
	if err != nil {
		return false
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

// Example — instrumentation helper for HTTP handlers.
//
//	mux.HandleFunc("/api/order", obsdk.InstrumentHTTP(client, "api.request", handler))
func InstrumentHTTP(client *Client, prefix string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tags := Tags{
			"method":   r.Method,
			"endpoint": r.URL.Path,
		}

		// Wrap ResponseWriter to capture status code
		wrapper := &statusWriter{ResponseWriter: w, status: 200}
		next(wrapper, r)

		duration := time.Since(start)
		tags["status"] = fmt.Sprintf("%d", wrapper.status)

		client.Histogram(prefix+".duration_ms", float64(duration.Milliseconds()), tags)
		client.Counter(prefix+".count", 1, tags)

		if wrapper.status >= 500 {
			client.Counter(prefix+".errors", 1, tags)
		}
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
