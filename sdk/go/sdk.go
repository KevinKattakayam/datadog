// Package obsdk provides a Go SDK for instrumenting applications
// to send metrics to the Observability Pipeline ingestor.
//
// Usage:
//
//	client := obsdk.New("https://ingest.example.com",
//		obsdk.WithAPIKey(os.Getenv("OBS_API_KEY")),
//		obsdk.WithHost("my-service-01"))
//	defer client.Close()
//
//	client.Gauge("cpu.usage.percent", 67.3, obsdk.Tags{"service": "api"})
//	client.Counter("api.requests.total", 1, obsdk.Tags{"endpoint": "/cart"})
//	client.Histogram("api.latency.ms", 142.7, obsdk.Tags{"method": "GET"})
//
// Delivery semantics. Recording never blocks the caller. Metrics are buffered
// in memory (bounded, see WithMaxBuffer) and sent in batches by one
// background worker. The worker honours the ingestor's contract:
//
//   - 202: all delivered.
//   - 207: items the server marks retryable are re-queued; the rest dropped.
//   - 429 / 503 / network error: the whole batch is re-queued and retried
//     with backoff, using the server's Retry-After when present.
//   - other 4xx: the batch can never succeed unchanged; it is dropped.
//
// Nothing is dropped silently: Stats reports every outcome and WithOnError
// receives every failure. Data still buffered when the process exits, or
// that overflowed the buffer, is lost — this is a client-side buffer, not a
// durable queue.
package obsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// MaxBatch is the ingestor's per-request limit.
const MaxBatch = 1000

// Tags is a convenience type for metric tags.
type Tags map[string]string

// Metric represents a single metric data point.
type Metric struct {
	Name      string  `json:"name"`
	Value     float64 `json:"value"`
	Unit      string  `json:"unit,omitempty"`
	Tags      Tags    `json:"tags,omitempty"`
	Timestamp int64   `json:"timestamp"`
	Host      string  `json:"host"`
}

// BatchRequest is the payload for the batch ingest endpoint.
type BatchRequest struct {
	Metrics []Metric `json:"metrics"`
}

type itemError struct {
	Index     int    `json:"index"`
	Error     string `json:"error"`
	Retryable bool   `json:"retryable"`
}

type ingestResponse struct {
	Accepted int         `json:"accepted"`
	Errors   []itemError `json:"errors"`
}

// Stats are cumulative counters since the client was created.
type Stats struct {
	Sent      uint64 // acknowledged by the ingestor (in Kafka)
	Retried   uint64 // re-queued after a retryable failure
	Rejected  uint64 // dropped: the server refused them as invalid
	Dropped   uint64 // dropped: the client buffer was full
	Abandoned uint64 // dropped: still unsent when Close's deadline expired
	Buffered  int    // currently waiting to be sent
}

// Client is the SDK client for the observability pipeline.
type Client struct {
	endpoint   string
	host       string
	apiKey     string
	httpClient *http.Client
	batchSize  int
	flushMs    int
	maxBuffer  int
	onError    func(error)
	closeWait  time.Duration

	mu     sync.Mutex
	buffer []Metric
	// backoffUntil pauses sending after 429/503 so the client does not
	// hammer an ingestor that has just asked it to wait.
	backoffUntil time.Time
	attempt      int

	kick      chan struct{}
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	sent, retried, rejected, dropped, abandoned atomic.Uint64
}

// Option configures the SDK client.
type Option func(*Client)

// WithHost sets the host identifier for all metrics.
func WithHost(host string) Option { return func(c *Client) { c.host = host } }

// WithAPIKey sets the bearer token. Required against any production ingestor.
func WithAPIKey(key string) Option { return func(c *Client) { c.apiKey = key } }

// WithBatchSize sets the batch size before auto-flush (capped at MaxBatch).
func WithBatchSize(size int) Option { return func(c *Client) { c.batchSize = size } }

// WithFlushInterval sets the auto-flush interval in milliseconds.
func WithFlushInterval(ms int) Option { return func(c *Client) { c.flushMs = ms } }

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) Option { return func(c *Client) { c.httpClient = client } }

// WithMaxBuffer bounds metrics held in memory while the ingestor is
// unreachable. When full, new metrics are dropped and counted in Stats.
func WithMaxBuffer(n int) Option { return func(c *Client) { c.maxBuffer = n } }

// WithOnError receives every delivery failure. Called from the worker
// goroutine; it must not block.
func WithOnError(fn func(error)) Option { return func(c *Client) { c.onError = fn } }

// WithCloseTimeout bounds how long Close keeps trying to deliver.
func WithCloseTimeout(d time.Duration) Option { return func(c *Client) { c.closeWait = d } }

// New creates a new SDK client and starts its background sender.
func New(endpoint string, opts ...Option) *Client {
	hostname, _ := os.Hostname()
	c := &Client{
		endpoint:   endpoint,
		host:       hostname,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		batchSize:  100,
		flushMs:    1000,
		maxBuffer:  10000,
		closeWait:  5 * time.Second,
		kick:       make(chan struct{}, 1),
		closeCh:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.batchSize <= 0 || c.batchSize > MaxBatch {
		c.batchSize = MaxBatch
	}
	if c.flushMs <= 0 {
		c.flushMs = 1000
	}
	if c.maxBuffer < c.batchSize {
		c.maxBuffer = c.batchSize
	}

	c.wg.Add(1)
	go c.worker()
	return c
}

// Gauge records a point-in-time value. The unit is left empty because
// "gauge" is a metric kind, not a unit; the ingestor rejects unknown units.
func (c *Client) Gauge(name string, value float64, tags Tags) {
	c.Record(name, value, "", tags)
}

// Counter records a counter metric (incremental).
func (c *Client) Counter(name string, value float64, tags Tags) {
	c.Record(name, value, "count", tags)
}

// Histogram records a latency-style distribution sample in milliseconds.
func (c *Client) Histogram(name string, value float64, tags Tags) {
	c.Record(name, value, "ms", tags)
}

// Timer records a duration metric.
func (c *Client) Timer(name string, duration time.Duration, tags Tags) {
	c.Record(name, float64(duration.Microseconds())/1000.0, "ms", tags)
}

// TimerFunc measures and records the duration of a function call.
func (c *Client) TimerFunc(name string, tags Tags, fn func()) {
	start := time.Now()
	fn()
	c.Timer(name, time.Since(start), tags)
}

// Record buffers one metric with an explicit unit (one of the ingestor's
// accepted units: "", ms, s, bytes, kb, mb, gb, count, percent, ops, req).
func (c *Client) Record(name string, value float64, unit string, tags Tags) {
	m := Metric{
		Name:      name,
		Value:     value,
		Unit:      unit,
		Tags:      tags,
		Timestamp: time.Now().Unix(),
		Host:      c.host,
	}

	c.mu.Lock()
	if len(c.buffer) >= c.maxBuffer {
		c.mu.Unlock()
		c.dropped.Add(1)
		c.report(errors.New("obsdk: buffer full; metric dropped"))
		return
	}
	c.buffer = append(c.buffer, m)
	full := len(c.buffer) >= c.batchSize
	c.mu.Unlock()

	if full {
		c.nudge()
	}
}

// Flush asks the worker to send what is buffered now. It does not wait.
func (c *Client) Flush() { c.nudge() }

// Stats returns delivery counters.
func (c *Client) Stats() Stats {
	c.mu.Lock()
	buffered := len(c.buffer)
	c.mu.Unlock()
	return Stats{
		Sent:      c.sent.Load(),
		Retried:   c.retried.Load(),
		Rejected:  c.rejected.Load(),
		Dropped:   c.dropped.Load(),
		Abandoned: c.abandoned.Load(),
		Buffered:  buffered,
	}
}

// Close stops the worker after trying, for up to the close timeout, to
// deliver everything buffered. Safe to call more than once.
func (c *Client) Close() error {
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.wg.Wait()
	if s := c.Stats(); s.Abandoned > 0 {
		return fmt.Errorf("obsdk: %d metrics undelivered at close", s.Abandoned)
	}
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
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

func (c *Client) nudge() {
	select {
	case c.kick <- struct{}{}:
	default: // a flush is already pending
	}
}

func (c *Client) report(err error) {
	if c.onError != nil {
		c.onError(err)
	}
}

// worker is the only goroutine that sends. One sender keeps batches ordered
// and bounds concurrency to one in-flight request per client.
func (c *Client) worker() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(c.flushMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.drain(context.Background(), false)
		case <-c.kick:
			c.drain(context.Background(), false)
		case <-c.closeCh:
			ctx, cancel := context.WithTimeout(context.Background(), c.closeWait)
			c.drain(ctx, true)
			cancel()
			c.mu.Lock()
			left := len(c.buffer)
			c.buffer = nil
			c.mu.Unlock()
			if left > 0 {
				c.abandoned.Add(uint64(left))
				c.report(fmt.Errorf("obsdk: %d metrics undelivered at close", left))
			}
			return
		}
	}
}

// drain sends full batches until the buffer is empty or a send fails. On
// close it also waits out backoff, bounded by ctx.
func (c *Client) drain(ctx context.Context, closing bool) {
	for {
		c.mu.Lock()
		wait := time.Until(c.backoffUntil)
		c.mu.Unlock()
		if wait > 0 {
			if !closing {
				return // the ticker will try again after the backoff
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
		}

		c.mu.Lock()
		n := len(c.buffer)
		if n == 0 {
			c.mu.Unlock()
			return
		}
		if n > c.batchSize {
			n = c.batchSize
		}
		batch := make([]Metric, n)
		copy(batch, c.buffer[:n])
		c.buffer = c.buffer[n:]
		c.mu.Unlock()

		if !c.send(ctx, batch) {
			if ctx.Err() != nil {
				return
			}
			if !closing {
				return
			}
		}
	}
}

// send delivers one batch and re-queues whatever should be retried.
// Returns true if the server accepted the request (202 or 207).
func (c *Client) send(ctx context.Context, batch []Metric) bool {
	data, err := json.Marshal(BatchRequest{Metrics: batch})
	if err != nil {
		// NaN or Inf values cannot be encoded; the ingestor would refuse them too.
		c.rejected.Add(uint64(len(batch)))
		c.report(fmt.Errorf("obsdk: encode batch: %w", err))
		return true
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.httpClient.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.endpoint+"/ingest/batch", bytes.NewReader(data))
	if err != nil {
		c.rejected.Add(uint64(len(batch)))
		c.report(fmt.Errorf("obsdk: build request: %w", err))
		return true
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.requeue(batch, 0)
		c.report(fmt.Errorf("obsdk: send: %w", err))
		return false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusAccepted:
		c.sent.Add(uint64(len(batch)))
		c.resetBackoff()
		return true

	case resp.StatusCode == http.StatusMultiStatus:
		var r ingestResponse
		if err := json.Unmarshal(body, &r); err != nil {
			// Cannot tell which items failed. Resending the whole batch
			// never loses data but may duplicate the accepted items, and a
			// client resend is a new Kafka record that ClickHouse will NOT
			// deduplicate. Chosen deliberately: duplicates are visible and
			// bounded; loss is silent.
			c.requeue(batch, 0)
			return true
		}
		var retry []Metric
		rejected := 0
		for _, e := range r.Errors {
			if e.Index < 0 || e.Index >= len(batch) {
				continue
			}
			if e.Retryable {
				retry = append(retry, batch[e.Index])
			} else {
				rejected++
				c.report(fmt.Errorf("obsdk: metric %q rejected: %s", batch[e.Index].Name, e.Error))
			}
		}
		c.sent.Add(uint64(r.Accepted))
		c.rejected.Add(uint64(rejected))
		c.resetBackoff()
		if len(retry) > 0 {
			c.requeue(retry, 0)
		}
		return true

	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		c.requeue(batch, retryAfter(resp.Header.Get("Retry-After")))
		c.report(fmt.Errorf("obsdk: ingestor returned %d; will retry", resp.StatusCode))
		return false

	default:
		// 400/401/403/413: resending unchanged cannot succeed.
		c.rejected.Add(uint64(len(batch)))
		c.report(fmt.Errorf("obsdk: ingestor returned %d: %s", resp.StatusCode, truncate(body, 256)))
		return true
	}
}

// requeue puts metrics back at the front of the buffer (oldest first) and
// schedules a backoff. Anything that no longer fits is counted as dropped.
func (c *Client) requeue(ms []Metric, serverWait time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	room := c.maxBuffer - len(c.buffer)
	if room < len(ms) {
		lost := len(ms) - max(room, 0)
		c.dropped.Add(uint64(lost))
		ms = ms[:max(room, 0)]
	}
	c.retried.Add(uint64(len(ms)))
	c.buffer = append(append(make([]Metric, 0, len(ms)+len(c.buffer)), ms...), c.buffer...)

	c.attempt++
	wait := serverWait
	if wait <= 0 {
		// Exponential backoff with full jitter: 100ms .. 30s.
		ceiling := 100 * time.Millisecond << min(c.attempt, 9)
		if ceiling > 30*time.Second {
			ceiling = 30 * time.Second
		}
		wait = time.Duration(rand.Int63n(int64(ceiling)) + 1)
	}
	c.backoffUntil = time.Now().Add(wait)
}

func (c *Client) resetBackoff() {
	c.mu.Lock()
	c.attempt = 0
	c.backoffUntil = time.Time{}
	c.mu.Unlock()
}

func retryAfter(h string) time.Duration {
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

// InstrumentHTTP records duration, count and 5xx errors for a handler.
//
// The endpoint tag is the raw URL path. If paths contain IDs, use
// InstrumentRoute instead: tagging "/users/123" creates one series per user.
//
//	mux.HandleFunc("/api/order", obsdk.InstrumentHTTP(client, "api.request", handler))
func InstrumentHTTP(client *Client, prefix string, next http.HandlerFunc) http.HandlerFunc {
	return InstrumentRoute(client, prefix, "", next)
}

// InstrumentRoute is InstrumentHTTP with a fixed, low-cardinality route tag
// (for example "/users/{id}"). An empty route falls back to the URL path.
func InstrumentRoute(client *Client, prefix, route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		endpoint := route
		if endpoint == "" {
			endpoint = r.URL.Path
		}
		wrapper := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next(wrapper, r)

		tags := Tags{
			"method":   r.Method,
			"endpoint": endpoint,
			"status":   strconv.Itoa(wrapper.status),
		}
		client.Timer(prefix+".duration_ms", time.Since(start), tags)
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
