package obsdk

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeIngestor scripts responses per request and records what it received.
type fakeIngestor struct {
	mu       sync.Mutex
	received [][]Metric
	auth     []string
	respond  func(call int, batch []Metric, w http.ResponseWriter)
	calls    atomic.Int32
}

func (f *fakeIngestor) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var b BatchRequest
		_ = json.Unmarshal(body, &b)
		f.mu.Lock()
		f.received = append(f.received, b.Metrics)
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		f.mu.Unlock()
		call := int(f.calls.Add(1))
		if f.respond == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		f.respond(call, b.Metrics, w)
	})
}

func (f *fakeIngestor) names() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for _, b := range f.received {
		for _, m := range b {
			out[m.Name]++
		}
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSendsBearerToken(t *testing.T) {
	f := &fakeIngestor{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithAPIKey("s3cret"), WithFlushInterval(20))
	c.Counter("a", 1, nil)
	waitFor(t, "send", func() bool { return c.Stats().Sent == 1 })
	_ = c.Close()
	if f.auth[0] != "Bearer s3cret" {
		t.Fatalf("Authorization = %q", f.auth[0])
	}
}

func TestGaugeUsesAUnitTheIngestorAccepts(t *testing.T) {
	f := &fakeIngestor{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithFlushInterval(20))
	c.Gauge("g", 1, nil)
	waitFor(t, "send", func() bool { return c.Stats().Sent == 1 })
	_ = c.Close()
	if u := f.received[0][0].Unit; u != "" {
		t.Fatalf(`gauge unit must be "" (ingestor rejects "gauge"), got %q`, u)
	}
}

func TestRetriesOn503ThenDelivers(t *testing.T) {
	f := &fakeIngestor{respond: func(call int, _ []Metric, w http.ResponseWriter) {
		if call <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithFlushInterval(10))
	for i := 0; i < 5; i++ {
		c.Counter(fmt.Sprintf("m%d", i), 1, nil)
	}
	waitFor(t, "delivery after retries", func() bool { return c.Stats().Sent == 5 })
	_ = c.Close()
	s := c.Stats()
	if s.Retried == 0 || s.Rejected != 0 || s.Dropped != 0 {
		t.Fatalf("unexpected stats %+v", s)
	}
}

func TestPartialResendsOnlyRetryableItems(t *testing.T) {
	f := &fakeIngestor{respond: func(call int, batch []Metric, w http.ResponseWriter) {
		if call == 1 {
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(`{"status":"partial","accepted":1,"rejected":1,"skipped":1,
				"errors":[{"index":1,"error":"bad name","retryable":false},
				          {"index":2,"error":"kafka","retryable":true}]}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithBatchSize(3), WithFlushInterval(10))
	c.Counter("ok", 1, nil)
	c.Counter("invalid", 1, nil)
	c.Counter("retry.me", 1, nil)
	waitFor(t, "resend", func() bool { return c.Stats().Sent == 2 })
	_ = c.Close()

	got := f.names()
	if got["ok"] != 1 || got["invalid"] != 1 || got["retry.me"] != 2 {
		t.Fatalf("want ok x1, invalid x1 (not resent), retry.me x2; got %v", got)
	}
	if s := c.Stats(); s.Rejected != 1 {
		t.Fatalf("invalid item should count as rejected: %+v", s)
	}
}

func TestClientErrorsAreNotRetried(t *testing.T) {
	f := &fakeIngestor{respond: func(int, []Metric, http.ResponseWriter) {}}
	f.respond = func(_ int, _ []Metric, w http.ResponseWriter) { w.WriteHeader(http.StatusUnauthorized) }
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	var errs atomic.Int32
	c := New(srv.URL, WithFlushInterval(10), WithOnError(func(error) { errs.Add(1) }))
	c.Counter("a", 1, nil)
	waitFor(t, "rejection", func() bool { return c.Stats().Rejected == 1 })
	time.Sleep(100 * time.Millisecond)
	_ = c.Close()
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("401 must not be retried; calls=%d", n)
	}
	if errs.Load() == 0 {
		t.Fatal("OnError must be told about the rejection")
	}
}

func TestHonoursRetryAfter(t *testing.T) {
	var first time.Time
	var second time.Time
	f := &fakeIngestor{}
	f.respond = func(call int, _ []Metric, w http.ResponseWriter) {
		if call == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		second = time.Now()
		w.WriteHeader(http.StatusAccepted)
	}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithFlushInterval(10))
	c.Counter("a", 1, nil)
	waitFor(t, "delivery", func() bool { return c.Stats().Sent == 1 })
	_ = c.Close()
	if gap := second.Sub(first); gap < 900*time.Millisecond {
		t.Fatalf("retried after %v; Retry-After asked for 1s", gap)
	}
}

func TestBufferIsBounded(t *testing.T) {
	// Ingestor down: nothing drains, so the buffer must cap and count drops.
	f := &fakeIngestor{respond: func(_ int, _ []Metric, w http.ResponseWriter) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithBatchSize(10), WithMaxBuffer(50), WithFlushInterval(5),
		WithCloseTimeout(50*time.Millisecond))
	for i := 0; i < 500; i++ {
		c.Counter("flood", 1, nil)
	}
	if b := c.Stats().Buffered; b > 50 {
		t.Fatalf("buffer exceeded its bound: %d", b)
	}
	err := c.Close()
	s := c.Stats()
	if s.Dropped == 0 {
		t.Fatalf("overflow must be counted as dropped: %+v", s)
	}
	if err == nil || s.Abandoned == 0 {
		t.Fatalf("undelivered data at close must be reported: err=%v stats=%+v", err, s)
	}
	if s.Sent+s.Dropped+s.Abandoned+s.Rejected != 500 {
		t.Fatalf("every metric must be accounted for: %+v", s)
	}
}

func TestCloseIsIdempotentAndFlushes(t *testing.T) {
	f := &fakeIngestor{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithFlushInterval(60_000)) // only Close will flush
	c.Counter("last.words", 1, nil)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal("second Close must not panic or fail")
	}
	if f.names()["last.words"] != 1 {
		t.Fatal("Close must deliver buffered metrics")
	}
}

func TestBatchSizeCappedAtServerLimit(t *testing.T) {
	c := New("http://127.0.0.1:1", WithBatchSize(5000))
	defer c.Close()
	if c.batchSize != MaxBatch {
		t.Fatalf("batch size %d exceeds the ingestor's 1000-item limit", c.batchSize)
	}
}

func TestInstrumentRouteUsesTemplate(t *testing.T) {
	f := &fakeIngestor{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(srv.URL, WithFlushInterval(10))
	h := InstrumentRoute(c, "api", "/users/{id}", func(w http.ResponseWriter, _ *http.Request) {})
	h(httptest.NewRecorder(), httptest.NewRequest("GET", "/users/12345", nil))
	waitFor(t, "send", func() bool { return c.Stats().Sent >= 2 })
	_ = c.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range f.received {
		for _, m := range b {
			if m.Tags["endpoint"] != "/users/{id}" {
				t.Fatalf("endpoint tag must be the route template, got %q", m.Tags["endpoint"])
			}
		}
	}
}
