package obsdk

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	c := New("http://localhost:8080", WithHost("test-host"), WithBatchSize(10))
	defer c.Close()

	if c.host != "test-host" {
		t.Errorf("expected host 'test-host', got '%s'", c.host)
	}
	if c.batchSize != 10 {
		t.Errorf("expected batchSize 10, got %d", c.batchSize)
	}
}

func TestGaugeCounterHistogram(t *testing.T) {
	// Mock ingestor server
	received := make(chan BatchRequest, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ingest/batch" {
			body, _ := io.ReadAll(r.Body)
			var batch BatchRequest
			json.Unmarshal(body, &batch)
			received <- batch
			w.WriteHeader(202)
			w.Write([]byte(`{"status":"accepted","accepted":` + "1" + `}`))
		} else if r.URL.Path == "/health" {
			w.WriteHeader(200)
		}
	}))
	defer server.Close()

	c := New(server.URL, WithHost("test"), WithBatchSize(3), WithFlushInterval(50))
	defer c.Close()

	c.Gauge("cpu.usage", 67.3, Tags{"service": "api"})
	c.Counter("requests.total", 1, Tags{"endpoint": "/cart"})
	c.Histogram("latency.ms", 142.7, Tags{"method": "GET"})

	// Wait for auto-flush (batch size = 3, so should flush after 3rd metric)
	select {
	case batch := <-received:
		if len(batch.Metrics) != 3 {
			t.Errorf("expected 3 metrics in batch, got %d", len(batch.Metrics))
		}
		if batch.Metrics[0].Name != "cpu.usage" {
			t.Errorf("expected first metric name 'cpu.usage', got '%s'", batch.Metrics[0].Name)
		}
		if batch.Metrics[0].Host != "test" {
			t.Errorf("expected host 'test', got '%s'", batch.Metrics[0].Host)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for batch flush")
	}
}

func TestTimerFunc(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
		w.Write([]byte(`{"status":"accepted","accepted":1}`))
	}))
	defer server.Close()

	c := New(server.URL, WithHost("test"), WithBatchSize(100), WithFlushInterval(50))
	defer c.Close()

	c.TimerFunc("slow.operation", Tags{"op": "compute"}, func() {
		time.Sleep(10 * time.Millisecond)
	})

	c.mu.Lock()
	if len(c.buffer) != 1 {
		t.Errorf("expected 1 buffered metric, got %d", len(c.buffer))
	}
	if c.buffer[0].Value < 10 {
		t.Errorf("expected timer value >= 10ms, got %f", c.buffer[0].Value)
	}
	c.mu.Unlock()
}

func TestHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			w.Write([]byte(`{"status":"healthy"}`))
		}
	}))
	defer server.Close()

	c := New(server.URL, WithHost("test"))
	defer c.Close()

	if !c.Healthy() {
		t.Error("expected client to report healthy")
	}
}

func TestInstrumentHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
	}))
	defer server.Close()

	c := New(server.URL, WithHost("test"), WithBatchSize(100))
	defer c.Close()

	handler := InstrumentHTTP(c, "api.request", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	req := httptest.NewRequest("GET", "/api/v1/test", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	c.mu.Lock()
	if len(c.buffer) < 2 {
		t.Errorf("expected at least 2 metrics (duration + count), got %d", len(c.buffer))
	}
	c.mu.Unlock()
}
