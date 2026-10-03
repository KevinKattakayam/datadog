package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/KevinKattakayam/datadog/ingestor/internal/middleware"
	"github.com/KevinKattakayam/datadog/ingestor/internal/model"
)

// These tests drive the production IngestHandler. The previous versions
// re-implemented a handler inline in each test and asserted on that copy,
// so the real 202/207/503 logic had no coverage at all.

func init() {
	gin.SetMode(gin.TestMode)
}

// fakePublisher records what reached "Kafka" and can fail chosen records.
type fakePublisher struct {
	mu        sync.Mutex
	down      bool                      // every publish fails
	failNames map[string]bool           // batch items with these names fail
	published map[string][]model.Metric // tenant -> metrics acked
	calls     int
}

var errBroker = errors.New("NOT_ENOUGH_REPLICAS")

func newFake() *fakePublisher {
	return &fakePublisher{failNames: map[string]bool{}, published: map[string][]model.Metric{}}
}

func (f *fakePublisher) Publish(_ context.Context, tenant string, m *model.Metric) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.down {
		return errBroker
	}
	f.published[tenant] = append(f.published[tenant], *m)
	return nil
}

func (f *fakePublisher) PublishBatch(_ context.Context, tenant string, ms []model.Metric) ([]error, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	out := make([]error, len(ms))
	for i, m := range ms {
		if f.down || f.failNames[m.Name] {
			out[i] = errBroker
			continue
		}
		f.published[tenant] = append(f.published[tenant], m)
	}
	return out, nil
}

func (f *fakePublisher) count(tenant string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published[tenant])
}

// router wires the real handler behind a stub that sets the tenant the way
// APIKeyAuth would, plus the production body-size middleware.
func router(p Publisher, q Quota, maxBody int64) *gin.Engine {
	h := NewIngestHandler(p, q, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r := gin.New()
	r.Use(middleware.MaxBodyBytes(maxBody))
	r.Use(func(c *gin.Context) {
		tenant := c.GetHeader("X-Test-Tenant")
		if tenant == "" {
			tenant = "tenant-a"
		}
		c.Set(middleware.TenantContextKey, tenant)
	})
	r.POST("/ingest", h.IngestSingle)
	r.POST("/ingest/batch", h.IngestBatch)
	return r
}

func metricJSON(name string) string {
	return fmt.Sprintf(`{"name":%q,"value":1.5,"unit":"ms","timestamp":%d,"host":"prod-01","tags":{"svc":"api"}}`,
		name, time.Now().Unix())
}

func batchJSON(items ...string) string {
	return `{"metrics":[` + strings.Join(items, ",") + `]}`
}

func do(t *testing.T, r *gin.Engine, path, body string, headers ...string) (*httptest.ResponseRecorder, model.IngestResponse) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var resp model.IngestResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	return w, resp
}

// ── Single ───────────────────────────────────────────────────

func TestSingle_AcceptedOnlyAfterPublish(t *testing.T) {
	f := newFake()
	w, resp := do(t, router(f, nil, 0), "/ingest", metricJSON("api.latency"))
	if w.Code != http.StatusAccepted || resp.Accepted != 1 {
		t.Fatalf("want 202 accepted=1, got %d %s", w.Code, w.Body)
	}
	if f.count("tenant-a") != 1 {
		t.Fatal("metric was not published under the authenticated tenant")
	}
}

func TestSingle_KafkaDownIs503WithRetryAfter(t *testing.T) {
	f := newFake()
	f.down = true
	w, _ := do(t, router(f, nil, 0), "/ingest", metricJSON("api.latency"))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("Kafka down must be 503, never 202; got %d", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("503 must carry Retry-After")
	}
}

func TestSingle_InvalidJSONAndInvalidMetricAre400(t *testing.T) {
	f := newFake()
	r := router(f, nil, 0)
	for _, body := range []string{
		`{not json`,
		`{"name":"no.value","timestamp":1,"host":"h"}`,
		fmt.Sprintf(`{"name":"bad..name","value":1,"timestamp":%d,"host":"h"}`, time.Now().Unix()),
		fmt.Sprintf(`{"name":"ok","value":1,"timestamp":%d,"host":""}`, time.Now().Unix()),
	} {
		if w, _ := do(t, r, "/ingest", body); w.Code != http.StatusBadRequest {
			t.Errorf("body %q: want 400, got %d", body, w.Code)
		}
	}
	if f.calls != 0 {
		t.Fatal("invalid input must never reach Kafka")
	}
}

func TestSingle_ExplicitZeroValueIsAccepted(t *testing.T) {
	f := newFake()
	r := router(f, nil, 0)
	for _, v := range []string{"0", "0.0", "-0"} {
		body := fmt.Sprintf(`{"name":"zero.metric","value":%s,"timestamp":%d,"host":"h"}`, v, time.Now().Unix())
		if w, _ := do(t, r, "/ingest", body); w.Code != http.StatusAccepted {
			t.Errorf("value %s rejected: %d %s", v, w.Code, w.Body)
		}
	}
}

// ── Body limit ───────────────────────────────────────────────

func TestBodyLimit_DeclaredLengthIs413(t *testing.T) {
	w, _ := do(t, router(newFake(), nil, 64), "/ingest", metricJSON("x"+strings.Repeat("y", 200)))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d", w.Code)
	}
}

func TestBodyLimit_ChunkedBodyIs413(t *testing.T) {
	// No Content-Length: the limit must still hold while streaming.
	r := router(newFake(), nil, 64)
	body := io.MultiReader(strings.NewReader(metricJSON("x" + strings.Repeat("y", 200))))
	req := httptest.NewRequest(http.MethodPost, "/ingest", body)
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413 for chunked oversize body, got %d", w.Code)
	}
}

// ── Batch ────────────────────────────────────────────────────

func TestBatch_AllAcceptedIs202(t *testing.T) {
	f := newFake()
	w, resp := do(t, router(f, nil, 0), "/ingest/batch",
		batchJSON(metricJSON("a.one"), metricJSON("a.two"), metricJSON("a.three")))
	if w.Code != http.StatusAccepted || resp.Accepted != 3 || len(resp.Errors) != 0 {
		t.Fatalf("want 202 accepted=3, got %d %s", w.Code, w.Body)
	}
}

func TestBatch_InvalidItemsAreSkippedWithIndices(t *testing.T) {
	// Roadmap acceptance: a batch with 3 invalid metrics reports skipped: 3.
	f := newFake()
	bad := fmt.Sprintf(`{"name":"bad..name","value":1,"timestamp":%d,"host":"h"}`, time.Now().Unix())
	w, resp := do(t, router(f, nil, 0), "/ingest/batch",
		batchJSON(bad, metricJSON("ok.one"), bad, metricJSON("ok.two"), bad))
	if w.Code != http.StatusMultiStatus {
		t.Fatalf("want 207, got %d %s", w.Code, w.Body)
	}
	if resp.Accepted != 2 || resp.Skipped != 3 || resp.Rejected != 0 {
		t.Fatalf("counts wrong: %+v", resp)
	}
	want := []int{0, 2, 4}
	if len(resp.Errors) != 3 {
		t.Fatalf("want 3 item errors, got %+v", resp.Errors)
	}
	for i, e := range resp.Errors {
		if e.Index != want[i] || e.Retryable {
			t.Errorf("error %d: want index %d non-retryable, got %+v", i, want[i], e)
		}
	}
}

func TestBatch_PartialKafkaFailureReportsOriginalIndices(t *testing.T) {
	// Interleave an invalid item so the valid->request index mapping is
	// actually exercised: Kafka failures must point at request positions.
	f := newFake()
	f.failNames["fail.me"] = true
	bad := fmt.Sprintf(`{"name":"bad..name","value":1,"timestamp":%d,"host":"h"}`, time.Now().Unix())
	w, resp := do(t, router(f, nil, 0), "/ingest/batch",
		batchJSON(metricJSON("ok.one"), bad, metricJSON("fail.me"), metricJSON("ok.two")))
	if w.Code != http.StatusMultiStatus {
		t.Fatalf("want 207, got %d %s", w.Code, w.Body)
	}
	if resp.Accepted != 2 || resp.Rejected != 1 || resp.Skipped != 1 {
		t.Fatalf("counts wrong: %+v", resp)
	}
	if resp.Accepted+resp.Rejected+resp.Skipped != 4 {
		t.Fatal("counts must partition the request")
	}
	if len(resp.Errors) != 2 ||
		resp.Errors[0].Index != 1 || resp.Errors[0].Retryable ||
		resp.Errors[1].Index != 2 || !resp.Errors[1].Retryable {
		t.Fatalf("want [idx1 non-retryable, idx2 retryable], got %+v", resp.Errors)
	}
}

func TestBatch_KafkaDownIs503(t *testing.T) {
	f := newFake()
	f.down = true
	w, resp := do(t, router(f, nil, 0), "/ingest/batch", batchJSON(metricJSON("a"), metricJSON("b")))
	if w.Code != http.StatusServiceUnavailable || resp.Accepted != 0 || resp.Rejected != 2 {
		t.Fatalf("want 503 rejected=2, got %d %s", w.Code, w.Body)
	}
	for _, e := range resp.Errors {
		if !e.Retryable {
			t.Fatalf("Kafka failures must be retryable: %+v", e)
		}
	}
}

func TestBatch_NothingValidIs400(t *testing.T) {
	f := newFake()
	bad := fmt.Sprintf(`{"name":"","value":1,"timestamp":%d,"host":"h"}`, time.Now().Unix())
	w, resp := do(t, router(f, nil, 0), "/ingest/batch", batchJSON(bad, bad))
	if w.Code != http.StatusBadRequest || resp.Skipped != 2 || len(resp.Errors) != 2 {
		t.Fatalf("want 400 skipped=2, got %d %s", w.Code, w.Body)
	}
	if f.calls != 0 {
		t.Fatal("nothing should be published")
	}
}

func TestBatch_EmptyAndOversizedBatchAre400(t *testing.T) {
	r := router(newFake(), nil, 0)
	if w, _ := do(t, r, "/ingest/batch", `{"metrics":[]}`); w.Code != http.StatusBadRequest {
		t.Fatalf("empty batch: want 400, got %d", w.Code)
	}
	items := make([]string, 1001)
	for i := range items {
		items[i] = metricJSON("m")
	}
	if w, _ := do(t, r, "/ingest/batch", batchJSON(items...)); w.Code != http.StatusBadRequest {
		t.Fatalf("1001 items: want 400, got %d", w.Code)
	}
}

// ── Quota ────────────────────────────────────────────────────

func TestQuota_CountsMetricsNotRequests(t *testing.T) {
	// 10 metrics/sec, burst 10. One batch of 8 fits; a second does not —
	// even though it is only the second *request*.
	f := newFake()
	q := middleware.NewTenantQuota(10, 10, nil)
	r := router(f, q, 0)
	items := make([]string, 8)
	for i := range items {
		items[i] = metricJSON("q.metric")
	}
	if w, _ := do(t, r, "/ingest/batch", batchJSON(items...)); w.Code != http.StatusAccepted {
		t.Fatalf("first batch: want 202, got %d", w.Code)
	}
	w, _ := do(t, r, "/ingest/batch", batchJSON(items...))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second batch must exceed the metrics quota: got %d", w.Code)
	}
	ra, err := strconv.Atoi(w.Header().Get("Retry-After"))
	if err != nil || ra < 1 {
		t.Fatalf("429 needs Retry-After >= 1s, got %q", w.Header().Get("Retry-After"))
	}
	if f.count("tenant-a") != 8 {
		t.Fatalf("refused batch must not be published; published=%d", f.count("tenant-a"))
	}
}

func TestQuota_TenantsAreIsolated(t *testing.T) {
	f := newFake()
	q := middleware.NewTenantQuota(5, 5, nil)
	r := router(f, q, 0)
	items := make([]string, 5)
	for i := range items {
		items[i] = metricJSON("iso.metric")
	}
	do(t, r, "/ingest/batch", batchJSON(items...), "X-Test-Tenant", "noisy")
	if w, _ := do(t, r, "/ingest/batch", batchJSON(items...), "X-Test-Tenant", "noisy"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("noisy tenant should be throttled, got %d", w.Code)
	}
	if w, _ := do(t, r, "/ingest/batch", batchJSON(items...), "X-Test-Tenant", "quiet"); w.Code != http.StatusAccepted {
		t.Fatalf("tenant B must not be throttled by tenant A's flood, got %d", w.Code)
	}
}

func TestQuota_RefillsOverTime(t *testing.T) {
	q := middleware.NewTenantQuota(1000, 1000, nil)
	if ok, _ := q.AllowN("t", 1000); !ok {
		t.Fatal("full burst should be admitted")
	}
	ok, wait := q.AllowN("t", 100)
	if ok || wait <= 0 || wait > 200*time.Millisecond {
		t.Fatalf("want refusal with ~100ms wait, got ok=%v wait=%v", ok, wait)
	}
	time.Sleep(wait + 20*time.Millisecond)
	if ok, _ := q.AllowN("t", 100); !ok {
		t.Fatal("quota should have refilled")
	}
}

func TestQuota_BatchLargerThanBurstSaysSplit(t *testing.T) {
	q := middleware.NewTenantQuota(10, 10, nil)
	if ok, wait := q.AllowN("t", 11); ok || wait >= 0 {
		t.Fatalf("n > burst can never succeed; want negative wait, got ok=%v wait=%v", ok, wait)
	}
}
