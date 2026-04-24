package cache

import (
	"context"
	"testing"
	"time"

	"log/slog"
	"os"

	"github.com/alicebob/miniredis/v2"
)

func setupTestCache(t *testing.T) (*RedisCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)

	cfg := Config{
		Addr: mr.Addr(),
		TTL:  5 * time.Minute,
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	rc, err := New(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("failed to create redis cache: %v", err)
	}

	return rc, mr
}

func TestCacheMetric_SetAndGet(t *testing.T) {
	rc, _ := setupTestCache(t)
	defer rc.Close()
	ctx := context.Background()

	tags := map[string]string{"service": "api", "region": "us-east-1"}
	err := rc.CacheMetric(ctx, "cpu.usage", "host-01", 85.5, tags)
	if err != nil {
		t.Fatalf("CacheMetric failed: %v", err)
	}

	result, err := rc.GetMetric(ctx, "cpu.usage", "host-01")
	if err != nil {
		t.Fatalf("GetMetric failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected cached metric, got nil (cache miss)")
	}
	if result["name"] != "cpu.usage" {
		t.Errorf("expected name=cpu.usage, got %v", result["name"])
	}
	if result["host"] != "host-01" {
		t.Errorf("expected host=host-01, got %v", result["host"])
	}
}

func TestGetMetric_CacheMiss(t *testing.T) {
	rc, _ := setupTestCache(t)
	defer rc.Close()
	ctx := context.Background()

	result, err := rc.GetMetric(ctx, "nonexistent.metric", "unknown-host")
	if err != nil {
		t.Fatalf("GetMetric returned error on miss: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil on cache miss, got %v", result)
	}
}

func TestCacheMetric_TTLExpiry(t *testing.T) {
	rc, mr := setupTestCache(t)
	defer rc.Close()
	ctx := context.Background()

	err := rc.CacheMetric(ctx, "mem.free", "host-02", 1024.0, nil)
	if err != nil {
		t.Fatalf("CacheMetric failed: %v", err)
	}

	// Verify it exists
	result, _ := rc.GetMetric(ctx, "mem.free", "host-02")
	if result == nil {
		t.Fatal("expected cached metric before TTL expiry")
	}

	// Fast-forward time past TTL
	mr.FastForward(6 * time.Minute)

	result, _ = rc.GetMetric(ctx, "mem.free", "host-02")
	if result != nil {
		t.Error("expected cache miss after TTL expiry")
	}
}

func TestTrackRecentMetric_AndRetrieve(t *testing.T) {
	rc, _ := setupTestCache(t)
	defer rc.Close()
	ctx := context.Background()

	now := time.Now().Unix()

	// Track 5 data points
	for i := int64(0); i < 5; i++ {
		err := rc.TrackRecentMetric(ctx, "api.latency", float64(i*10), now+i)
		if err != nil {
			t.Fatalf("TrackRecentMetric failed at i=%d: %v", i, err)
		}
	}

	// Retrieve all within range
	results, err := rc.GetRecentMetrics(ctx, "api.latency", now, now+4)
	if err != nil {
		t.Fatalf("GetRecentMetrics failed: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 recent metrics, got %d", len(results))
	}

	// Retrieve partial range
	results, err = rc.GetRecentMetrics(ctx, "api.latency", now+2, now+4)
	if err != nil {
		t.Fatalf("GetRecentMetrics partial failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 recent metrics in partial range, got %d", len(results))
	}
}

func TestIncrementCounter(t *testing.T) {
	rc, _ := setupTestCache(t)
	defer rc.Close()
	ctx := context.Background()

	err := rc.IncrementCounter(ctx, "metrics_ingested", 100)
	if err != nil {
		t.Fatalf("IncrementCounter failed: %v", err)
	}
	err = rc.IncrementCounter(ctx, "metrics_ingested", 50)
	if err != nil {
		t.Fatalf("IncrementCounter second call failed: %v", err)
	}

	val, err := rc.GetStats(ctx, "metrics_ingested")
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if val != 150 {
		t.Errorf("expected counter=150, got %d", val)
	}
}

func TestGetStats_NonExistent(t *testing.T) {
	rc, _ := setupTestCache(t)
	defer rc.Close()
	ctx := context.Background()

	val, err := rc.GetStats(ctx, "nonexistent_counter")
	if err != nil {
		t.Fatalf("GetStats returned error: %v", err)
	}
	if val != 0 {
		t.Errorf("expected 0 for nonexistent counter, got %d", val)
	}
}

func TestIsHealthy(t *testing.T) {
	rc, _ := setupTestCache(t)
	defer rc.Close()
	ctx := context.Background()

	if !rc.IsHealthy(ctx) {
		t.Error("expected healthy cache")
	}
}
