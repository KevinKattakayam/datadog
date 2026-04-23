// Package cache implements a Redis-backed caching layer for recent metrics.
// It uses a cache-aside pattern: check cache first, fall through to storage
// on miss, then populate cache for subsequent reads.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DefaultTTL is the default cache entry time-to-live.
	DefaultTTL = 5 * time.Minute

	// MetricPrefix is the Redis key prefix for cached metrics.
	MetricPrefix = "metric:"

	// RecentPrefix is the Redis key prefix for recent metric lists.
	RecentPrefix = "recent:"

	// StatsPrefix is the Redis key prefix for aggregated statistics.
	StatsPrefix = "stats:"
)

// RedisCache provides a Redis-backed caching layer.
type RedisCache struct {
	client *redis.Client
	logger *slog.Logger
	ttl    time.Duration
}

// Config holds Redis connection configuration.
type Config struct {
	Addr     string
	Password string
	DB       int
	TTL      time.Duration
}

// New creates a new RedisCache connection.
func New(ctx context.Context, cfg Config, logger *slog.Logger) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     50,
		MinIdleConns: 10,
	})

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	ttl := cfg.TTL
	if ttl == 0 {
		ttl = DefaultTTL
	}

	logger.Info("redis cache connected",
		"addr", cfg.Addr,
		"ttl", ttl,
	)

	return &RedisCache{
		client: client,
		logger: logger,
		ttl:    ttl,
	}, nil
}

// CacheMetric stores a metric in Redis with TTL.
func (rc *RedisCache) CacheMetric(ctx context.Context, name, host string, value float64, tags map[string]string) error {
	key := fmt.Sprintf("%s%s:%s", MetricPrefix, name, host)
	entry := map[string]interface{}{
		"name":      name,
		"host":      host,
		"value":     value,
		"tags":      tags,
		"cached_at": time.Now().Unix(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal cache entry: %w", err)
	}

	return rc.client.Set(ctx, key, data, rc.ttl).Err()
}

// GetMetric retrieves a cached metric by name and host.
func (rc *RedisCache) GetMetric(ctx context.Context, name, host string) (map[string]interface{}, error) {
	key := fmt.Sprintf("%s%s:%s", MetricPrefix, name, host)
	data, err := rc.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // Cache miss
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal cache: %w", err)
	}
	return result, nil
}

// TrackRecentMetric adds a metric to the sorted set of recent metrics.
// Score is the timestamp, so we can query by time range.
func (rc *RedisCache) TrackRecentMetric(ctx context.Context, name string, value float64, ts int64) error {
	key := fmt.Sprintf("%s%s", RecentPrefix, name)
	member := redis.Z{
		Score:  float64(ts),
		Member: fmt.Sprintf("%d:%.6f", ts, value),
	}

	pipe := rc.client.Pipeline()
	pipe.ZAdd(ctx, key, member)
	pipe.Expire(ctx, key, 24*time.Hour)
	// Trim to last 10,000 entries
	pipe.ZRemRangeByRank(ctx, key, 0, -10001)
	_, err := pipe.Exec(ctx)
	return err
}

// GetRecentMetrics retrieves recent metric values within a time range.
func (rc *RedisCache) GetRecentMetrics(ctx context.Context, name string, from, to int64) ([]string, error) {
	key := fmt.Sprintf("%s%s", RecentPrefix, name)
	return rc.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", from),
		Max: fmt.Sprintf("%d", to),
	}).Result()
}

// IncrementCounter atomically increments a stats counter.
func (rc *RedisCache) IncrementCounter(ctx context.Context, stat string, delta int64) error {
	key := fmt.Sprintf("%s%s", StatsPrefix, stat)
	pipe := rc.client.Pipeline()
	pipe.IncrBy(ctx, key, delta)
	pipe.Expire(ctx, key, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// GetStats retrieves a stats counter value.
func (rc *RedisCache) GetStats(ctx context.Context, stat string) (int64, error) {
	key := fmt.Sprintf("%s%s", StatsPrefix, stat)
	val, err := rc.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// IsHealthy checks if Redis is reachable.
func (rc *RedisCache) IsHealthy(ctx context.Context) bool {
	return rc.client.Ping(ctx).Err() == nil
}

// Close closes the Redis connection.
func (rc *RedisCache) Close() error {
	return rc.client.Close()
}
