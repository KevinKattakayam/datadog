// Package middleware provides HTTP middleware for the ingestor.
// tenant.go implements multi-tenancy: tenant extraction from headers,
// per-tenant rate limiting, and Kafka partition key assignment.
package middleware

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// TenantExtractor extracts the tenant ID from the X-Tenant-ID header.
// If the header is missing, it defaults to DefaultTenantID ("default").
func TenantExtractor() gin.HandlerFunc {
	return func(c *gin.Context) {
		tenantID := c.GetHeader(TenantHeaderKey)
		if tenantID == "" {
			tenantID = DefaultTenantID
		} else {
			tenantID = strings.ToLower(strings.TrimSpace(tenantID))
		}
		c.Set(TenantContextKey, tenantID)
		c.Next()
	}
}

const (
	// TenantHeaderKey is the HTTP header for tenant identification.
	TenantHeaderKey = "X-Tenant-ID"

	// TenantContextKey stores the tenant ID in gin context.
	TenantContextKey = "tenant_id"

	// DefaultTenantID is used when no tenant header is provided.
	DefaultTenantID = "default"

	// DefaultTenantRPS is the default per-tenant rate limit.
	DefaultTenantRPS = 1000
)

// TenantConfig holds per-tenant configuration.
type TenantConfig struct {
	// MaxRPS is the maximum requests per second for this tenant.
	MaxRPS int
	// Enabled indicates if the tenant is active.
	Enabled bool
}

// TenantRateLimiter provides per-tenant rate limiting using token buckets.
type TenantRateLimiter struct {
	mu         sync.RWMutex
	buckets    map[string]*tokenBucket
	configs    map[string]TenantConfig
	defaultRPS int
}

type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func newTokenBucket(rps int) *tokenBucket {
	return &tokenBucket{
		tokens:     float64(rps),
		maxTokens:  float64(rps),
		refillRate: float64(rps),
		lastRefill: time.Now(),
	}
}

func (tb *tokenBucket) allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// NewTenantRateLimiter creates a multi-tenant rate limiter.
func NewTenantRateLimiter(defaultRPS int, configs map[string]TenantConfig) *TenantRateLimiter {
	if defaultRPS <= 0 {
		defaultRPS = DefaultTenantRPS
	}
	if configs == nil {
		configs = make(map[string]TenantConfig)
	}
	return &TenantRateLimiter{
		buckets:    make(map[string]*tokenBucket),
		configs:    configs,
		defaultRPS: defaultRPS,
	}
}

func (trl *TenantRateLimiter) getBucket(tenantID string) *tokenBucket {
	// Read-mostly path: most requests hit an existing bucket
	trl.mu.RLock()
	bucket, exists := trl.buckets[tenantID]
	trl.mu.RUnlock()
	if exists {
		return bucket
	}

	// Write path: create a new bucket
	trl.mu.Lock()
	defer trl.mu.Unlock()
	// Double-check after acquiring write lock
	if bucket, exists = trl.buckets[tenantID]; !exists {
		rps := trl.defaultRPS
		if cfg, ok := trl.configs[tenantID]; ok {
			rps = cfg.MaxRPS
		}
		bucket = newTokenBucket(rps)
		trl.buckets[tenantID] = bucket
	}
	return bucket
}

// TenantRateLimit returns gin middleware that applies per-tenant rate limiting.
// Each tenant gets an independent token bucket with configurable RPS.
func TenantRateLimit(limiter *TenantRateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		tenantID, exists := c.Get(TenantContextKey)
		if !exists {
			tenantID = DefaultTenantID
		}

		tid := tenantID.(string)

		// Check if tenant is explicitly disabled
		if cfg, ok := limiter.configs[tid]; ok && !cfg.Enabled {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error":  "tenant disabled",
				"tenant": tid,
			})
			return
		}

		bucket := limiter.getBucket(tid)
		if !bucket.allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":  "tenant rate limit exceeded",
				"tenant": tid,
			})
			return
		}

		c.Next()
	}
}

// GetTenantID extracts the tenant ID from gin context.
// Used by handlers to set the Kafka partition key.
func GetTenantID(c *gin.Context) string {
	if tid, exists := c.Get(TenantContextKey); exists {
		return tid.(string)
	}
	return DefaultTenantID
}
