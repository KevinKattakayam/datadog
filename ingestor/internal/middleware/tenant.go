// Package middleware provides HTTP middleware for the ingestor.
// tenant.go implements multi-tenancy: tenant extraction from headers,
// per-tenant rate limiting, and Kafka partition key assignment.
package middleware

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// TenantExtractor extracts the tenant ID from the X-Tenant-ID header.
// If the header is missing, it defaults to DefaultTenantID ("default").
//
// NOT for production routing: the header is client-controlled. The server
// derives tenant identity from the API key (see APIKeyAuth). This exists for
// tests and for trusted internal hops only.
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
// The same type serves two purposes with different units: requests per
// second (middleware) and metrics per second (quota, charged per batch item).
type TenantRateLimiter struct {
	mu         sync.RWMutex
	buckets    map[string]*tokenBucket
	configs    map[string]TenantConfig
	defaultRPS int
	// burst is the bucket capacity. For a metrics quota it must be at least
	// the maximum batch size, or a full batch could never be admitted.
	burst int
}

type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func newTokenBucket(rate, burst int) *tokenBucket {
	return &tokenBucket{
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: float64(rate),
		lastRefill: time.Now(),
	}
}

// take removes n tokens if available. Otherwise it removes nothing and
// reports how long until n tokens will be available, for Retry-After.
// All state is mutated under tb.mu: the original version mutated tokens
// outside any lock, a data race under concurrent requests for one tenant.
func (tb *tokenBucket) take(n int) (bool, time.Duration) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = math.Min(tb.maxTokens, tb.tokens+elapsed*tb.refillRate)
	tb.lastRefill = now

	need := float64(n)
	if tb.tokens >= need {
		tb.tokens -= need
		return true, 0
	}
	if need > tb.maxTokens || tb.refillRate <= 0 {
		// Can never be satisfied by waiting; caller should split the batch.
		return false, -1
	}
	wait := (need - tb.tokens) / tb.refillRate
	return false, time.Duration(wait * float64(time.Second))
}

// NewTenantRateLimiter creates a multi-tenant request-rate limiter whose
// burst equals its per-second rate.
func NewTenantRateLimiter(defaultRPS int, configs map[string]TenantConfig) *TenantRateLimiter {
	if defaultRPS <= 0 {
		defaultRPS = DefaultTenantRPS
	}
	return NewTenantQuota(defaultRPS, defaultRPS, configs)
}

// NewTenantQuota creates a per-tenant limiter with an explicit burst
// capacity. Use it for metrics-per-second quotas: burst >= max batch size.
func NewTenantQuota(ratePerSec, burst int, configs map[string]TenantConfig) *TenantRateLimiter {
	if ratePerSec <= 0 {
		ratePerSec = DefaultTenantRPS
	}
	if burst < ratePerSec {
		burst = ratePerSec
	}
	if configs == nil {
		configs = make(map[string]TenantConfig)
	}
	return &TenantRateLimiter{
		buckets:    make(map[string]*tokenBucket),
		configs:    configs,
		defaultRPS: ratePerSec,
		burst:      burst,
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
		rate, burst := trl.defaultRPS, trl.burst
		if cfg, ok := trl.configs[tenantID]; ok && cfg.MaxRPS > 0 {
			rate = cfg.MaxRPS
			if burst < rate {
				burst = rate
			}
		}
		bucket = newTokenBucket(rate, burst)
		trl.buckets[tenantID] = bucket
	}
	return bucket
}

// AllowN charges n units to the tenant. When refused, retryAfter is the time
// until the request could succeed, or negative if it never can (n > burst).
func (trl *TenantRateLimiter) AllowN(tenantID string, n int) (bool, time.Duration) {
	if n <= 0 {
		return true, 0
	}
	return trl.getBucket(tenantID).take(n)
}

// SetRetryAfter writes a Retry-After header, rounded up to whole seconds
// (the header has one-second resolution; rounding down invites a retry
// storm that is refused again).
func SetRetryAfter(c *gin.Context, d time.Duration) {
	secs := int(math.Ceil(d.Seconds()))
	if secs < 1 {
		secs = 1
	}
	c.Header("Retry-After", strconv.Itoa(secs))
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

		ok, wait := limiter.getBucket(tid).take(1)
		if !ok {
			rateLimited.WithLabelValues("requests").Inc()
			SetRetryAfter(c, wait)
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
