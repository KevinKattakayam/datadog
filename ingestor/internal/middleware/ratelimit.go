package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
)

// RateLimiter implements a token bucket rate limiter.
type RateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// NewRateLimiter creates a rate limiter with the given requests-per-second limit.
func NewRateLimiter(rps int) *RateLimiter {
	return &RateLimiter{
		tokens:     float64(rps),
		maxTokens:  float64(rps),
		refillRate: float64(rps),
		lastRefill: time.Now(),
	}
}

// Allow checks if a request should be allowed.
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * rl.refillRate
	if rl.tokens > rl.maxTokens {
		rl.tokens = rl.maxTokens
	}
	rl.lastRefill = now

	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}

// RateLimit returns a gin middleware that enforces rate limiting.
func RateLimit(rps int) gin.HandlerFunc {
	limiter := NewRateLimiter(rps)

	return func(c *gin.Context) {
		if !limiter.Allow() {
			c.JSON(http.StatusTooManyRequests, model.ErrorResponse{
				Error: "rate limit exceeded",
				Code:  http.StatusTooManyRequests,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
