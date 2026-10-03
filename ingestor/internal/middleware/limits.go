package middleware

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DefaultMaxBodyBytes bounds one request body. A full 1000-metric batch with
// 20 maximum-length tags each is roughly 4 MiB of JSON, so 5 MiB admits every
// valid batch and rejects anything larger before it is read into memory.
const DefaultMaxBodyBytes int64 = 5 << 20

var rateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ingestor_rate_limited_total",
	Help: "Requests refused by a per-tenant limit, by limit kind (requests or metrics).",
}, []string{"kind"})

var bodyTooLarge = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ingestor_body_too_large_total",
	Help: "Requests refused because the body exceeded the configured maximum.",
})

// RecordRateLimited counts a refusal by a limiter outside this package.
func RecordRateLimited(kind string) {
	rateLimited.WithLabelValues(kind).Inc()
}

// MaxBodyBytes refuses oversized requests. A declared Content-Length over the
// limit is refused immediately with 413; a chunked or lying body is cut off
// by http.MaxBytesReader at the limit, which handlers detect with
// IsBodyTooLarge. Without this, one client can make the server buffer an
// arbitrarily large JSON document.
func MaxBodyBytes(limit int64) gin.HandlerFunc {
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	return func(c *gin.Context) {
		if c.Request.ContentLength > limit {
			bodyTooLarge.Inc()
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
				"error": "request body too large",
				"limit": limit,
			})
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		c.Next()
	}
}

// IsBodyTooLarge reports whether a bind error came from MaxBodyBytes.
func IsBodyTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		bodyTooLarge.Inc()
		return true
	}
	return false
}
