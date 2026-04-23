// Package tracing provides W3C Trace Context propagation middleware.
// This enables distributed tracing across the pipeline by extracting
// and injecting traceparent/tracestate headers.
//
// When the Go ingestor publishes to Kafka, the trace context is
// propagated via Kafka record headers, allowing the Rust processor
// to continue the trace span.
package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	// W3C Trace Context header names
	TraceparentHeader = "traceparent"
	TracestateHeader  = "tracestate"
)

// TraceContext represents a W3C Trace Context.
type TraceContext struct {
	Version    string
	TraceID    string
	SpanID     string
	TraceFlags string
	TraceState string
}

// String returns the traceparent header value.
func (tc TraceContext) String() string {
	return fmt.Sprintf("%s-%s-%s-%s", tc.Version, tc.TraceID, tc.SpanID, tc.TraceFlags)
}

// ParseTraceparent parses a W3C traceparent header.
// Format: version-traceid-parentid-traceflags (e.g., "00-abc123...-def456...-01")
func ParseTraceparent(header string) (TraceContext, bool) {
	parts := strings.Split(header, "-")
	if len(parts) != 4 {
		return TraceContext{}, false
	}
	if len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return TraceContext{}, false
	}
	return TraceContext{
		Version:    parts[0],
		TraceID:    parts[1],
		SpanID:     parts[2],
		TraceFlags: parts[3],
	}, true
}

// NewTraceContext generates a new trace context with random IDs.
func NewTraceContext() TraceContext {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	rand.Read(traceID)
	rand.Read(spanID)

	return TraceContext{
		Version:    "00",
		TraceID:    hex.EncodeToString(traceID),
		SpanID:     hex.EncodeToString(spanID),
		TraceFlags: "01", // sampled
	}
}

// NewChildSpan creates a child span from an existing trace context.
func (tc TraceContext) NewChildSpan() TraceContext {
	spanID := make([]byte, 8)
	rand.Read(spanID)

	return TraceContext{
		Version:    tc.Version,
		TraceID:    tc.TraceID,
		SpanID:     hex.EncodeToString(spanID),
		TraceFlags: tc.TraceFlags,
		TraceState: tc.TraceState,
	}
}

// Middleware returns a gin middleware that extracts or creates W3C trace context.
// The trace context is stored in the gin context for downstream handlers.
func TracingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		var tc TraceContext

		// Extract existing trace context from request headers
		if traceparent := c.GetHeader(TraceparentHeader); traceparent != "" {
			parsed, ok := ParseTraceparent(traceparent)
			if ok {
				// Create child span
				tc = parsed.NewChildSpan()
				tc.TraceState = c.GetHeader(TracestateHeader)
			} else {
				tc = NewTraceContext()
			}
		} else {
			// No incoming trace — create new root trace
			tc = NewTraceContext()
		}

		// Store in context for downstream use
		c.Set("trace_context", tc)
		c.Set("trace_id", tc.TraceID)
		c.Set("span_id", tc.SpanID)

		// Set response headers for trace propagation
		c.Header(TraceparentHeader, tc.String())
		if tc.TraceState != "" {
			c.Header(TracestateHeader, tc.TraceState)
		}

		c.Next()
	}
}

// GetTraceContext retrieves the trace context from a gin context.
func GetTraceContext(c *gin.Context) (TraceContext, bool) {
	val, exists := c.Get("trace_context")
	if !exists {
		return TraceContext{}, false
	}
	tc, ok := val.(TraceContext)
	return tc, ok
}

// InjectHTTPHeaders adds trace context headers to an outgoing HTTP request.
func InjectHTTPHeaders(tc TraceContext, req *http.Request) {
	req.Header.Set(TraceparentHeader, tc.String())
	if tc.TraceState != "" {
		req.Header.Set(TracestateHeader, tc.TraceState)
	}
}

// KafkaHeaders returns trace context as key-value pairs for Kafka record headers.
func KafkaHeaders(tc TraceContext) map[string]string {
	headers := map[string]string{
		TraceparentHeader: tc.String(),
	}
	if tc.TraceState != "" {
		headers[TracestateHeader] = tc.TraceState
	}
	return headers
}
