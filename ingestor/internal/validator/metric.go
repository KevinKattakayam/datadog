// Package validator provides metric payload validation logic.
package validator

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/KevinKattakayam/datadog/ingestor/internal/model"
)

const (
	// MaxTagKeys is the maximum number of tags allowed per metric.
	MaxTagKeys = 20

	// MaxNameLength is the maximum length of a metric name.
	MaxNameLength = 256

	// MaxTagKeyLength is the maximum length of a tag key.
	MaxTagKeyLength = 64

	// MaxTagValueLength is the maximum length of a tag value.
	// Unbounded tag values are how a single tenant turns a LowCardinality
	// column into a full dictionary scan. request_id as a tag is the classic mistake.
	MaxTagValueLength = 256

	// MaxHostLength per RFC 1035.
	MaxHostLength = 253
)

// ValidUnits are the accepted measurement units.
var ValidUnits = map[string]bool{
	"":        true, // optional
	"ms":      true,
	"s":       true,
	"bytes":   true,
	"kb":      true,
	"mb":      true,
	"gb":      true,
	"count":   true,
	"percent": true,
	"ops":     true,
	"req":     true,
}

// ValidateMetric checks that a metric conforms to the expected schema.
func ValidateMetric(m *model.Metric) error {
	// Name must be non-empty and dot-separated
	if m.Name == "" {
		return fmt.Errorf("metric name is required")
	}
	if len(m.Name) > MaxNameLength {
		return fmt.Errorf("metric name exceeds max length of %d", MaxNameLength)
	}
	if !isValidMetricName(m.Name) {
		return fmt.Errorf("metric name must be dot-separated alphanumeric: %s", m.Name)
	}

	// Host is required
	if m.Host == "" {
		return fmt.Errorf("host is required")
	}
	if len(m.Host) > MaxHostLength {
		return fmt.Errorf("host exceeds max length of %d", MaxHostLength)
	}
	if !isPrintable(m.Host) {
		return fmt.Errorf("host must be printable UTF-8 without control characters")
	}

	// Value must be finite (NaN and Inf poison downstream aggregations)
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
		return fmt.Errorf("value must be finite, got %v", m.Value)
	}

	// Timestamp must be reasonable (within last 24h to 1 minute in the future)
	now := time.Now().Unix()
	if m.Timestamp < now-86400 || m.Timestamp > now+60 {
		return fmt.Errorf("timestamp out of range: must be within last 24h")
	}

	// Unit validation
	if !ValidUnits[m.Unit] {
		return fmt.Errorf("invalid unit: %s", m.Unit)
	}

	// Tag count limit
	if len(m.Tags) > MaxTagKeys {
		return fmt.Errorf("too many tags: %d (max %d)", len(m.Tags), MaxTagKeys)
	}

	// Tag key/value length limits — prevents cardinality explosion from
	// tags like request_id or full URLs
	for k, v := range m.Tags {
		if len(k) > MaxTagKeyLength || len(v) > MaxTagValueLength {
			return fmt.Errorf("tag %q exceeds length limits (key max %d, value max %d)", k, MaxTagKeyLength, MaxTagValueLength)
		}
		if !isValidTagKey(k) {
			return fmt.Errorf("tag key %q must be non-empty and use only letters, digits, '_', '.', '-' or '/'", k)
		}
		if !isPrintable(v) {
			return fmt.Errorf("tag %q value must be printable UTF-8 without control characters", k)
		}
	}

	return nil
}

// isValidMetricName checks for dot-separated alphanumeric segments.
func isValidMetricName(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) < 1 {
		return false
	}
	for _, p := range parts {
		if p == "" {
			return false
		}
		for _, c := range p {
			if !isAlphanumericOrUnderscore(c) {
				return false
			}
		}
	}
	return true
}

func isAlphanumericOrUnderscore(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// isValidTagKey restricts keys to the charset that survives every downstream
// system unescaped: Prometheus-style labels, ClickHouse Map keys, Grafana
// legends, and URL query parameters.
func isValidTagKey(k string) bool {
	if k == "" {
		return false
	}
	for _, c := range k {
		if !isAlphanumericOrUnderscore(c) && c != '.' && c != '-' && c != '/' {
			return false
		}
	}
	return true
}

// isPrintable rejects invalid UTF-8 and control characters (newlines, NUL,
// ANSI escapes), which corrupt log lines and dashboards when echoed back.
func isPrintable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
