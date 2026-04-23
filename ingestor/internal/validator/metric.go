// Package validator provides metric payload validation logic.
package validator

import (
	"fmt"
	"strings"
	"time"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
)

// MaxTagKeys is the maximum number of tags allowed per metric.
const MaxTagKeys = 20

// MaxNameLength is the maximum length of a metric name.
const MaxNameLength = 256

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
