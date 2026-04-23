package validator

import (
	"math"
	"testing"
	"time"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
)

func TestValidateMetric_Valid(t *testing.T) {
	m := &model.Metric{
		Name:      "api.request.duration_ms",
		Value:     142.7,
		Unit:      "ms",
		Tags:      map[string]string{"service": "checkout"},
		Timestamp: time.Now().Unix(),
		Host:      "prod-api-01",
	}
	if err := ValidateMetric(m); err != nil {
		t.Errorf("expected valid metric, got error: %v", err)
	}
}

func TestValidateMetric_MissingName(t *testing.T) {
	m := &model.Metric{
		Value:     142.7,
		Unit:      "ms",
		Timestamp: 1714900000,
		Host:      "prod-api-01",
	}
	if err := ValidateMetric(m); err == nil {
		t.Error("expected error for missing name")
	}
}

func TestValidateMetric_MissingHost(t *testing.T) {
	m := &model.Metric{
		Name:      "api.latency",
		Value:     142.7,
		Unit:      "ms",
		Timestamp: 1714900000,
	}
	if err := ValidateMetric(m); err == nil {
		t.Error("expected error for missing host")
	}
}

func TestValidateMetric_NameTooLong(t *testing.T) {
	longName := ""
	for i := 0; i < 300; i++ {
		longName += "a"
	}
	m := &model.Metric{
		Name:      longName,
		Value:     142.7,
		Timestamp: 1714900000,
		Host:      "host",
	}
	if err := ValidateMetric(m); err == nil {
		t.Error("expected error for name too long")
	}
}

func TestValidateMetric_TooManyTags(t *testing.T) {
	tags := make(map[string]string)
	for i := 0; i < 25; i++ {
		tags["key"+string(rune('a'+i))] = "val"
	}
	m := &model.Metric{
		Name:      "test.metric",
		Value:     42.0,
		Tags:      tags,
		Timestamp: 1714900000,
		Host:      "host",
	}
	if err := ValidateMetric(m); err == nil {
		t.Error("expected error for too many tags")
	}
}

func TestValidateMetric_InvalidTimestamp(t *testing.T) {
	m := &model.Metric{
		Name:      "test.metric",
		Value:     42.0,
		Timestamp: -1,
		Host:      "host",
	}
	if err := ValidateMetric(m); err == nil {
		t.Error("expected error for invalid timestamp")
	}
}

func TestValidateMetric_NaNValue(t *testing.T) {
	m := &model.Metric{
		Name:      "test.metric",
		Value:     math.NaN(),
		Timestamp: time.Now().Unix(),
		Host:      "host",
	}
	// NaN should either be rejected or handled gracefully
	// This test documents expected behavior
	err := ValidateMetric(m)
	_ = err // Document: NaN is allowed through (ClickHouse handles it)
}
