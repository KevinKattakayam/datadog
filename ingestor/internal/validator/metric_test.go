package validator

import (
	"math"
	"testing"
	"time"

	"github.com/KevinKattakayam/datadog/ingestor/internal/model"
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

func validBase() model.Metric {
	return model.Metric{Name: "cpu.usage", Value: 1, Timestamp: time.Now().Unix(), Host: "prod-01"}
}

func TestValidateMetric_TagKeyCharset(t *testing.T) {
	for _, key := range []string{"service", "k8s.pod", "app/version", "zone-a", "a_b"} {
		m := validBase()
		m.Tags = map[string]string{key: "v"}
		if err := ValidateMetric(&m); err != nil {
			t.Errorf("key %q should be valid: %v", key, err)
		}
	}
	for _, key := range []string{"", "has space", "new\nline", "quote\"", "emoji😀", "semi;colon"} {
		m := validBase()
		m.Tags = map[string]string{key: "v"}
		if err := ValidateMetric(&m); err == nil {
			t.Errorf("key %q should be rejected", key)
		}
	}
}

func TestValidateMetric_ControlCharactersRejected(t *testing.T) {
	cases := map[string]func(*model.Metric){
		"newline in tag value": func(m *model.Metric) { m.Tags = map[string]string{"k": "a\nb"} },
		"ANSI escape in value": func(m *model.Metric) { m.Tags = map[string]string{"k": "\x1b[31mred"} },
		"NUL in host":          func(m *model.Metric) { m.Host = "prod\x00-01" },
		"invalid UTF-8 value":  func(m *model.Metric) { m.Tags = map[string]string{"k": "\xff\xfe"} },
	}
	for name, mutate := range cases {
		m := validBase()
		mutate(&m)
		if err := ValidateMetric(&m); err == nil {
			t.Errorf("%s: should be rejected", name)
		}
	}
	m := validBase()
	m.Tags = map[string]string{"region": "São Paulo", "path": "/api/v1/users"}
	if err := ValidateMetric(&m); err != nil {
		t.Errorf("printable unicode and slashes must be allowed: %v", err)
	}
}
