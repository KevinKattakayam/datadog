// Package config handles application configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration.
type Config struct {
	// Server
	Port     string
	LogLevel string

	// Kafka
	KafkaBootstrapServers string
	KafkaTopicRaw         string

	// Batching
	BatchSize     int
	FlushInterval time.Duration

	// Rate limiting
	RateLimitRPS int

	// Auth
	APIKeysPath          string
	AllowInsecureDevAuth bool

	// Graceful shutdown
	DrainDelay time.Duration
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	batchSize := getEnvInt("INGESTOR_BATCH_SIZE", 500)
	flushIntervalMS := getEnvInt("INGESTOR_FLUSH_INTERVAL_MS", 100)
	rateLimit := getEnvInt("INGESTOR_RATE_LIMIT_RPS", 10000)
	drainDelaySeconds := getEnvInt("INGESTOR_DRAIN_DELAY_SECONDS", 15)
	batchSize = clamp(batchSize, 1, 1000)
	flushIntervalMS = clamp(flushIntervalMS, 10, 60000)
	rateLimit = clamp(rateLimit, 1, 1000000)
	drainDelaySeconds = clamp(drainDelaySeconds, 1, 20)
	return &Config{
		Port:                  getEnv("INGESTOR_PORT", "8080"),
		LogLevel:              getEnv("INGESTOR_LOG_LEVEL", "info"),
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		KafkaTopicRaw:         getEnv("KAFKA_TOPIC_RAW", "metrics.raw"),
		BatchSize:             batchSize,
		FlushInterval:         time.Duration(flushIntervalMS) * time.Millisecond,
		RateLimitRPS:          rateLimit,
		APIKeysPath:           getEnv("API_KEYS_PATH", "/etc/secrets/api-keys"),
		AllowInsecureDevAuth:  getEnv("AUTH_ALLOW_INSECURE_DEV", "false") == "true",
		DrainDelay:            time.Duration(drainDelaySeconds) * time.Second,
	}
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
