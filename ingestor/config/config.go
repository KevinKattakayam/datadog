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
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		Port:                  getEnv("INGESTOR_PORT", "8080"),
		LogLevel:              getEnv("INGESTOR_LOG_LEVEL", "info"),
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		KafkaTopicRaw:         getEnv("KAFKA_TOPIC_RAW", "metrics.raw"),
		BatchSize:             getEnvInt("INGESTOR_BATCH_SIZE", 500),
		FlushInterval:         time.Duration(getEnvInt("INGESTOR_FLUSH_INTERVAL_MS", 100)) * time.Millisecond,
		RateLimitRPS:          getEnvInt("INGESTOR_RATE_LIMIT_RPS", 10000),
	}
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
