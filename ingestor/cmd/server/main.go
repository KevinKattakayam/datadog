// Enterprise Observability Pipeline — Go Ingestor
//
// HTTP server that receives metric payloads, validates them,
// and publishes to Apache Kafka for downstream processing.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Kevinbastin/observability-pipeline/ingestor/config"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/handler"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/middleware"
	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/producer"
)

func main() {
	// ── Load Configuration ──────────────────────────────────
	cfg := config.Load()

	// ── Setup Structured Logger ─────────────────────────────
	logLevel := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	logger.Info("starting observability pipeline ingestor",
		"port", cfg.Port,
		"kafka_brokers", cfg.KafkaBootstrapServers,
		"kafka_topic", cfg.KafkaTopicRaw,
	)

	// ── Initialize Kafka Producer ───────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaProducer, err := producer.New(
		ctx,
		cfg.KafkaBootstrapServers,
		cfg.KafkaTopicRaw,
		cfg.BatchSize,
		cfg.FlushInterval,
		logger,
	)
	if err != nil {
		logger.Error("failed to initialize kafka producer", "error", err)
		os.Exit(1)
	}
	defer kafkaProducer.Close()

	// ── Setup HTTP Router ───────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	// Global middleware
	router.Use(gin.Recovery())
	router.Use(middleware.StructuredLogger(logger))
	router.Use(middleware.PrometheusMetrics())
	router.Use(middleware.RateLimit(cfg.RateLimitRPS))
	router.Use(middleware.TracingMiddleware()) // W3C Trace Context propagation

	// ── Register Handlers ───────────────────────────────────
	ingestHandler := handler.NewIngestHandler(kafkaProducer, logger)
	healthHandler := handler.NewHealthHandler(kafkaProducer, logger)

	// Health endpoints (no rate limit needed)
	router.GET("/health", healthHandler.Health)
	router.GET("/ready", healthHandler.Ready)

	// Prometheus metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Ingestion endpoints
	router.POST("/ingest", ingestHandler.IngestSingle)
	router.POST("/ingest/batch", ingestHandler.IngestBatch)

	// ── Start HTTP Server ───────────────────────────────────
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		logger.Info("HTTP server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// ── Graceful Shutdown ───────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", "signal", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	// Flush Kafka producer
	if err := kafkaProducer.Flush(shutdownCtx); err != nil {
		logger.Error("kafka flush error on shutdown", "error", err)
	}

	// Shutdown HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("ingestor shut down gracefully")
}
