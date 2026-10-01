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

	// ── Middleware — order is load-bearing ──────────────────
	router.Use(gin.Recovery())
	router.Use(middleware.StructuredLogger(logger))
	router.Use(middleware.PrometheusMetrics())
	router.Use(middleware.TracingMiddleware()) // W3C Trace Context propagation

	// Auth first: it establishes the tenant identity everything downstream trusts.
	auth, err := middleware.NewAPIKeyAuth(cfg.APIKeysPath, cfg.AllowInsecureDevAuth, logger)
	if err != nil {
		logger.Error("failed to load API keys", "error", err)
		os.Exit(1)
	}
	// Per-tenant rate limiting (now actually registered)
	tenantLimiter := middleware.NewTenantRateLimiter(cfg.RateLimitRPS, nil)

	// ── Register Handlers ───────────────────────────────────
	ingestHandler := handler.NewIngestHandler(kafkaProducer, logger)
	healthHandler := handler.NewHealthHandler(kafkaProducer, logger)

	// Health endpoints (no rate limit needed — they are before the rate limit middleware in the group)
	router.GET("/health", healthHandler.Health)
	router.GET("/ready", healthHandler.Ready)

	// Prometheus metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Ingestion endpoints
	ingestRoutes := router.Group("")
	ingestRoutes.Use(auth.Authenticate(), middleware.TenantRateLimit(tenantLimiter))
	ingestRoutes.POST("/ingest", ingestHandler.IngestSingle)
	ingestRoutes.POST("/ingest/batch", ingestHandler.IngestBatch)

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

	// ── Graceful Shutdown — HTTP first, THEN flush ──────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", "signal", sig)

	// 1. Fail readiness so the endpoints controller pulls this pod out of the
	//    Service before it stops accepting. Sleep past the probe period.
	healthHandler.SetDraining(true)
	time.Sleep(cfg.DrainDelay) // >= readiness periodSeconds * failureThreshold

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	// 2. Stop accepting, let in-flight handlers finish. Those handlers are still
	//    producing to Kafka, which is exactly why this must precede the flush.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	// 3. Only now is the producer guaranteed to have no new work arriving.
	if err := kafkaProducer.Flush(shutdownCtx); err != nil {
		logger.Error("kafka flush error on shutdown", "error", err)
	}
	kafkaProducer.Close()

	logger.Info("ingestor shut down cleanly")
}
