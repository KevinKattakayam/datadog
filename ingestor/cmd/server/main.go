// Enterprise Observability Pipeline — Go Ingestor
//
// HTTP server that receives metric payloads, validates them,
// and publishes to Apache Kafka for downstream processing.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/KevinKattakayam/datadog/ingestor/config"
	"github.com/KevinKattakayam/datadog/ingestor/internal/handler"
	"github.com/KevinKattakayam/datadog/ingestor/internal/middleware"
	"github.com/KevinKattakayam/datadog/ingestor/internal/producer"
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
	defer kafkaProducer.Close() // idempotent; also runs on early return

	// ── Setup HTTP Router ───────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	// ── Middleware — order is load-bearing ──────────────────
	router.Use(gin.Recovery())
	router.Use(middleware.StructuredLogger(logger))
	router.Use(middleware.PrometheusMetrics())
	router.Use(middleware.TracingMiddleware()) // W3C Trace Context propagation

	// Auth establishes the tenant identity everything downstream trusts.
	auth, err := middleware.NewAPIKeyAuth(cfg.APIKeysPath, cfg.AllowInsecureDevAuth, logger)
	if err != nil {
		logger.Error("failed to load API keys", "error", err)
		os.Exit(1)
	}
	go auth.Watch(ctx, cfg.APIKeysReloadInterval)

	// Two independent per-tenant limits:
	//   - requests/sec, enforced before the body is read (cheap rejection);
	//   - metrics/sec, charged per batch item after parsing, so a client
	//     cannot multiply its throughput 1000x by batching.
	requestLimiter := middleware.NewTenantRateLimiter(cfg.RateLimitRPS, nil)
	metricQuota := middleware.NewTenantQuota(cfg.TenantMetricsPerSec, maxBatch, nil)

	// ── Register Handlers ───────────────────────────────────
	ingestHandler := handler.NewIngestHandler(kafkaProducer, metricQuota, logger)
	healthHandler := handler.NewHealthHandler(kafkaProducer, logger)

	// Operational endpoints: no auth, no tenant limits.
	router.GET("/health", healthHandler.Health)
	router.GET("/ready", healthHandler.Ready)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Ingestion endpoints. Body limit first so an oversized request is
	// refused before authentication does any work.
	ingestRoutes := router.Group("")
	ingestRoutes.Use(
		middleware.MaxBodyBytes(cfg.MaxBodyBytes),
		auth.Authenticate(),
		middleware.TenantRateLimit(requestLimiter),
	)
	ingestRoutes.POST("/ingest", ingestHandler.IngestSingle)
	ingestRoutes.POST("/ingest/batch", ingestHandler.IngestBatch)

	// ── Start HTTP Server ───────────────────────────────────
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%s", cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second, // slowloris guard
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// SIGHUP forces an immediate API key reload (in addition to polling).
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if applied, err := auth.Reload(); err != nil {
				logger.Warn("SIGHUP key reload failed; keeping current keys", "error", err)
			} else {
				logger.Info("SIGHUP key reload", "applied", applied)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-quit:
		logger.Info("shutdown signal received", "signal", sig)
	case err := <-serverErr:
		// Fall through to the same orderly shutdown instead of os.Exit,
		// which would skip the producer flush.
		logger.Error("HTTP server error", "error", err)
	}

	// ── Graceful Shutdown — readiness, then HTTP, then flush ──
	// 1. Fail readiness so the endpoints controller pulls this pod out of the
	//    Service before it stops accepting. Sleep past the probe period.
	healthHandler.SetDraining(true)
	time.Sleep(cfg.DrainDelay) // >= readiness periodSeconds * failureThreshold

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	// 2. Stop accepting, let in-flight handlers finish. Those handlers are
	//    still producing to Kafka, which is why this must precede the flush.
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

// maxBatch mirrors the binding limit on model.BatchRequest. The metrics quota
// burst must be at least this, or a full valid batch could never be admitted.
const maxBatch = 1000
