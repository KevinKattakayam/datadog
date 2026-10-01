// Enterprise Observability Pipeline — Rust Stream Processor
//
// Consumes metrics from Kafka, applies anomaly detection (EWMA + Z-score),
// writes processed data to ClickHouse, and fires alerts.
//
// Architecture: the consumer owns batching, persistence and offset commits
// in one loop. There is no in-memory queue between "message consumed" and
// "offset committed", so there is no window in which a crash loses data.

mod config;
mod consumer;
mod detector;
mod dlq;
mod metrics;
mod model;
mod producer;
mod storage;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use prometheus::Encoder;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info};

use crate::config::Config;
use crate::consumer::ConsumerLoop;
use crate::dlq::DlqProducer;
use crate::producer::AlertProducer;
use crate::storage::clickhouse::ClickHouseWriter;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing (structured JSON logging)
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting Observability Pipeline Processor v2.0.0");

    // Load configuration
    let config = Arc::new(Config::from_env());
    info!(
        kafka_brokers = %config.kafka_brokers,
        raw_topic = %config.kafka_topic_raw,
        consumer_group = %config.kafka_consumer_group,
        metrics_port = config.metrics_port,
        "Configuration loaded"
    );

    // Initialize Prometheus metrics
    metrics::init();
    metrics::DETECTOR_SERIES_CAPACITY.set(config.detector_capacity as f64);

    // Initialize ClickHouse writer (no more mpsc channel — consumer owns the path)
    let ch_writer = Arc::new(ClickHouseWriter::new(
        &config.clickhouse_url,
        "observability",
        &config.clickhouse_user,
        &config.clickhouse_password,
        config.max_write_attempts,
    )?);

    // Initialize alert producer
    let alert_producer = Arc::new(AlertProducer::new(
        &config.kafka_brokers,
        &config.kafka_topic_alerts,
        &config.kafka_topic_processed,
    )?);

    // Initialize DLQ producer
    let dlq_producer = Arc::new(DlqProducer::new(
        &config.kafka_brokers,
        &config.kafka_topic_dlq,
    )?);

    // Shutdown signal: SIGTERM/SIGINT → drain and exit
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn Prometheus metrics + health HTTP server
    let metrics_port = config.metrics_port;
    let writer_for_health = ch_writer.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_port, writer_for_health).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    info!(port = metrics_port, "Prometheus metrics server started");

    // Spawn SIGTERM handler
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("failed to register SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv() => info!("received SIGINT"),
        }
        let _ = shutdown_tx.send(true);
    });

    // Create and run the consumer loop (blocks until shutdown)
    let mut consumer_loop =
        ConsumerLoop::new(config.clone(), ch_writer, alert_producer, dlq_producer)?;

    consumer_loop.run(shutdown_rx).await?;

    info!("Processor shut down cleanly");
    Ok(())
}

/// Serve Prometheus metrics and health/readiness on HTTP.
async fn serve_metrics(port: u16, writer: Arc<ClickHouseWriter>) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    info!(addr = %addr, "Metrics endpoint listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let writer = writer.clone();

        tokio::spawn(async move {
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        let w = writer.clone();
                        handle_metrics(req, w)
                    }),
                )
                .await
            {
                error!(error = %e, "Metrics connection error");
            }
        });
    }
}

/// Handle metrics HTTP requests.
async fn handle_metrics(
    req: Request<hyper::body::Incoming>,
    writer: Arc<ClickHouseWriter>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    match req.uri().path() {
        "/metrics" => {
            let encoder = prometheus::TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();

            Ok(Response::builder()
                .header("Content-Type", encoder.format_type())
                .body(Full::new(Bytes::from(buffer)))
                .unwrap())
        }
        "/health" => {
            // Liveness: process is running
            Ok(Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(r#"{"status":"alive"}"#)))
                .unwrap())
        }
        "/ready" => {
            // Readiness: can reach ClickHouse, circuit closed
            let ch_ok = writer.health_check().await;
            let circuit = writer.circuit_state();
            let closed = circuit == "closed";

            if ch_ok && closed {
                Ok(Response::builder()
                    .status(200)
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(r#"{"status":"ready"}"#)))
                    .unwrap())
            } else {
                let body = format!(
                    r#"{{"status":"not_ready","clickhouse":{},"circuit":"{}"}}"#,
                    ch_ok, circuit
                );
                Ok(Response::builder()
                    .status(503)
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap())
            }
        }
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}
