// Enterprise Observability Pipeline — Rust Stream Processor
//
// Consumes metrics from Kafka, applies anomaly detection (EWMA + Z-score),
// writes processed data to ClickHouse, and fires alerts.

mod config;
mod consumer;
mod detector;
mod metrics;
mod model;
mod producer;
mod storage;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use http_body_util::Full;
use prometheus::Encoder;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::Config;
use crate::model::{AlertRow, MetricRow};
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

    info!("Starting Observability Pipeline Processor v1.0.0");

    // Load configuration
    let config = Arc::new(Config::from_env());
    info!(?config, "Configuration loaded");

    // Initialize Prometheus metrics
    metrics::init();

    // Create channels for ClickHouse writer
    let (metric_tx, metric_rx) = mpsc::channel::<MetricRow>(10000);
    let (alert_tx, alert_rx) = mpsc::channel::<AlertRow>(1000);

    // Initialize ClickHouse writer
    let ch_writer = Arc::new(ClickHouseWriter::new(
        &config.clickhouse_url,
        config.batch_size,
        config.flush_interval_ms,
    )?);

    // Initialize alert producer
    let alert_producer = Arc::new(AlertProducer::new(
        &config.kafka_brokers,
        &config.kafka_topic_alerts,
        &config.kafka_topic_processed,
    )?);

    // Spawn ClickHouse metric writer task
    let ch_metric = ch_writer.clone();
    tokio::spawn(async move {
        ch_metric.run_metric_writer(metric_rx).await;
    });

    // Spawn ClickHouse alert writer task
    let ch_alert = ch_writer.clone();
    tokio::spawn(async move {
        ch_alert.run_alert_writer(alert_rx).await;
    });

    // Spawn Prometheus metrics HTTP server
    let metrics_port = config.metrics_port;
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_port).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    info!(port = metrics_port, "Prometheus metrics server started");

    // Run the Kafka consumer (blocks)
    consumer::run(config, metric_tx, alert_tx, alert_producer).await?;

    Ok(())
}

/// Serve Prometheus metrics on an HTTP endpoint.
async fn serve_metrics(port: u16) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    info!(addr = %addr, "Metrics endpoint listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service_fn(handle_metrics))
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
        "/health" => Ok(Response::builder()
            .status(200)
            .body(Full::new(Bytes::from(r#"{"status":"healthy"}"#)))
            .unwrap()),
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}
