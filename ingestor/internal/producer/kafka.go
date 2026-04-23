// Package producer provides Kafka publishing functionality using franz-go.
package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
)

// KafkaProducer wraps franz-go client for metric publishing.
type KafkaProducer struct {
	client        *kgo.Client
	topic         string
	batchSize     int
	flushInterval time.Duration
	logger        *slog.Logger

	mu      sync.Mutex
	buffer  []*kgo.Record
	closeCh chan struct{}
}

// New creates a new KafkaProducer with batching support.
func New(ctx context.Context, brokers string, topic string, batchSize int, flushInterval time.Duration, logger *slog.Logger) (*KafkaProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(1048576), // 1MB max batch
		kgo.ProducerLinger(flushInterval),
		kgo.MaxBufferedRecords(batchSize*2),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	// Verify connectivity
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to kafka at %s: %w", brokers, err)
	}

	p := &KafkaProducer{
		client:        client,
		topic:         topic,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
		buffer:        make([]*kgo.Record, 0, batchSize),
		closeCh:       make(chan struct{}),
	}

	logger.Info("kafka producer connected",
		"brokers", brokers,
		"topic", topic,
		"batch_size", batchSize,
		"flush_interval", flushInterval,
	)

	return p, nil
}

// Publish serializes a metric and sends it to Kafka.
// The metric host is used as the partition key for locality.
func (p *KafkaProducer) Publish(ctx context.Context, metric *model.Metric) error {
	data, err := json.Marshal(metric)
	if err != nil {
		return fmt.Errorf("failed to marshal metric: %w", err)
	}

	record := &kgo.Record{
		Key:   []byte(metric.Host),
		Value: data,
		Topic: p.topic,
	}

	// Use background context — the HTTP request context may cancel before
	// the async produce callback fires, causing spurious "context canceled" errors.
	p.client.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			p.logger.Error("failed to publish metric",
				"error", err,
				"metric", metric.Name,
				"host", metric.Host,
			)
			publishErrors.Inc()
		} else {
			messagesPublished.Inc()
			p.logger.Debug("metric published",
				"topic", r.Topic,
				"partition", r.Partition,
				"offset", r.Offset,
			)
		}
	})

	return nil
}

// PublishBatch publishes multiple metrics efficiently.
func (p *KafkaProducer) PublishBatch(ctx context.Context, metrics []model.Metric) (int, error) {
	published := 0
	for i := range metrics {
		if err := p.Publish(ctx, &metrics[i]); err != nil {
			p.logger.Error("batch publish error", "index", i, "error", err)
			continue
		}
		published++
	}
	return published, nil
}

// Flush forces any buffered records to be sent.
func (p *KafkaProducer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

// Close gracefully shuts down the producer.
func (p *KafkaProducer) Close() {
	p.logger.Info("shutting down kafka producer")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = p.client.Flush(ctx)
	p.client.Close()
}

// IsHealthy checks if the producer can reach Kafka.
func (p *KafkaProducer) IsHealthy(ctx context.Context) bool {
	return p.client.Ping(ctx) == nil
}
