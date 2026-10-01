// Package producer provides Kafka publishing functionality using franz-go.
package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Kevinbastin/observability-pipeline/ingestor/internal/model"
)

// KafkaProducer wraps franz-go client for metric publishing.
// Synchronous produce with acks=all and idempotency — 202 means "in Kafka".
type KafkaProducer struct {
	client *kgo.Client
	topic  string
	logger *slog.Logger
}

// New creates a new KafkaProducer with synchronous, durable produce semantics.
func New(ctx context.Context, brokers, topic string, batchSize int,
	linger time.Duration, logger *slog.Logger) (*KafkaProducer, error) {

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
		// Durability: every in-sync replica must ack.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// franz-go enables the idempotent producer by default; stating it
		// documents the intent so nobody "optimises" it away later.
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerBatchMaxBytes(1_048_576),
		kgo.ProducerLinger(linger),
		kgo.MaxBufferedRecords(batchSize*4),
		kgo.RecordRetries(5),
		kgo.RequestTimeoutOverhead(10*time.Second),
		// NOTE: AllowAutoTopicCreation removed. Auto-created topics get the
		// broker defaults (RF=1, 1 partition) and silently defeat the
		// partitioning and replication this design depends on.
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect kafka %s: %w", brokers, err)
	}
	return &KafkaProducer{client: client, topic: topic, logger: logger}, nil
}

// Publish blocks until the record is acknowledged by all in-sync replicas.
// An error means the metric is NOT in Kafka and the caller must not report
// success to the client.
func (p *KafkaProducer) Publish(ctx context.Context, tenantID string, m *model.Metric) error {
	rec, err := p.record(tenantID, m)
	if err != nil {
		return err
	}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		publishErrors.WithLabelValues(classify(err)).Inc()
		return fmt.Errorf("produce: %w", err)
	}
	messagesPublished.Inc()
	return nil
}

// PublishBatch produces all records in one round-trip and reports exactly
// how many were acknowledged.
func (p *KafkaProducer) PublishBatch(ctx context.Context, tenantID string,
	metrics []model.Metric) (int, error) {

	recs := make([]*kgo.Record, 0, len(metrics))
	for i := range metrics {
		rec, err := p.record(tenantID, &metrics[i])
		if err != nil {
			return 0, err
		}
		recs = append(recs, rec)
	}

	acked, firstErr := 0, error(nil)
	for _, r := range p.client.ProduceSync(ctx, recs...) {
		if r.Err != nil {
			publishErrors.WithLabelValues(classify(r.Err)).Inc()
			if firstErr == nil {
				firstErr = r.Err
			}
			continue
		}
		acked++
	}
	messagesPublished.Add(float64(acked))
	return acked, firstErr
}

// record builds the Kafka record. Key is tenant|host: tenant gives isolation
// and locality, host spreads a large tenant across partitions instead of
// creating one hot partition per customer.
func (p *KafkaProducer) record(tenantID string, m *model.Metric) (*kgo.Record, error) {
	m.TenantID = tenantID
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal metric %q: %w", m.Name, err)
	}
	return &kgo.Record{
		Key:   []byte(tenantID + "|" + m.Host),
		Value: data,
		Topic: p.topic,
		Headers: []kgo.RecordHeader{
			{Key: "tenant_id", Value: []byte(tenantID)},
			{Key: "schema_version", Value: []byte("1")},
		},
	}, nil
}

func classify(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, kerr.NotLeaderForPartition):
		return "not_leader"
	case errors.Is(err, kerr.NotEnoughReplicas):
		return "insufficient_replicas"
	default:
		return "other"
	}
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
