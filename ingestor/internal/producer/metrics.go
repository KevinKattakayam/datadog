package producer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messagesPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_kafka_messages_published_total",
		Help: "Total number of messages successfully published to Kafka.",
	})

	publishErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingestor_publish_errors_total",
		Help: "Total number of Kafka publish failures.",
	}, []string{"reason"})
)

func init() {
	// CounterVec children do not appear in /metrics until first use. Publish
	// the supported error classes at zero so dashboards can distinguish a
	// healthy producer from a missing metric family.
	for _, reason := range []string{"timeout", "not_leader", "insufficient_replicas", "other"} {
		publishErrors.WithLabelValues(reason)
	}
}
