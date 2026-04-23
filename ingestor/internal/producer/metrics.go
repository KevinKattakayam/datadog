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

	publishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_publish_errors_total",
		Help: "Total number of Kafka publish failures.",
	})
)
