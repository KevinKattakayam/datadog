# Runbook: DLQ Rate High

## What fired

`DLQRateHigh`: Messages entering the dead-letter queue (`metrics.dlq`) at > 1/s for 5 minutes, grouped by reason.

## What it means

The processor is receiving messages it cannot parse. This is always a producer-side problem — the DLQ preserves the original bytes so they can be replayed after a fix.

Common causes by reason code:
- **`deserialize_error`**: Malformed JSON. A producer is sending invalid payloads.
- **`empty_payload`**: Kafka records with no value. Usually a producer bug.
- **`schema_rejected`**: Validation failure (wrong field types, missing required fields).
- **`value_not_finite`**: NaN or Infinity values. The producer is sending bad math.

## Diagnostic commands

```bash
# Inspect DLQ messages with headers
docker exec obs-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic metrics.dlq \
  --from-beginning --max-messages 5 \
  --property print.headers=true \
  --property print.key=true

# Check the rate by reason
curl -s http://localhost:9091/metrics | grep dlq_messages_total

# Query the DLQ events table for analysis
curl -s "http://localhost:8123/?query=SELECT+reason,count()+AS+cnt,min(ts)+AS+first,max(ts)+AS+last+FROM+observability.dlq_events+GROUP+BY+reason+ORDER+BY+cnt+DESC"
```

## Resolution

1. **Identify the producer.** The `dlq.source_partition` and `dlq.source_offset` headers in the DLQ message tell you which partition and offset the bad message came from. Correlate with producer logs.
2. **Fix the producer.** The most common fix is correcting the serialization format.
3. **Replay if needed.** The DLQ preserves the original bytes. After fixing the producer, you can republish corrected messages to `metrics.raw`.
4. **If the rate is very high (>100/s):** Something fundamental is wrong. Check if a new producer version was deployed that changed the message format.

## After recovery

- `processor_dlq_messages_total` rate should drop to near zero
- DLQ topic stops growing
