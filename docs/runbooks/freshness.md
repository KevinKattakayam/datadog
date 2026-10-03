# Runbook: pipeline freshness and stalled commits

Alerts: `PipelineFreshnessSlow`, `ProcessorCommitStalled`.

## What the signals mean

`pipeline_end_to_end_lag_seconds` is measured per record, from the Kafka
CreateTime the ingestor's producer stamps when it accepts a request, to the
moment the processor's offset commit covering that record succeeds. It is the
delay between a client receiving `202` and the row being durable in
ClickHouse.

A histogram only has samples when commits happen. If the processor stops
committing entirely, the freshness alert goes quiet rather than firing.
`ProcessorCommitStalled` covers that gap: it fires when
`processor_last_commit_timestamp_seconds` is more than two minutes old while
the consumer group still has lag.

Neither alert means data loss. Offsets are committed only after the
ClickHouse write, so a stalled processor leaves the backlog in Kafka.
Kafka's retention period (7 days in the local stack) is the real deadline.

## Triage

1. **Is ClickHouse healthy?** Check `processor_circuit_breaker_state`,
   `processor_clickhouse_write_errors_total` and
   `processor_clickhouse_timeouts_total`. If the circuit is open, follow
   [circuit-open.md](circuit-open.md).
2. **Are processors ready?** `kubectl get pods -l app=processor`. Not-ready
   with green liveness means the processor is running but cannot write.
3. **Is it a rebalance storm?** A rising
   `processor_rebalance_events_total` means members keep joining and leaving,
   for example a crash loop or a flush slower than `max.poll.interval.ms`.
   `processor_revoked_records_dropped_total` shows buffered work released to
   the next owner. That is safe, but it is repeated work.
4. **Is it just load?** If commits are happening (fresh
   `processor_last_commit_timestamp_seconds`) but p99 freshness is high, the
   processors are saturated. Check `kafka_consumergroup_lag_sum`. Scale up to
   the topic's partition count; the chart refuses to go higher.

## Recovery

When the cause is fixed, processors resume from the last committed offsets
and drain the backlog. Watch the lag fall and freshness return under the
threshold. No manual replay is needed.
