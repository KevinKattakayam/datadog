# Detector cardinality pressure

## Signals

- `DetectorCapacityHigh`: tracked detector series are above 80% of configured capacity for 10 minutes.
- `DetectorEvictionRateHigh`: LRU evictions exceed 10 per second for 10 minutes.
- Grafana `Cardinality and Cost`: top tenants and metrics by host/tag cardinality and estimated payload bytes.

## Triage

1. Check `processor_detector_series_tracked` and `processor_detector_series_capacity` on the `Cardinality and Cost` dashboard.
2. Review the ClickHouse attribution table for a tenant/metric with a sudden rise in distinct hosts or tag combinations.
3. Check for tags containing request IDs, full URLs, or other per-event values. Fix instrumentation before increasing capacity.
4. If the cardinality is expected and the processor has memory headroom, raise `PROCESSOR_DETECTOR_CAPACITY` and restart gradually.

The governor is report-only. It does not reject metric data. LRU eviction only resets the affected detector baseline; the source metric continues to ClickHouse. Estimated payload bytes are not disk usage or a billing figure, and may include duplicate replays.
