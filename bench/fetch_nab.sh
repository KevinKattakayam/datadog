#!/bin/bash
# Downloads the Numenta Anomaly Benchmark files used by the detector
# evaluation. NAB is AGPL-3.0, so the data is fetched on demand into a
# git-ignored directory and never committed.
#
#   bash bench/fetch_nab.sh            # -> .nab/
#   NAB_DIR=/tmp/nab bash bench/fetch_nab.sh
set -euo pipefail

NAB_DIR="${NAB_DIR:-.nab}"
BASE="https://raw.githubusercontent.com/numenta/NAB/master"
SERIES="nyc_taxi ec2_request_latency_system_failure cpu_utilization_asg_misconfiguration machine_temperature_system_failure ambient_temperature_system_failure rogue_agent_key_hold rogue_agent_key_updown"

mkdir -p "$NAB_DIR/data/realKnownCause" "$NAB_DIR/labels"
for s in $SERIES; do
    [ -s "$NAB_DIR/data/realKnownCause/$s.csv" ] || curl -fsSL -o "$NAB_DIR/data/realKnownCause/$s.csv" "$BASE/data/realKnownCause/$s.csv"
done
[ -s "$NAB_DIR/labels/combined_windows.json" ] || curl -fsSL -o "$NAB_DIR/labels/combined_windows.json" "$BASE/labels/combined_windows.json"
echo "NAB data ready in $NAB_DIR"
