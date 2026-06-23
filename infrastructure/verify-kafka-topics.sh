#!/usr/bin/env bash
# ==============================================================================
# PULSE PIPELINE — Kafka Topic Verification Script
# infrastructure/verify-kafka-topics.sh
# ==============================================================================
# PURPOSE
# -------
# A diagnostic utility to verify that the kafka-init service correctly created
# the required Kafka topics. Run this from the host after `docker compose up`.
#
# USAGE (from project root):
#   bash infrastructure/verify-kafka-topics.sh
#
# This script is for INFORMATIONAL purposes only — it does not modify state.
# ==============================================================================

set -euo pipefail

KAFKA_CONTAINER="pulse-kafka"
BOOTSTRAP="kafka:9092"

echo ""
echo "======================================================"
echo "  PULSE: Kafka Topic Verification"
echo "======================================================"
echo ""

# Check that the kafka container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
  echo "ERROR: Container '${KAFKA_CONTAINER}' is not running."
  echo "       Run: docker compose up -d"
  exit 1
fi

echo "--> Listing all topics on broker (${BOOTSTRAP}):"
docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --list

echo ""
echo "--> Topic details:"
docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe \
  --topic pulse.events.raw 2>/dev/null || echo "  [WARN] pulse.events.raw not found"

docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe \
  --topic pulse.events.dlq 2>/dev/null || echo "  [WARN] pulse.events.dlq not found"

echo ""
echo "======================================================"
echo "  Verification complete."
echo "======================================================"
echo ""
