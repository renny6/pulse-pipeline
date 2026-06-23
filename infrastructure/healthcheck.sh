#!/usr/bin/env bash
# ==============================================================================
# PULSE PIPELINE — Infrastructure Health Check Script
# infrastructure/healthcheck.sh
# ==============================================================================
# PURPOSE
# -------
# A single-command diagnostic script to verify that all Phase 1 infrastructure
# services are healthy and inter-connected before Phase 2 development begins.
#
# USAGE (from project root):
#   bash infrastructure/healthcheck.sh
#
# EXIT CODES:
#   0 — All services healthy
#   1 — One or more services are unhealthy or unreachable
# ==============================================================================

set -euo pipefail

PASS="✅"
FAIL="❌"
WARN="⚠️ "

OVERALL_STATUS=0

echo ""
echo "======================================================"
echo "  PULSE: Phase 1 Infrastructure Health Check"
echo "======================================================"
echo ""

# ------------------------------------------------------------------
# Helper: check if a container is in a healthy state
# ------------------------------------------------------------------
check_container() {
  local name="$1"
  local status

  status=$(docker inspect --format='{{.State.Health.Status}}' "${name}" 2>/dev/null || echo "not_found")

  case "${status}" in
    "healthy")
      echo "  ${PASS} ${name}: HEALTHY"
      ;;
    "starting")
      echo "  ${WARN} ${name}: STARTING (health checks pending — wait a moment)"
      OVERALL_STATUS=1
      ;;
    "unhealthy")
      echo "  ${FAIL} ${name}: UNHEALTHY"
      docker inspect --format='{{range .State.Health.Log}}  Log: {{.Output}}{{end}}' "${name}" 2>/dev/null | tail -3
      OVERALL_STATUS=1
      ;;
    "not_found"|"")
      echo "  ${FAIL} ${name}: NOT RUNNING"
      OVERALL_STATUS=1
      ;;
    *)
      echo "  ${WARN} ${name}: STATUS=${status}"
      ;;
  esac
}

echo "[ CONTAINER STATUS ]"
check_container "pulse-zookeeper"
check_container "pulse-kafka"
check_container "pulse-redis"
check_container "pulse-timescaledb"

echo ""
echo "[ CONNECTIVITY CHECKS ]"

# Zookeeper four-letter command
if docker exec pulse-zookeeper bash -c "echo ruok | nc -w 2 localhost 2181" 2>/dev/null | grep -q "imok"; then
  echo "  ${PASS} Zookeeper: ruok → imok"
else
  echo "  ${FAIL} Zookeeper: ruok check failed"
  OVERALL_STATUS=1
fi

# Redis PING
if docker exec pulse-redis redis-cli ping 2>/dev/null | grep -q "PONG"; then
  echo "  ${PASS} Redis: PING → PONG"
else
  echo "  ${FAIL} Redis: PING failed"
  OVERALL_STATUS=1
fi

# Kafka broker API versions check (internal listener)
if docker exec pulse-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
  echo "  ${PASS} Kafka: broker API versions reachable on kafka:9092"
else
  echo "  ${FAIL} Kafka: broker unreachable on kafka:9092"
  OVERALL_STATUS=1
fi

# TimescaleDB pg_isready check
if docker exec pulse-timescaledb pg_isready -U "${POSTGRES_USER:-pulse_admin}" -d "${POSTGRES_DB:-pulse_analytics}" > /dev/null 2>&1; then
  echo "  ${PASS} TimescaleDB: pg_isready passed"
else
  echo "  ${FAIL} TimescaleDB: pg_isready failed"
  OVERALL_STATUS=1
fi

echo ""
echo "[ KAFKA TOPICS ]"
if docker exec pulse-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "pulse.events.raw"; then
  echo "  ${PASS} Topic 'pulse.events.raw' exists"
else
  echo "  ${FAIL} Topic 'pulse.events.raw' NOT FOUND (kafka-init may not have completed)"
  OVERALL_STATUS=1
fi

if docker exec pulse-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "pulse.events.dlq"; then
  echo "  ${PASS} Topic 'pulse.events.dlq' exists"
else
  echo "  ${FAIL} Topic 'pulse.events.dlq' NOT FOUND"
  OVERALL_STATUS=1
fi

echo ""
echo "[ DATABASE SCHEMA ]"
TABLE_COUNT=$(docker exec pulse-timescaledb psql \
  -U "${POSTGRES_USER:-pulse_admin}" \
  -d "${POSTGRES_DB:-pulse_analytics}" \
  -t -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('ingested_events','dead_letter_queue');" 2>/dev/null | tr -d ' \n' || echo "0")

if [ "${TABLE_COUNT}" = "2" ]; then
  echo "  ${PASS} Tables: ingested_events + dead_letter_queue present"
else
  echo "  ${FAIL} Tables missing (found ${TABLE_COUNT}/2) — check init-db.sql logs"
  OVERALL_STATUS=1
fi

# Check TimescaleDB hypertable
HYPER_COUNT=$(docker exec pulse-timescaledb psql \
  -U "${POSTGRES_USER:-pulse_admin}" \
  -d "${POSTGRES_DB:-pulse_analytics}" \
  -t -c "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name='ingested_events';" 2>/dev/null | tr -d ' \n' || echo "0")

if [ "${HYPER_COUNT}" = "1" ]; then
  echo "  ${PASS} TimescaleDB: ingested_events is a hypertable"
else
  echo "  ${FAIL} TimescaleDB: ingested_events hypertable not found"
  OVERALL_STATUS=1
fi

echo ""
echo "======================================================"
if [ "${OVERALL_STATUS}" -eq 0 ]; then
  echo "  ${PASS} ALL SYSTEMS GO — Phase 1 infrastructure is healthy."
  echo "  Ready to proceed to Phase 2: Gateway & Global Guard."
else
  echo "  ${FAIL} DEGRADED — One or more checks failed. Review above."
  echo "  Tip: Run 'docker compose logs <service>' for details."
fi
echo "======================================================"
echo ""

exit "${OVERALL_STATUS}"
