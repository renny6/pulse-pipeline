# 🚨 OPERATIONS, OBSERVABILITY & COMPLIANCE MANDATES

## Context Directive for AI Code Generation
You are assisting in building the operational foundation of a Distributed Event-Driven Pipeline. A distributed system without observability is impossible to debug, and without operational guardrails, it becomes a liability.
**CRITICAL INSTRUCTION:** When generating logging utilities, data models, or deployment scripts, you must strictly adhere to the following tracing, compliance, and deployment guardrails.

---

## 1. DISTRIBUTED OBSERVABILITY (TRACING)

* **Distributed Blindness:** A fragmented architecture makes debugging individual failed requests nearly impossible without a unified trace.
  * **The Guardrail (Correlation IDs):** You must implement a strict Correlation ID middleware. The FastAPI ingestion layer must generate a `X-Correlation-ID` (UUID) for every incoming request. This ID must be:
    1. Included in all FastAPI standard output logs.
    2. Embedded into the root of the JSON payload sent to the Kafka broker.
    3. Extracted by the Celery worker and injected into the worker's thread-local logging context so every background process log maps back to the origin request.

## 2. DATA COMPLIANCE & IMMUTABILITY

* **Kafka Log Poisoning (PII / Secrets):** Kafka logs are immutable. Writing raw, unscrubbed payloads containing authentication tokens, API keys, or Personally Identifiable Information (PII) into a topic creates an irreversible compliance breach.
  * **The Guardrail (Data Masking):** Implement a Pydantic sanitization layer prior to the Kafka producer hand-off. Explicitly redact, drop, or hash any fields identified as sensitive. Never log raw HTTP `Authorization` headers or full client payload dumps to standard output or message brokers.

## 3. DATABASE OPERATIONS

* **Migration Deadlocks:** Applying schema mutations (e.g., `ALTER TABLE`) via Alembic/SQLAlchemy while high-throughput asynchronous workers are writing to the database will trigger exclusive table locks, causing catastrophic worker timeouts and pipeline crashes.
  * **The Guardrail (Zero-Downtime Design):** Database queries generated must be forward and backward compatible. Explicitly instruct any deployment or migration scripts to temporarily pause worker consumption (e.g., revoking Celery queues) before executing schema changes, or strictly use additive-only schema migrations.

## 4. CLOUD & RESOURCE LIMITS

* **The Auto-Scaling Billing Trap:** Unbounded worker scaling driven by queue depth can lead to massive infrastructure cost overruns during a volumetric attack or external API latency spike.
  * **The Guardrail:** If generating infrastructure-as-code (e.g., Docker Swarm configs, Kubernetes manifests, or cloud setups), explicitly define `resource.limits` (CPU/Memory) and strictly enforce a `max_replicas` cap on all worker node deployments to prevent runaway scaling.
