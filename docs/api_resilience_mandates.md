# 🚨 EXTERNAL API INTEGRATION & QUOTA SURVIVAL MANDATES

## 🎯 Context Directive for AI Code Generation
This document defines the mandatory engineering guardrails for all external API integrations within the distributed pipeline. As we are not using a local AI fallback, these protocols are the primary defense against vendor rate-limiting, downtime, and schema drift.

**CRITICAL INSTRUCTION:** When generating code that interacts with external third-party endpoints, you must proactively implement these resilience patterns.

---

## 1. VOLUMETRIC DEFENSE (THE "POLITE RETRIEVER")

* **Distributed Rate Limiting (Outbound):** You must not allow individual Celery workers to independently hammer external APIs.
    * **The Guardrail:** Use the **centralized Redis Token Bucket** (the same used for gateway ingestion) to track global outbound API consumption. Every worker thread must acquire a token from Redis before executing an outbound network request.
* **Exponential Backoff with Jitter:** When encountering `HTTP 429 Too Many Requests` or transient network errors, you must use the `tenacity` library to implement backoff.
    * **The Guardrail:** Never retry immediately. Apply exponential wait times (e.g., 1s, 2s, 4s, 8s) combined with **randomized millisecond jitter** to prevent synchronized retry waves that would trigger an IP ban.

## 2. CIRCUIT BREAKER PATTERN (THE "STOP-LOSS")

* **Circuit Breaker Logic:** To prevent worker starvation during long API outages, you must wrap all network calls in the `pybreaker` pattern.
    * **The Guardrail:** If the failure rate (e.g., 5 consecutive errors or 20% error rate) passes your threshold, the circuit must trip to the **OPEN** state. Subsequent calls must be rejected immediately without hitting the network, protecting your internal worker threads from hanging indefinitely.

## 3. DEAD LETTER QUEUE (DLQ) ROUTING (THE "DATA VAULT")

* **Non-Lossy Error Handling:** You must never discard payloads when an external API permanently fails or exhausts its quota.
    * **The Guardrail:** Catch final quota exhaustion errors (`402` or `403`) and route the raw event payload (with the original `X-Correlation-ID`) into the Kafka `pulse.events.dlq` topic. This allows the administrative team to replay the data at the start of the next billing cycle.

## 4. NETWORK & SECURITY GUARDRAILS

* **Strict Timeout Enforcement:** Never allow an external API handshake to hold an asynchronous worker thread open indefinitely.
    * **The Guardrail:** Instantiate all `httpx.AsyncClient` calls with strict `timeout` configurations (e.g., `connect=2.0`, `read=5.0`).
* **Content Validation:** Never trust an `HTTP 200 OK` status code. 
    * **The Guardrail:** Parse the JSON body immediately and validate the presence of baseline structural health markers. Throw a custom `IntegrationException` if the payload contains hidden functional error objects (e.g., `{"success": false}`).

## 5. SCHEMA DRIFT PROTECTION

* **Defensive Pydantic Parsing:** Public APIs frequently change response shapes without versioning.
    * **The Guardrail:** Wrap all external responses in highly defensive Pydantic serialization models. Use `default=None` and `Field(alias="...")` aggressively. If a required field is missing, catch the `ValidationError`, log a structural warning, and return a safe default object rather than crashing the worker process.