# 🚨 EXTERNAL API INTEGRATION HURDLES, ISSUES & GUARDRAILS

## Context Directive for AI Code Generation
This document explores the technical failures and architectural issues encountered when consuming external third-party APIs (e.g., financial data harvesters) under high-throughput conditions.

---

## 1. RATE LIMITING & CASCADING BLOCKS

* **External Throttling (HTTP 429) & Semaphore Defeat:** Isolated application-level rate-limiters fail to track global consumption across horizontal workers.
  * **The Guardrail:** Externalize the API consumption state using a **Distributed Token Bucket via Redis**. All outbound API workers must query Redis atomically before executing a request.
* **Cascading Network Retry Storms:** Immediate-retry loops during an API blip cause an exponential explosion of outgoing requests.
  * **The Guardrail:** Enforce a strict **Exponential Backoff with Jitter** retry strategy (e.g., using `tenacity`) to spread out retry intervals and avoid synchronized traffic waves.

## 2. SCHEMA DRIFT & PAYLOAD INTEGRITY

* **Silent Schema Drift:** Third-party providers modify JSON responses without incrementing API version headers, triggering catastrophic `ValidationError` exceptions.
  * **The Guardrail:** Implement strict Data Sanitization Wrappers using Pydantic's `default=None` or `alias` features to catch schema anomalies gracefully.
* **HTTP 200 OK Content-Type Deception:** APIs returning a `200 OK` status code but containing a custom error payload inside the body (e.g., `{"success": false, "error": "Over quota"}`).
  * **The Guardrail:** Never trust the HTTP status code alone. Explicitly inspect the deserialized JSON object for known failure keys before handing the payload off to downstream processing.

## 3. NETWORK TIMEOUTS & RESOURCE STARVATION

* **Indefinite Socket Hanging:** Unresponsive third-party endpoints keep asynchronous worker connections open forever, exhausting the worker pool.
  * **The Guardrail:** Enforce strict, non-negotiable **Connect and Read Timeouts** (e.g., `timeout=5.0`) on all HTTP client instances (`httpx.AsyncClient`).
* **DNS Resolution Bottlenecks:** High-frequency API calls repeatedly query slow upstream DNS servers, adding massive latency overhead.
  * **The Guardrail:** Implement a persistent, connection-pooled HTTP session instance that caches DNS lookups across worker tasks.

## 4. PAGINATION & DATA EXTRACTION FAILURES

* **Deep Pagination Memory Bloat:** Accumulating 500 pages of external API records in a single Python list before committing to the database triggers an Out-Of-Memory (OOM) kill.
  * **The Guardrail:** Implement **Streaming Yield Generators** that process, persist, and flush individual page chunks to TimescaleDB iteratively.
* **Uncheckpointed Extraction Failures:** A network drop on page 99 of a 100-page extraction forces the worker to re-fetch all 99 pages on retry.
  * **The Guardrail:** Store the cursor/page state in Redis after every successful page fetch. Instruct task retries to read the last successful checkpoint from Redis and resume extraction exactly where it dropped.

## 5. ERROR STATE ENTANGLEMENT

* **Transient vs. Terminal Misclassification:** Treating an `HTTP 401 Unauthorized` (terminal key revocation) the same as an `HTTP 503 Service Unavailable` (transient server blip) causes infinite, useless retries.
  * **The Guardrail:** Implement explicit status code routing. Transient errors (`50x`, `429`) trigger exponential backoff. Terminal errors (`401`, `403`, `404`) instantly break the loop and route the payload directly to the Dead Letter Queue (`pulse.events.dlq`).
* **SSL / TLS Handshake Corruption:** Upstream certificate expirations or cipher mismatches throw low-level socket errors that bypass standard HTTP try/catch blocks.
  * **The Guardrail:** Isolate SSL errors from typical HTTP exceptions so they do not pollute standard retries. Route to a Dead Letter Queue (DLQ) if necessary.
* **Circuit Breaker Paralysis:** Repeated API blips lock the circuit open forever if the reset timer is misconfigured, starving the pipeline of data.
  * **The Guardrail:** Implement the Circuit Breaker Pattern (`pybreaker`). If the failure rate exceeds a configured boundary, the circuit opens immediately, rejecting requests instantly or routing to a Dead Letter Queue (DLQ) to protect internal infrastructure. 