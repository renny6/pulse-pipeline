# 🚨 EXTERNAL API INTEGRATION & QUOTA SURVIVAL

## 1. VOLUMETRIC DEFENSE (THE "POLITE RETRIEVER")
* **Global Rate Limiting:** All outbound API requests must request a token from the centralized Redis instance before calling the network.
* **Jitter:** Add randomized 0-1000ms jitter to all retries to prevent "Avalanche Effect" retries.

## 2. CIRCUIT BREAKER PATTERN (THE "STOP-LOSS")
* Implement `pybreaker`. If 5+ requests fail or 20% error rate occurs, the circuit moves to **OPEN**.
* While OPEN, reject new API requests locally for 60 seconds.

## 3. DATA VAULT (DLQ)
* If an API quota is permanently exhausted (`402/403`), move the event payload to Kafka `pulse.events.dlq`.
* Log the full `X-Correlation-ID` and failure reason in the DB `dead_letter_queue` table for auditing.

## 4. DEFENSIVE PARSING
* All external responses must be validated with Pydantic models.
* Use `default=None` to handle missing fields without worker crashes.
