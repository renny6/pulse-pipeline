 ZERO-TRUST SECURITY & PERIMETER MANDATES

## Context Directive for AI Code Generation
You are assisting in building a distributed ingestion pipeline designed for environments with zero-trust architectural standards. You must assume the internal Docker network is potentially compromised and the system is under constant volumetric attack. Every endpoint, worker configuration, and data-fetching utility must adhere to the following protocols.

---

## 1. EDGE & VOLUMETRIC DEFENSE

* **The Distributed Botnet Bypass:** Malicious actors will rotate IP addresses via botnets to bypass the Redis Token Bucket.
  * **The Guardrail:** Assume the application sits behind a Web Application Firewall (WAF). Ensure reverse proxy or load balancer configurations (e.g., Nginx `nginx.conf`) enforce aggressive **Connection Limiting** (`limit_conn` and `limit_req`) to drop TCP floods at the edge.

## 2. PAYLOAD BOUNDARIES & RCE PREVENTION

* **"Poison Pill" Messages:** Attackers will send malformed payloads designed to crash background workers during deserialization, triggering Kafka rebalancing loops.
  * **The Guardrail:** Enforce **ruthless Pydantic strictness** at the FastAPI boundary (`extra = "forbid"`). Drop non-compliant payloads instantly with an `HTTP 422 Unprocessable Entity`.
* **Insecure Deserialization (Pickle RCE):** Celery workers must NEVER use `pickle` for serialization.
  * **The Guardrail:** Explicitly configure the Celery application to `accept_content=['json']` and set `task_serializer='json'`.

## 3. INTERNAL NETWORK ENCRYPTION

* **Plaintext Prohibition:** Do not use `PLAINTEXT://` for Kafka listeners or unencrypted `redis://` connections. Assume internal network sniffing is a constant threat.
* **Forced SSL:** Database connection strings must explicitly mandate SSL/TLS verification (e.g., appending `?ssl=require` to PostgreSQL URIs).

## 4. SSRF & CROSS-ORIGIN DEFENSE

* **Server-Side Request Forgery (SSRF) Blocks:** If the API utilizes `httpx` to fetch external enrichment data based on payload URLs, you must implement a strict blocklist.
  * **The Guardrail:** Deny any outbound requests from the worker to `localhost`, `127.0.0.0/8`, `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, internal Docker DNS names, and AWS metadata endpoints (`169.254.169.254`).
* **Production CORS Lockdown:** Never expose the backend API with `allow_origins=["*"]`. Hardcode the FastAPI CORS middleware to strictly whitelist the exact domain of the React dashboard.

## 5. SECRETS MANAGEMENT & AUTHENTICATION

* **Image Layer Protection:** Absolutely no hardcoded credentials or API keys are allowed in Python files or `Dockerfile` `ENV` commands. All sensitive configurations must rely exclusively on environment variables injected at runtime via an external `.env` file.
* **Secure WebSocket Upgrade Handshake:** Standard `ws://` connections lack authentication. Mandate `wss://` and require the client to pass a secure, short-lived JWT during the initial upgrade handshake to prevent unauthorized analytics eavesdropping.

