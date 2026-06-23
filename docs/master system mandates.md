# 🏛️ MASTER SYSTEM ARCHITECTURE & DISTRIBUTED CONSISTENCY MANDATES

## 🎯 Context Directive for AI Code Generation
You are assisting an advanced platform engineering team in building a high-throughput, horizontally scalable **Distributed Event-Driven Ingestion & Analytics Pipeline**. The system uses FastAPI, Redis, Apache Kafka, Celery, TimescaleDB/PostgreSQL, and a React frontend dashboard.

### 🛑 CRITICAL DIRECTIVE
You must strictly reject junior-level single-instance anti-patterns (such as local in-memory states, unthrottled API loops, or unbuffered relational writes). Every line of code, Docker deployment file, and database query generated must conform to the absolute multi-layered engineering constraints detailed below.

---

## 1. GLOBAL SYSTEM ARCHITECTURE LAYERS

The system is partitioned into decoupled operational boundaries to isolate compute overhead, maintain a sub-5ms gateway response time, and guarantee data durability under extreme traffic load. 

```mermaid
graph TD
    subgraph Client_Tier ["1. Control Center"]
        UI[React Dashboard]
        Sim[Traffic Simulator]
        UI --- Sim
    end

    subgraph Load_Balancing ["2. Edge Routing"]
        LB[Nginx / HAProxy Load Balancer]
    end

    subgraph Ingestion_Layer ["3. Stateless Gateways"]
        API1[FastAPI Node 1]
        API2[FastAPI Node 2]
    end

    subgraph State_Guard ["4. Global Guard"]
        Redis[(Redis Cache <br> Atomic Lua Token Bucket)]
    end

    subgraph Message_Bus ["5. Decoupled Buffer"]
        Kafka[[Apache Kafka / RabbitMQ <br> Event Stream]]
        DLQ[[Dead Letter Queue]]
    end

    subgraph Worker_Tier ["6. Horizontal Processing"]
        Workers((Celery Worker Pool <br> Idempotent Execution))
    end

    subgraph Persistence ["7. Analytical Vault"]
        DB[(TimescaleDB / PostgreSQL <br> Micro-Batched Writes)]
    end

    %% Network Flow
    Client_Tier -- HTTP / Batched WebSockets --> LB
    LB --> API1 & API2
    
    API1 & API2 -- 1. Validate Token --> Redis
    Redis -- 2. Approve/Deny --> API1 & API2
    
    API1 & API2 -- 3. Produce (HTTP 202) --> Kafka
    
    Kafka -- 4. Consume Topic --> Workers
    
    Workers -- 5. Async Batch Commit --> DB
    
%% Error Routing and Edge Cases
Workers -. Unrecoverable Error .-> DLQ


    classDef default fill:#1e1e1e,stroke:#333,stroke-width:2px,color:#fff;
    classDef highlight fill:#2d3748,stroke:#63b3ed,stroke-width:2px,color:#fff;
    classDef error fill:#742a2a,stroke:#fc8181,stroke-width:2px,color:#fff;
    
    class Redis,Kafka highlight;
    class DLQ error;
2. CONCURRENCY, STATE, & TIME SYNCHRONIZATION MANDATES
Strict Redis Lua Execution: You must never write standard Python read-evaluate-write loops for the rate-limiter. You must write atomic Redis Lua scripts (EVAL) to prevent concurrent FastAPI gateways from triggering race conditions and leaking tokens.
Global Clock Source: You must never use datetime.utcnow() or local container system clocks for rate-limiting calculations. To mitigate cluster clock drift, you must strictly use the internal Redis time command (redis.call('TIME')) inside the Lua script as the single source of chronological truth.
Replica Divergence Guardrail: Because redis.call('TIME') is non-deterministic, raw script replication in a clustered configuration will cause state desynchronization. Ensure the container cluster configuration enforces effects-based replication (standard in Redis v3.2+), or pass the timestamp as a strict argument (ARGV) from the gateway.
Thundering Herd Mitigation: Do not design rigid, stepped block-time refills for token capacities. Implement continuous fractional token replenishment math inside the Lua script, and introduce randomized Jitter (micro-delays) inside client retry routines to scatter synchronized traffic waves.
3. MESSAGE BUS & BACKGROUND WORKER ALIGNMENT
Idempotent Data Consumption: Assume Kafka will occasionally deliver duplicate messages due to network blips (At-Least-Once delivery guarantees). All downstream background worker persistence routines must be strictly idempotent. When executing analytical writes, you must enforce relational safeguards such as ON CONFLICT DO NOTHING or selective UPSERT statements using unique business keys.
Worker Memory Leak Protections: Persistent Python worker processes dealing with continuous, high-volume streams are highly vulnerable to object allocation creeping. You must configure the Celery worker setting worker_max_tasks_per_child to automatically recycle worker memory boundaries after processing a specific task threshold.
Zombie Task Elimination: Never leave asynchronous tasks hanging without strict execution thresholds. Every worker task must be initialized with explicit soft_time_limit and time_limit configuration parameters to kill and cleanly log tasks that hang on external I/O operations.
Consumer Group Rebalancing Defenses: If a worker process blocks the event loop with a heavy database commit, it will miss its Kafka heartbeat (max.poll.interval.ms), causing the broker to evict it and trigger a cluster-wide rebalancing storm. You must completely decouple the Kafka network polling thread from the execution worker pool thread and lower the max_poll_records boundaries during traffic bursts.
4. DATABASE INTEGRITY & PERSISTENCE MANAGEMENT
Connection Pool Exhaustion Guardrail: Scaling out horizontal worker tiers can easily exceed the incoming connection thresholds of a relational database. You must enforce a resilient connection pool architecture using specialized mid-tier network proxies (like PgBouncer) or explicitly configured asynchronous pool allocations via SQLAlchemy's AsyncEngine. Crucially, connect a cleanup event listener to the Celery pre-fork lifecycle (celery.signals.worker_process_init) to tear down and cycle inherited engine pools cleanly.
Write-Ahead Log (WAL) Flooding Protection: High-velocity data surges executing structural database operations will generate huge volumes of transactional logs on disk before mutating table states. Under severe streaming stress, WAL volumes can accumulate faster than the database can archive them, triggering an emergency panic shutdown. You must explicitly configure the postgresql.conf parameters to scale up max_wal_size and optimize checkpoint_completion_target to tolerate heavy ingestion vectors.
Micro-Batching Ingestion: Workers must not execute single-row INSERT statements for high-velocity streams. Implement micro-batching logic within the worker threads to gather records in memory and commit them in optimized chunks (e.g., every 500 records or 2 seconds) to eliminate database write locks.
5. THIRD-PARTY & FREE-TIER API INTEGRATION PROTOCOLS
Circuit Breaker Integration: Free APIs aggressively enforce monthly usage quotas, returning hard errors like 403 Forbidden or 402 Payment Required upon token exhaustion. You must wrap all external API handshakes in a software Circuit Breaker (e.g., via pybreaker). Upon detecting a quota exhaustion status code, the circuit must immediately open and short-circuit subsequent outbound requests to prevent worker paralysis.
Graceful Degradation (Dead Letter Queue): If the external cloud API fails or trips the circuit breaker, the worker process must degrade gracefully rather than throwing a fatal crash. Implement an automatic Dead Letter Queue (DLQ) Routing. Upon catching a quota or network failure, the worker must fall back to moving the payload to a dedicated DLQ Kafka topic (pulse.events.dlq) for future replay.
Dead Letter Queue (DLQ) Routing: Tasks that permanently fail due to external API blocks must never be discarded or left clogging the primary message bus. Catch final quota exceptions and cleanly route the raw message payload into a dedicated Kafka Dead Letter Queue (DLQ) topic for future admin replay.
Exponential Backoff with Jitter: When encountering transient external throttling errors (HTTP 429 Too Many Requests), do not retry immediately. Implement an exponential backoff loop using the tenacity library, reinforced with randomized millisecond jitter to avoid synchronized retry waves.
Defensive Schema Parsing: Free public endpoints do not guarantee strict schema versioning or SLAs. Wrap all external JSON responses in highly defensive Pydantic serialization models using default=None and Field(alias="...") aggressively. Catch ValidationError exceptions cleanly and return a safe default object to keep the worker process alive.
Cursor-Based Pagination Checkpointing: For deep data extraction across multi-page endpoints, workers must save their pagination state (the current cursor or page number) to Redis after every single successful page fetch. If a worker process encounters an out-of-memory error on page 499, the subsequent task retry must read from Redis and resume extraction at page 499 instead of starting over from page 1.
Stateful Idempotency Keys: For any external API calls that mutate state outside your system, you must append a unique Idempotency-Key header mapped directly to the Kafka Event ID. If a network timeout causes a worker retry, the vendor can recognize the duplicate key and return the cached successful response without executing the action a second time.
6. ZERO-TRUST PERIMETER & INTERNAL SECURITY MANDATES
Ruthless Model Schema Boundaries: Attackers will attempt to bypass front-gate rate limits by firing "poison pill" malformed messages designed to break backend JSON deserializers. You must enforce absolute Pydantic strictness at the FastAPI boundary (extra = "forbid"). Do not accept loose kwargs or raw untyped dictionaries. Drop non-compliant payloads instantly with an HTTP 422 Unprocessable Entity before they ever reach the Kafka messaging layer.
Prohibition of Pickle Serialization: Celery workers must NEVER use Python's native pickle module for serialization or task results due to critical Remote Code Execution (RCE) vulnerabilities. You must explicitly configure the Celery application engine to task_serializer='json' and accept_content=['json'].
Internal TLS Mandate: Do not pass plaintext credentials or unencrypted data payloads inside the internal Docker network boundaries. Enforce TLS internally: all database connection strings must explicitly mandate SSL/TLS verification (e.g., appending ?ssl=require to PostgreSQL URIs), and internal Kafka/Redis listeners must handle encryption handshakes.
Server-Side Request Forgery (SSRF) Blocklist: If the ingestion layer utilizes an HTTP client (httpx or aiohttp) to fetch enrichment data based on user-provided payload URLs, you must implement a strict, non-bypassable URL parsing guardrail. Explicitly block and deny any outbound requests targeting localhost, 127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, internal Docker DNS names (redis, db), or cloud instance metadata endpoints (169.254.169.254).
Production CORS Lockdown: Never expose your backend API with an open wildcard policy (allow_origins=["*"]). Hardcode the FastAPI CORS middleware configuration to strictly white-list the explicit domain and port where your production React dashboard is hosted.
Secure WebSocket Upgrade Handshake: Standard unencrypted WebSockets (ws://) lack built-in authentication headers. You must enforce Secure WebSockets (wss://) and implement a strict handshake protocol requiring the React client to pass a secure, short-lived JSON Web Token (JWT) as a query parameter during the initial connection upgrade routine.
7. OPERATIONS, OBSERVABILITY, & COMPLIANCE
Distributed Tracing & Correlation IDs: A fragmented asynchronous pipeline makes debugging individual transactions nearly impossible without a centralized anchor. You must implement a strict Correlation ID middleware. The FastAPI ingestion layer must generate a unique X-Correlation-ID (UUID) for every incoming connection. This ID must be printed in all FastAPI output logs, embedded into the root of the JSON payload sent to Kafka, and extracted by the Celery worker to inject into its thread-local logging context.
Immutable Log Masking: Kafka logs cannot be easily altered or scrubbed once written. Writing raw, unredacted payloads containing authentication headers, access tokens, or Personally Identifiable Information (PII) creates an immediate compliance breach. Implement a defensive masking layer prior to the Kafka producer hand-off, automatically replacing sensitive fields with one-way hashes or *** blocks.
Zero-Downtime Database Migrations: Executing structural database mutations (like ALTER TABLE) via Alembic or SQLAlchemy while high-throughput asynchronous workers are hammering the database will instantly trigger exclusive locks, deadlocking the cluster. All database schemas generated must be forward and backward compatible. Instruct any deployment scripts to temporarily pause worker consumption queues before applying migrations, or stick strictly to additive-only database modifications.
