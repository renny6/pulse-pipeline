# Pulse Pipeline

> A high-velocity, real-time observability platform and event streaming pipeline designed for production-grade scale.

Pulse Pipeline is engineered to ingest, buffer, and persist immense telemetry data streams. By decoupling the fast ingestion boundary from the downstream storage layer, the system provides high availability and zero data loss under massive traffic spikes, all while giving operators a live, tactical view of system health and throughput.

![Pulse Pipeline System Overview](docs/assets/about%20pulse.png)
*Figure 1: Pulse Pipeline Overview - High-performance distributed telemetry ingestion architecture, showcasing decoupled event streams, rate limiting, and real-time visualization.*

---

## 🏗️ Architecture

Pulse utilizes a distributed, resilient architecture to guarantee data durability and provide sub-second observability:

1. **FastAPI Ingestion Gateway:** Validates incoming JSON payloads at the edge and enforces sliding-window rate limits via Redis.
2. **Kafka Event Bus:** Acts as the central nervous system, absorbing traffic spikes and buffering events into partitions to protect downstream databases from lockups.
3. **Celery Workers:** A fleet of decoupled background consumers that poll Kafka and perform highly optimized bulk upserts.
4. **TimescaleDB:** A robust PostgreSQL extension tailored for time-series data, acting as the final cold storage.
5. **WebSocket Dashboard:** A React frontend that connects to the FastAPI backend via WebSockets. It receives a 100ms batched metric stream to render the live data topology without succumbing to backpressure.

![Infrastructure Status Monitor](docs/assets/pulse%20infra%20moniter.png)
*Figure 2: Infrastructure Status Monitor - Live health checks and connection state verification for Kafka, Redis, and TimescaleDB containers.*

---

## ✨ Key Features

- **Pipeline Latency Tracker:** Inject UUIDs into the traffic simulator and measure exact Round Trip Time (RTT) across the entire distributed pipeline, visualizing it directly on the dashboard.
- **Causality Diagnostics:** Detailed error reporting prevents silent failures. Dropped events (e.g., DLQ failures, rate limits) trigger alerts via Redis PubSub, broadcasting the failure reason straight to the frontend.
- **System Health Widget:** Active polling constantly checks the status of Redis, TimescaleDB, and Kafka, illuminating red or green indicator dots to immediately surface infrastructure outages.
- **Anti-Backpressure UI:** The frontend leverages `@xyflow/react` and custom HTML5 canvas rendering to visualize thousands of events per second without crashing the browser.

---

## 📊 System Visualization

![Pulse Dashboard](docs/assets/pulse%20dashboard.png)
*Figure 3: Real-Time Tactical Control Center - The GPU-accelerated HTML5 Canvas rendering dashboard showing live network throughput, partition workloads, and node latencies.*

---

## 🛠️ Tech Stack

- **Backend:** Python, FastAPI, Celery
- **Message Broker:** Apache Kafka, Zookeeper
- **Caching & PubSub:** Redis
- **Database:** PostgreSQL with TimescaleDB
- **Frontend:** React, TypeScript, Tailwind CSS, React Flow

---

## 🚀 Quick Start

Ensure you have **Docker** and **Docker Compose** installed.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/pulse-pipeline.git
   cd pulse-pipeline
   ```

2. **Launch the infrastructure:**
   This command starts the API gateway, Celery workers, and all required infrastructure components (Kafka, Redis, TimescaleDB).
   ```bash
   docker compose up -d --build
   ```

3. **Start the tactical dashboard:**
   In a separate terminal, navigate to the frontend directory to run the UI.
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

4. **Observe:**
   Open `http://localhost:5173/` in your browser. Use the Load Tester module to simulate a massive traffic spike and watch the system absorb it.

![API Ingestion Interface](docs/assets/api%20pulse%20.png)
*Figure 4: API Gateway Ingestion - The FastAPI `/api/v1/ingest` interface, designed for sub-5ms payload validation and edge token-bucket throttling.*

---

## 📚 Documentation

### Architecture Diagram

```mermaid
flowchart TD
    %% Define styles
    classDef client fill:#0f172a,stroke:#38bdf8,stroke-width:2px,color:#fff
    classDef gateway fill:#1e1b4b,stroke:#a855f7,stroke-width:2px,color:#fff
    classDef broker fill:#14532d,stroke:#22c55e,stroke-width:2px,color:#fff
    classDef worker fill:#312e81,stroke:#6366f1,stroke-width:2px,color:#fff
    classDef storage fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#fff
    classDef cache fill:#7f1d1d,stroke:#ef4444,stroke-width:2px,color:#fff
    classDef ui fill:#0f172a,stroke:#0ea5e9,stroke-width:2px,color:#fff

    %% Primary Pipeline
    Gen["Traffic Generator\n(Load Tester)"]:::client
    API["FastAPI Gateway\n(Ingestion)"]:::gateway
    Kafka["Kafka Event Bus"]:::broker
    Celery["Celery Workers\n(Processing)"]:::worker
    DB[("TimescaleDB\n(Storage)")]:::storage
    
    %% Secondary Components
    Redis[("Redis\n(Rate Limit & PubSub)")]:::cache
    WS["WebSocket Manager"]:::gateway
    UI["React Dashboard\n(Control Center)"]:::ui

    %% Core Data Flow
    Gen -- "POST /api/v1/ingest" --> API
    API -- "Publish Event" --> Kafka
    Kafka -- "Poll Batches" --> Celery
    Celery -- "Bulk Upsert" --> DB

    %% Observability & Control Flow
    API -. "Check Rate Limit" .-> Redis
    API == "Record Metric (100ms)" ==> WS
    Celery -. "DLQ Drops (PubSub)" .-> Redis
    Redis -. "Listen to metrics" .-> WS
    WS == "Broadcast via WebSocket" ==> UI
```

---

## ✅ System Verification

The pipeline has been rigorously validated for **at-least-once** data delivery guarantees. 

Our automated End-to-End (E2E) integration test verifies the entire asynchronous data flow. The smoke test uses `pytest` to inject 100 uniquely correlated mock events directly into the Kafka ingestion topic, waits for the decoupled consumer daemon to process the batch, and asserts that exactly 100 records are successfully persisted in the TimescaleDB hypertable without data loss or duplication.

To run the verification suite:
```bash
docker compose exec api pytest tests/test_e2e_integration.py -v -s
```

**Status:** `[PASSED]` (2026-06-24)

### Historical Log Persistence

![Historical Logs](docs/assets/pulse%20historical%20logs.png)
*Figure 5: Historical Logs & Persistence - Cold storage query analytics and audit trail of ingested events verified in TimescaleDB database.*

### Volumetric Load Testing

![Load Tester](docs/assets/pulse%20loadtester.png)
*Figure 6: Volumetric Load Tester - High-throughput traffic generator simulating spike injections of up to 5,000 requests per second to evaluate pipeline backpressure.*
