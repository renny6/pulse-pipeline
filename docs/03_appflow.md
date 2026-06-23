# 03 | App Flow — Navigation & Journey Map

## Navigation Type
* Left-aligned persistent sidebar containing 4 main navigation tabs.

## Pages / Tabs List
1. **`/` (Live System Map):** The hero screen. Displays the React Flow node architecture and the HTML5 canvas for live particle tracking.
2. **`/simulator` (Load Tester):** The command center. Contains sliders for RPS (Requests Per Second), attack profiles (Normal User vs Bot Spam), and the "Fire Traffic Spike" trigger.
3. **`/health` (Infrastructure Monitor):** A grid of real-time metrics cards reading active container stats (CPU, Kafka Queue Depth, Redis Token levels, Active Workers).
4. **`/audit` (Historical Logs):** A paginated, searchable data table displaying the raw events successfully committed to TimescaleDB.

## Core User Journey (The Demo Flow)
1. User lands on `/` and sees the idle system architecture.
2. User navigates to `/simulator`, sets RPS to 5,000, and selects "Malicious Bot Spam".
3. User clicks **"LAUNCH TRAFFIC SPIKE"**.
4. The frontend triggers the load test against the FastAPI endpoints.
5. The UI automatically switches back to `/` (System Map).
6. WebSockets stream batched metrics (every 100ms) to the UI.
7. The canvas renders thousands of red lasers shattering against the Redis node (HTTP 429) and a controlled stream of green lasers passing through to Kafka (HTTP 202).
8. User navigates to `/audit` to verify that only clean, rate-limited data was saved to the database.

## Edge & Error States
* **WebSocket Disconnect:** Show a persistent red "Reconnecting..." banner at the top of the UI.
* **Empty States:** The `/audit` table should display a clean "Awaiting Traffic..." graphic before any requests are fired.
