import type { LiveMetrics } from '../contexts/MetricsContext'

type SetMetricsFn = (metrics: LiveMetrics | ((prev: LiveMetrics) => LiveMetrics)) => void

export class WebSocketManager {
  private static instance: WebSocketManager
  private ws: WebSocket | null = null
  private url = 'ws://127.0.0.1:8000/ws/metrics'
  private reconnectTimeout = 1000
  private maxReconnectTimeout = 30000
  private isConnecting = false
  private setMetricsCb: SetMetricsFn | null = null

  private constructor() {}

  public static getInstance(): WebSocketManager {
    if (!WebSocketManager.instance) {
      WebSocketManager.instance = new WebSocketManager()
    }
    return WebSocketManager.instance
  }

  public registerMetricsCallback(cb: SetMetricsFn) {
    this.setMetricsCb = cb
  }

  public connect() {
    if (this.ws || this.isConnecting) return

    this.isConnecting = true
    this.ws = new WebSocket(this.url)

    this.ws.onopen = () => {
      this.isConnecting = false
      this.reconnectTimeout = 1000 // Reset exponential backoff
      window.dispatchEvent(new Event('ws:connected'))
      console.log('[WebSocketManager] Connected to backend')
    }

    this.ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data)
        this.handlePayload(payload)
      } catch (err) {
        console.error('[WebSocketManager] Failed to parse message', err)
      }
    }

    this.ws.onclose = () => {
      this.ws = null
      this.isConnecting = false
      window.dispatchEvent(new Event('ws:disconnected'))
      console.log(`[WebSocketManager] Disconnected. Reconnecting in ${this.reconnectTimeout}ms...`)

      setTimeout(() => this.connect(), this.reconnectTimeout)
      
      // Apply exponential backoff
      this.reconnectTimeout = Math.min(this.reconnectTimeout * 2, this.maxReconnectTimeout)
    }

    this.ws.onerror = (err) => {
      console.error('[WebSocketManager] Error:', err)
      this.ws?.close() // Force close which triggers onclose and reconnection
    }
  }

  private handlePayload(payload: any) {
    console.log("WebSocket Message Received: " + JSON.stringify(payload));
    
    // Dispatch raw payload for components like charts that need full history
    window.dispatchEvent(new CustomEvent('ws:message', { detail: payload }))

    // 1. Update Global Metrics Context
    if (this.setMetricsCb) {
      this.setMetricsCb(prev => ({
        // We multiply accepted by 10 to approximate events/sec since window is 100ms
        eventsSec: payload.accepted !== undefined ? payload.accepted * 10 : prev.eventsSec,
        kafkaLag: payload.kafka_lag ?? prev.kafkaLag,
        dbWrites: payload.db_writes ?? prev.dbWrites,
        dlqErrors: payload.dlq_errors ?? prev.dlqErrors,
      }))
    }

    // Pipeline Latency Tracker
    if (payload.tracking_ids && payload.tracking_ids.length > 0) {
      const stored = localStorage.getItem('latency_tracker_start')
      if (stored) {
        try {
          const parsed = JSON.parse(stored)
          if (payload.tracking_ids.includes(parsed.id)) {
            const rtt = Date.now() - parsed.ts
            window.dispatchEvent(new CustomEvent('latency_measured', { detail: rtt }))
            localStorage.removeItem('latency_tracker_start')
          }
        } catch (e) {
          // ignore
        }
      }
    }

    // 2. Trigger Physics Engine Particles if the global function is available
    const sim = (window as any).simulateTraffic
    if (typeof sim === 'function') {
      
      // Ingest traffic
      if (typeof payload.accepted === 'number' && payload.accepted > 0) {
        // Spawn particles based on the accepted events in this 100ms window
        const count = Math.min(Math.ceil(payload.accepted / 2), 10)
        for (let i = 0; i < count; i++) {
          setTimeout(() => sim('ingest', 'success'), i * 30)
        }
      }

      // Check Rate hits
      if (payload.blocked && payload.blocked > 0) {
        // Send a visual pulse for a rate limit event
        sim('check_rate', 'rate_limited')
      } else if (payload.accepted > 0) {
        // Otherwise a normal rate check pulse
        setTimeout(() => sim('check_rate', 'success'), 50)
      }

      // Queue (Kafka depth)
      if (payload.kafka_lag > 0 || payload.accepted > 0) {
        const queueVal = payload.kafka_lag || payload.accepted
        const count = Math.min(Math.ceil(queueVal / 2), 5)
        for (let i = 0; i < count; i++) {
          setTimeout(() => sim('queue', 'processing'), i * 40)
        }
      }

      // Persist (DB Writes)
      if (typeof payload.accepted === 'number' && payload.accepted > 0) {
        const dbVal = payload.db_writes || payload.accepted
        const count = Math.min(Math.ceil(dbVal / 2), 10)
        for (let i = 0; i < count; i++) {
          setTimeout(() => sim('persist', 'success'), i * 30)
        }
      }
    }
  }
}
