import { useState, useEffect } from 'react'
import { Outlet } from 'react-router-dom'
import Sidebar from './Sidebar'
import { WebSocketManager } from '../lib/WebSocketManager'
import { useMetrics } from '../contexts/MetricsContext'

// ─── Component ────────────────────────────────────────────────────────────────
/**
 * Root layout shell.
 *
 * Renders the persistent sidebar alongside a scrollable main content area.
 * Tracks WebSocket connectivity and propagates disconnect state to the sidebar.
 *
 * Usage: wrap all authenticated routes with <Layout /> in App.tsx.
 */
export default function Layout() {
  const [wsDisconnected, setWsDisconnected] = useState(false)
  const { setMetrics } = useMetrics()

  // ── WebSocket connectivity tracker ────────────────────────────────────────
  useEffect(() => {
    // Initialize singleton WebSocket connection
    const wsManager = WebSocketManager.getInstance()
    wsManager.registerMetricsCallback(setMetrics)
    wsManager.connect()

    // Listen for global custom events dispatched by the WS hook / service layer.
    // The hook will fire 'ws:connected' / 'ws:disconnected' on the window.
    const handleConnected    = () => setWsDisconnected(false)
    const handleDisconnected = () => setWsDisconnected(true)

    window.addEventListener('ws:connected',    handleConnected)
    window.addEventListener('ws:disconnected', handleDisconnected)

    return () => {
      window.removeEventListener('ws:connected',    handleConnected)
      window.removeEventListener('ws:disconnected', handleDisconnected)
    }
  }, [])

  return (
    <div
      id="app-shell"
      style={{
        display: 'flex',
        height: '100vh',
        width: '100vw',
        overflow: 'hidden',
        backgroundColor: 'var(--color-charcoal)',
      }}
    >
      {/* ── Persistent sidebar ────────────────────────────────────────────── */}
      <Sidebar wsDisconnected={wsDisconnected} />

      {/* ── Main content area ─────────────────────────────────────────────── */}
      <main
        id="main-content"
        style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
          position: 'relative',
          backgroundColor: 'var(--color-charcoal)',
        }}
      >
        {/* Outlet renders the matched child route page */}
        <Outlet />
      </main>
    </div>
  )
}
