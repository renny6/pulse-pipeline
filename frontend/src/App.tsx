import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import LiveSystemMap from './pages/LiveSystemMap'
import LoadTester from './pages/LoadTester'
import InfrastructureMonitor from './pages/InfrastructureMonitor'
import HistoricalLogs from './pages/HistoricalLogs'
import About from './pages/About'
import { MetricsProvider } from './contexts/MetricsContext'

/**
 * Root application entry.
 *
 * Route map:
 *   /           → Live System Map   (real-time topology)
 *   /simulator  → Load Tester       (ingestion burst simulator)
 *   /health     → Infra Monitor     (container health grid)
 *   /audit      → Historical Logs   (TimescaleDB audit trail)
 *   /about      → About Pulse       (Project overview)
 *   *           → redirect → /
 *
 * All routes are wrapped inside <Layout />, which renders the persistent
 * sidebar and main content shell.
 */
export default function App() {
  return (
    <MetricsProvider>
      <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/"          element={<LiveSystemMap />} />
          <Route path="/simulator" element={<LoadTester />} />
          <Route path="/health"    element={<InfrastructureMonitor />} />
          <Route path="/audit"     element={<HistoricalLogs />} />
          <Route path="/about"     element={<About />} />
          {/* Catch-all — redirect unknown paths to home */}
          <Route path="*"          element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
    </MetricsProvider>
  )
}
