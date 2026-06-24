import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Zap, Play, Square } from 'lucide-react'
import { PageHeader } from './LiveSystemMap'
import { ThroughputChart } from '../components/ThroughputChart'

/**
 * PAGE: Load Tester  ( route: "/simulator" )
 *
 * Phase 5 implementation — ingestion burst simulator.
 * Provides configurable RPS sliders + traffic generation logic.
 */
export default function LoadTester() {
  const [targetRps, setTargetRps] = useState(500)
  const [burstDuration, setBurstDuration] = useState('30s')
  const [payloadSize, setPayloadSize] = useState(256)
  const [isRunning, setIsRunning] = useState(false)
  const navigate = useNavigate()

  const handleStart = (e?: React.MouseEvent) => {
    if (e) e.preventDefault();

    // Safe browser tick rate (10 requests per second)
    const ticksPerSecond = 10;
    const tickIntervalMs = 1000 / ticksPerSecond;

    // Calculate events per batch (e.g., 500 RPS / 10 = 50 events per batch)
    const eventsPerBatch = Math.ceil(targetRps / ticksPerSecond);
    const dummyString = "x".repeat(payloadSize);

    // Latency Tracker ID
    const trackingId = crypto.randomUUID();
    localStorage.setItem('latency_tracker_start', JSON.stringify({ id: trackingId, ts: Date.now() }));

    // Clear any existing loop
    if ((window as any).loadTestInterval) {
      clearInterval((window as any).loadTestInterval);
    }

    setIsRunning(true);

    // Start background loop
    (window as any).loadTestInterval = setInterval(() => {
      const batchPayload = Array.from({ length: eventsPerBatch }, (_, idx) => ({
        event_type: "simulator_spike",
        payload: idx === 0 ? { data: dummyString, _tracking_id: trackingId } : { data: dummyString }
      }));

      fetch('http://127.0.0.1:8000/api/v1/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(batchPayload)
      }).catch(() => {
        // Silently suppress network errors to keep loop running
      });
    }, tickIntervalMs);

    // Redirect user to see the visualization
    navigate('/');
  };

  const handleStop = () => {
    if ((window as any).loadTestInterval) {
      clearInterval((window as any).loadTestInterval);
      (window as any).loadTestInterval = null;
      console.log("🛑 Traffic spike halted.");
    }
  };

  return (
    <div
      id="page-load-tester"
      style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}
    >
      <PageHeader
        icon={<Zap size={14} style={{ color: 'var(--color-electric-blue)' }} />}
        title="Load Tester"
        badge="SIMULATOR"
        badgeColor="var(--color-electric-blue)"
        description="Configurable ingestion burst tester — validate Kafka throughput under load"
      />

      <div style={{ flex: 1, overflow: 'auto', padding: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>

        {/* Control panel */}
        <div
          className="panel glow-blue"
          style={{ padding: 16 }}
        >
          <p className="section-label" style={{ marginBottom: 12 }}>Simulator Controls</p>

          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'flex-end' }}>
            
            {/* Target RPS */}
            <div>
              <p className="section-label" style={{ marginBottom: 4 }}>Target RPS</p>
              <input
                type="number"
                value={targetRps}
                onChange={(e) => setTargetRps(Number(e.target.value))}
                style={{
                  width: 120, height: 32,
                  border: '1px solid var(--color-border)',
                  backgroundColor: 'var(--color-black)',
                  color: 'var(--color-text-primary)',
                  paddingInline: 10,
                  fontFamily: 'var(--font-mono)',
                  fontSize: 12,
                  outline: 'none'
                }}
              />
            </div>

            {/* Burst Duration */}
            <div>
              <p className="section-label" style={{ marginBottom: 4 }}>Burst Duration</p>
              <input
                type="text"
                value={burstDuration}
                onChange={(e) => setBurstDuration(e.target.value)}
                style={{
                  width: 120, height: 32,
                  border: '1px solid var(--color-border)',
                  backgroundColor: 'var(--color-black)',
                  color: 'var(--color-text-primary)',
                  paddingInline: 10,
                  fontFamily: 'var(--font-mono)',
                  fontSize: 12,
                  outline: 'none'
                }}
              />
            </div>

            {/* Payload Size */}
            <div>
              <p className="section-label" style={{ marginBottom: 4 }}>Payload Size (B)</p>
              <input
                type="number"
                value={payloadSize}
                onChange={(e) => setPayloadSize(Number(e.target.value))}
                style={{
                  width: 120, height: 32,
                  border: '1px solid var(--color-border)',
                  backgroundColor: 'var(--color-black)',
                  color: 'var(--color-text-primary)',
                  paddingInline: 10,
                  fontFamily: 'var(--font-mono)',
                  fontSize: 12,
                  outline: 'none'
                }}
              />
            </div>

            {/* Action buttons */}
            <div style={{ display: 'flex', gap: 8 }}>
              <button
                id="btn-start-simulation"
                type="button"
                onClick={handleStart}
                style={{
                  display: 'flex', alignItems: 'center', gap: 6,
                  padding: '6px 14px',
                  backgroundColor: 'rgba(0,229,255,0.1)',
                  border: '1px solid var(--color-electric-blue)',
                  color: 'var(--color-electric-blue)',
                  fontFamily: 'var(--font-mono)',
                  fontSize: 11, fontWeight: 600, letterSpacing: '0.06em',
                  cursor: 'pointer',
                }}
              >
                <Play size={11} />
                START
              </button>
              <button
                id="btn-stop-simulation"
                type="button"
                onClick={handleStop}
                style={{
                  display: 'flex', alignItems: 'center', gap: 6,
                  padding: '6px 14px',
                  backgroundColor: 'transparent',
                  border: '1px solid var(--color-border)',
                  color: 'var(--color-text-secondary)',
                  fontFamily: 'var(--font-mono)',
                  fontSize: 11, fontWeight: 600, letterSpacing: '0.06em',
                  cursor: 'pointer',
                }}
              >
                <Square size={11} />
                STOP
              </button>
            </div>
          </div>
        </div>

        {/* Metrics grid stub */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 8 }}>
          {[
            { label: 'Sent',          value: '—',  unit: 'events',   color: 'var(--color-neon-green)'    },
            { label: 'Accepted',      value: '—',  unit: 'events',   color: 'var(--color-electric-blue)' },
            { label: 'Rate-Limited',  value: '—',  unit: 'dropped',  color: 'var(--color-crimson)'       },
            { label: 'Latency P99',   value: '—',  unit: 'ms',       color: 'var(--color-electric-blue)' },
          ].map(({ label, value, unit, color }) => (
            <div
              key={label}
              className="panel"
              style={{ padding: '12px 14px' }}
            >
              <p className="section-label" style={{ marginBottom: 6 }}>{label}</p>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 4 }}>
                <span
                  className="tabular-nums"
                  style={{ fontSize: 22, fontWeight: 700, color }}
                >
                  {value}
                </span>
                <span style={{ fontSize: 10, color: 'var(--color-text-muted)' }}>{unit}</span>
              </div>
            </div>
          ))}
        </div>

        {/* Live Recharts component */}
        <div className="panel flex-1 min-h-[160px]">
          <ThroughputChart />
        </div>
      </div>
    </div>
  )
}
