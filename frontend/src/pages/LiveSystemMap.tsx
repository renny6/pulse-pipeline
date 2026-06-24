import { useEffect, useRef, useState } from 'react'
import { Activity, Radio, Zap, Network, ShieldAlert, Database, Clock } from 'lucide-react'
import { CanvasEngine } from '../lib/CanvasEngine'
import { useMetrics } from '../contexts/MetricsContext'
import { SystemHealthStatus } from '../components/SystemHealthStatus'
import {
  ReactFlow,
  Background,
  Handle,
  Position,
  type Node,
  type Edge
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

/**
 * Custom Cyber Node Type
 */
function CyberNode({ data }: { data: any }) {
  const Icon = data.icon
  const glowClass = data.glow === 'blue' ? 'glow-blue' : ''

  return (
    <div
      className={`panel ${glowClass}`}
      style={{
        padding: '12px 16px',
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        minWidth: 180,
        backgroundColor: 'var(--color-card)',
        borderColor: 'var(--color-border)',
        borderWidth: 1,
        borderStyle: 'solid',
        color: 'var(--color-text-primary)',
        position: 'relative'
      }}
    >
      <Handle type="target" position={Position.Left} id="left" style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Left} id="left" style={{ opacity: 0 }} />
      <Handle type="target" position={Position.Right} id="right" style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Right} id="right" style={{ opacity: 0 }} />
      <Handle type="target" position={Position.Top} id="top" style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Top} id="top" style={{ opacity: 0 }} />
      <Handle type="target" position={Position.Bottom} id="bottom" style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Bottom} id="bottom" style={{ opacity: 0 }} />

      {Icon && <Icon size={16} style={{ color: data.glow === 'blue' ? 'var(--color-electric-blue)' : 'var(--color-text-secondary)' }} />}
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fontWeight: 600 }}>
        {data.label}
      </span>
    </div>
  )
}

const nodeTypes = {
  cyber: CyberNode
}

const initialNodes: Node[] = [
  {
    id: 'generator',
    type: 'cyber',
    position: { x: 50, y: 200 },
    data: { label: 'Traffic Generator', icon: Zap }
  },
  {
    id: 'gateway',
    type: 'cyber',
    position: { x: 300, y: 200 },
    data: { label: 'FastAPI Gateway', icon: Network, glow: 'blue' }
  },
  {
    id: 'redis',
    type: 'cyber',
    position: { x: 300, y: 50 },
    data: { label: 'Redis Rate Limiter', icon: ShieldAlert }
  },
  {
    id: 'kafka',
    type: 'cyber',
    position: { x: 550, y: 200 },
    data: { label: 'Kafka Event Bus', icon: Activity, glow: 'blue' }
  },
  {
    id: 'timescaledb',
    type: 'cyber',
    position: { x: 800, y: 200 },
    data: { label: 'TimescaleDB', icon: Database }
  }
]

const initialEdges: Edge[] = [
  {
    id: 'e-gen-gw',
    source: 'generator',
    target: 'gateway',
    sourceHandle: 'right',
    targetHandle: 'left',
    animated: true,
    style: { stroke: 'var(--color-text-secondary)' },
  },
  {
    id: 'e-gw-redis',
    source: 'gateway',
    target: 'redis',
    sourceHandle: 'top',
    targetHandle: 'bottom',
    animated: true,
    style: { stroke: 'var(--color-text-secondary)' },
  },
  {
    id: 'e-gw-kafka',
    source: 'gateway',
    target: 'kafka',
    sourceHandle: 'right',
    targetHandle: 'left',
    animated: true,
    style: { stroke: 'var(--color-text-secondary)' },
  },
  {
    id: 'e-kafka-db',
    source: 'kafka',
    target: 'timescaledb',
    sourceHandle: 'right',
    targetHandle: 'left',
    animated: true,
    style: { stroke: 'var(--color-text-secondary)' },
  }
]

/**
 * PAGE: Live System Map  ( route: "/" )
 *
 * Phase 5 implementation — topology canvas built with @xyflow/react.
 */
export default function LiveSystemMap() {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const { metrics } = useMetrics()
  const [latencyMs, setLatencyMs] = useState<number | null>(null)

  useEffect(() => {
    if (!canvasRef.current) return

    const engine = new CanvasEngine(canvasRef.current)

    // Expose temporarily for console testing
    ;(window as any).simulateTraffic = (route: 'ingest' | 'check_rate' | 'queue' | 'persist', status: 'success' | 'rate_limited' | 'processing') => {
      engine.spawnParticle(route, status)
    }

    const handleLatency = (e: Event) => {
      const customEvent = e as CustomEvent<number>
      setLatencyMs(customEvent.detail)
    }
    window.addEventListener('latency_measured', handleLatency)

    return () => {
      engine.destroy()
      delete (window as any).simulateTraffic
      window.removeEventListener('latency_measured', handleLatency)
    }
  }, [])

  return (
    <div
      id="page-live-system-map"
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        overflow: 'hidden',
      }}
    >
      {/* ── Page header ──────────────────────────────────────────────────── */}
      <PageHeader
        icon={<Activity size={14} style={{ color: 'var(--color-neon-green)' }} />}
        title="Live System Map"
        badge="LIVE"
        badgeColor="var(--color-neon-green)"
        description="Real-time event topology across Kafka, Celery, and TimescaleDB"
      />

      {/* ── Canvas Area ───────────────────────────────────────────── */}
      <div style={{ flex: 1, overflow: 'hidden', padding: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>

        {/* Status bar */}
        <div
          className="panel"
          style={{ padding: '8px 14px', display: 'flex', alignItems: 'center', gap: 16 }}
        >
          {[
            { label: 'Events/sec', value: metrics.eventsSec, color: 'var(--color-neon-green)' },
            { label: 'Kafka Lag',  value: metrics.kafkaLag, color: 'var(--color-electric-blue)' },
            { label: 'DB Writes',  value: metrics.dbWrites, color: 'var(--color-electric-blue)' },
            { label: 'DLQ Errors', value: metrics.dlqErrors, color: 'var(--color-crimson)' },
          ].map(({ label, value, color }) => (
            <div key={label} style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
              <span className="section-label">{label}</span>
              <span
                className="tabular-nums"
                style={{ fontSize: 14, fontWeight: 600, color }}
              >
                {value}
              </span>
            </div>
          ))}

          <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 16 }}>
            {latencyMs !== null && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <Clock size={11} style={{ color: 'var(--color-text-primary)' }} />
                <span className="section-label" style={{ color: 'var(--color-text-primary)' }}>
                  RTT: {latencyMs}ms
                </span>
              </div>
            )}
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <Radio size={11} style={{ color: 'var(--color-neon-green)' }} className="animate-pulse-glow" />
              <span className="section-label" style={{ color: 'var(--color-neon-green)' }}>
                Streaming
              </span>
            </div>
          </div>
        </div>

        {/* Canvas area */}
        <div
          className="panel glow-green scanlines"
          style={{
            flex: 1,
            position: 'relative',
            minHeight: 0,
            backgroundColor: 'var(--color-black)', // True Black background for map
            padding: 0
          }}
        >
          <SystemHealthStatus />

          <ReactFlow
            nodes={initialNodes}
            edges={initialEdges}
            nodeTypes={nodeTypes}
            fitView
            proOptions={{ hideAttribution: true }}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={false}
          >
            <Background color="var(--color-border)" gap={28} size={1} />
          </ReactFlow>

          {/* HTML5 Particle Canvas */}
          <canvas
            ref={canvasRef}
            className="absolute inset-0 pointer-events-none w-full h-full"
            style={{
              position: 'absolute',
              inset: 0,
              pointerEvents: 'none',
              zIndex: 10
            }}
          />
        </div>
      </div>
    </div>
  )
}

// ─── Shared sub-components ────────────────────────────────────────────────────

interface PageHeaderProps {
  icon: React.ReactNode
  title: string
  badge?: string
  badgeColor?: string
  description: string
}

export function PageHeader({ icon, title, badge, badgeColor, description }: PageHeaderProps) {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        padding: '12px 16px',
        borderBottom: '1px solid var(--color-border)',
        backgroundColor: 'var(--color-black)',
        flexShrink: 0,
      }}
    >
      {icon}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <h1
            style={{
              fontFamily: 'var(--font-mono)',
              fontSize: 13,
              fontWeight: 600,
              color: 'var(--color-text-primary)',
              letterSpacing: '0.04em',
            }}
          >
            {title}
          </h1>
          {badge && (
            <span
              style={{
                fontFamily: 'var(--font-mono)',
                fontSize: 9,
                fontWeight: 700,
                color: badgeColor,
                border: `1px solid ${badgeColor}`,
                padding: '1px 5px',
                letterSpacing: '0.1em',
              }}
            >
              {badge}
            </span>
          )}
        </div>
        <p style={{ fontSize: 10, color: 'var(--color-text-muted)', marginTop: 1 }}>
          {description}
        </p>
      </div>
    </div>
  )
}
