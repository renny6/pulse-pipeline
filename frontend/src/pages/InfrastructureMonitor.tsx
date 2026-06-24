import { Server } from 'lucide-react'
import { PageHeader } from './LiveSystemMap'

/**
 * PAGE: Infrastructure Monitor  ( route: "/health" )
 *
 * Phase 5 stub — live container health grid.
 * Will show Docker health-check status for all pulse-* services.
 */

const SERVICES = [
  { name: 'pulse-timescaledb',    role: 'Analytical Vault',    status: 'healthy'   },
  { name: 'pulse-kafka',          role: 'Message Bus',         status: 'healthy'   },
  { name: 'pulse-redis',          role: 'State Guard',         status: 'healthy'   },
  { name: 'pulse-api',            role: 'FastAPI Gateway',      status: 'healthy'   },
  { name: 'pulse-celery-worker',  role: 'Persistence Worker',  status: 'healthy'   },
  { name: 'pulse-kafka-consumer', role: 'Consumer Daemon',     status: 'healthy'   },
  { name: 'pulse-zookeeper',      role: 'Kafka Coordinator',   status: 'healthy'   },
  { name: 'pulse-kafka-init',     role: 'Topic Initializer',   status: 'exited'    },
]

type ServiceStatus = 'healthy' | 'unhealthy' | 'exited' | 'starting'

const STATUS_COLOR: Record<ServiceStatus, string> = {
  healthy:   'var(--color-neon-green)',
  unhealthy: 'var(--color-crimson)',
  exited:    'var(--color-text-secondary)',
  starting:  'var(--color-electric-blue)',
}

export default function InfrastructureMonitor() {
  return (
    <div
      id="page-infrastructure-monitor"
      style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}
    >
      <PageHeader
        icon={<Server size={14} style={{ color: 'var(--color-electric-blue)' }} />}
        title="Infrastructure Monitor"
        badge="HEALTH"
        badgeColor="var(--color-neon-green)"
        description="Container health-check grid for all pulse-* Docker services"
      />

      <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>

        {/* Summary row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8, marginBottom: 16 }}>
          {[
            { label: 'Healthy',  count: 7, color: 'var(--color-neon-green)'    },
            { label: 'Unhealthy', count: 0, color: 'var(--color-crimson)'      },
            { label: 'Total',    count: 8, color: 'var(--color-text-primary)' },
          ].map(({ label, count, color }) => (
            <div key={label} className="panel" style={{ padding: '10px 14px' }}>
              <p className="section-label" style={{ marginBottom: 4 }}>{label}</p>
              <span className="tabular-nums" style={{ fontSize: 26, fontWeight: 700, color }}>
                {count}
              </span>
            </div>
          ))}
        </div>

        {/* Service grid */}
        <p className="section-label" style={{ marginBottom: 8 }}>Container Status</p>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {SERVICES.map(({ name, role, status }) => {
            const color = STATUS_COLOR[status as ServiceStatus] ?? 'var(--color-text-muted)'
            const isHealthy = status === 'healthy'
            return (
              <div
                key={name}
                className="panel"
                style={{
                  padding: '10px 14px',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 12,
                  borderLeft: `2px solid ${color}`,
                }}
              >
                {/* Status dot */}
                <span
                  className={`status-dot ${isHealthy ? 'green' : status === 'exited' ? 'muted' : 'red'} ${isHealthy ? 'animate-pulse-glow' : ''}`}
                />

                {/* Container name */}
                <span
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 11,
                    color: 'var(--color-text-primary)',
                    flex: 1,
                  }}
                >
                  {name}
                </span>

                {/* Role */}
                <span style={{ fontSize: 10, color: 'var(--color-text-muted)', minWidth: 140 }}>
                  {role}
                </span>

                {/* Status badge */}
                <span
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 9,
                    fontWeight: 700,
                    color,
                    border: `1px solid ${color}`,
                    padding: '1px 6px',
                    letterSpacing: '0.1em',
                    textTransform: 'uppercase',
                    minWidth: 70,
                    textAlign: 'center',
                  }}
                >
                  {status}
                </span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
