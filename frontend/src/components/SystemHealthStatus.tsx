import { useEffect, useState } from 'react'
import { Server, Database, Activity, ShieldAlert } from 'lucide-react'

interface HealthResponse {
  status: string
  redis: string
  postgres: string
  kafka: string
}

export function SystemHealthStatus() {
  const [health, setHealth] = useState<HealthResponse | null>(null)

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const res = await fetch('http://127.0.0.1:8000/health/system')
        if (res.ok) {
          const data = await res.json()
          setHealth(data)
        } else {
          setHealth({ status: 'degraded', redis: 'unreachable', postgres: 'unreachable', kafka: 'unreachable' })
        }
      } catch (err) {
        setHealth({ status: 'degraded', redis: 'unreachable', postgres: 'unreachable', kafka: 'unreachable' })
      }
    }

    checkHealth()
    const interval = setInterval(checkHealth, 5000)
    return () => clearInterval(interval)
  }, [])

  const StatusDot = ({ status }: { status?: string }) => (
    <div style={{
      width: 6,
      height: 6,
      borderRadius: '50%',
      backgroundColor: status === 'ok' ? 'var(--color-neon-green)' : 'var(--color-crimson)',
      boxShadow: status === 'ok' ? '0 0 6px var(--color-neon-green)' : '0 0 6px var(--color-crimson)'
    }} />
  )

  const items = [
    { label: 'API Gateway', status: health ? 'ok' : 'unreachable', icon: Server },
    { label: 'Redis', status: health?.redis, icon: ShieldAlert },
    { label: 'Kafka', status: health?.kafka, icon: Activity },
    { label: 'TimescaleDB', status: health?.postgres, icon: Database },
  ]

  return (
    <div className="panel" style={{
      position: 'absolute',
      bottom: 24,
      right: 24,
      padding: '12px 16px',
      display: 'flex',
      flexDirection: 'column',
      gap: 12,
      zIndex: 100,
      minWidth: 200,
      backgroundColor: 'rgba(0, 0, 0, 0.8)',
      backdropFilter: 'blur(8px)'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ fontSize: 11, fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--color-text-primary)' }}>
          SYSTEM HEALTH
        </span>
        <StatusDot status={health?.status === 'ready' ? 'ok' : 'degraded'} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {items.map((item) => (
          <div key={item.label} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <item.icon size={12} style={{ color: 'var(--color-text-secondary)' }} />
              <span style={{ fontSize: 10, color: 'var(--color-text-secondary)' }}>{item.label}</span>
            </div>
            <StatusDot status={item.status} />
          </div>
        ))}
      </div>
    </div>
  )
}
