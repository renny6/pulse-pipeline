import { NavLink, useLocation } from 'react-router-dom'
import {
  Activity,
  Zap,
  Server,
  ScrollText,
  WifiOff,
  Cpu,
  Info
} from 'lucide-react'

// ─── Types ────────────────────────────────────────────────────────────────────
interface SidebarProps {
  /** Pass true when the WebSocket connection to the backend is lost */
  wsDisconnected?: boolean
}

// ─── Navigation entries ───────────────────────────────────────────────────────
const NAV_ITEMS = [
  {
    path: '/',
    label: 'Live System Map',
    sublabel: 'Real-time topology',
    Icon: Activity,
    exact: true,
  },
  {
    path: '/simulator',
    label: 'Load Tester',
    sublabel: 'Ingestion simulator',
    Icon: Zap,
    exact: false,
  },
  {
    path: '/health',
    label: 'Infra Monitor',
    sublabel: 'Container health',
    Icon: Server,
    exact: false,
  },
  {
    path: '/audit',
    label: 'Historical Logs',
    sublabel: 'Audit trail',
    Icon: ScrollText,
    exact: false,
  },
  {
    path: '/about',
    label: 'About Pulse',
    sublabel: 'Project overview',
    Icon: Info,
    exact: false,
  },
]

// ─── Component ────────────────────────────────────────────────────────────────
export default function Sidebar({ wsDisconnected = false }: SidebarProps) {
  const location = useLocation()

  return (
    <aside
      id="sidebar"
      style={{
        width: 'var(--sidebar-width)',
        minWidth: 'var(--sidebar-width)',
        maxWidth: 'var(--sidebar-width)',
        backgroundColor: 'var(--color-black)',
        borderRight: '1px solid var(--color-border)',
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        position: 'relative',
        zIndex: 10,
        flexShrink: 0,
      }}
    >
      {/* ── Wordmark / identity ──────────────────────────────────────────── */}
      <header
        style={{
          padding: '20px 16px 16px',
          borderBottom: '1px solid var(--color-border-subtle)',
          flexShrink: 0,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
          <Cpu
            size={16}
            style={{ color: 'var(--color-neon-green)', flexShrink: 0 }}
          />
          <span
            style={{
              fontFamily: 'var(--font-mono)',
              fontSize: 13,
              fontWeight: 600,
              color: 'var(--color-text-primary)',
              letterSpacing: '0.06em',
            }}
          >
            PULSE
          </span>
          <span
            className="section-label"
            style={{ color: 'var(--color-electric-blue)', marginLeft: 0 }}
          >
            v5
          </span>
        </div>
        <p className="section-label" style={{ paddingLeft: 24 }}>
          Control Center
        </p>
      </header>

      {/* ── WebSocket disconnect alert banner ────────────────────────────── */}
      {wsDisconnected && (
        <div
          id="ws-disconnect-banner"
          role="alert"
          aria-live="assertive"
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            padding: '8px 12px',
            backgroundColor: 'rgba(255,0,60,0.12)',
            borderBottom: '1px solid var(--color-crimson)',
            borderLeft: '3px solid var(--color-crimson)',
            flexShrink: 0,
          }}
        >
          <WifiOff
            size={12}
            style={{ color: 'var(--color-crimson)', flexShrink: 0 }}
            className="animate-pulse-glow"
          />
          <div>
            <p
              style={{
                fontFamily: 'var(--font-mono)',
                fontSize: 10,
                fontWeight: 600,
                color: 'var(--color-crimson)',
                letterSpacing: '0.08em',
                textTransform: 'uppercase',
              }}
            >
              WS DISCONNECTED
            </p>
            <p style={{ fontSize: 10, color: 'rgba(255,0,60,0.7)', marginTop: 1 }}>
              Live feed interrupted
            </p>
          </div>
        </div>
      )}

      {/* ── Section label ────────────────────────────────────────────────── */}
      <div style={{ padding: '14px 16px 6px' }}>
        <p className="section-label">Navigation</p>
      </div>

      {/* ── Nav items ────────────────────────────────────────────────────── */}
      <nav
        id="sidebar-nav"
        style={{ flex: 1, overflowY: 'auto', padding: '0 8px' }}
      >
        {NAV_ITEMS.map(({ path, label, sublabel, Icon, exact }) => {
          const isActive = exact
            ? location.pathname === path
            : location.pathname.startsWith(path)

          return (
            <NavLink
              key={path}
              to={path}
              id={`nav-${path === '/' ? 'live' : path.replace('/', '')}`}
              end={exact}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                padding: '9px 10px',
                marginBottom: 2,
                borderRadius: 0,
                textDecoration: 'none',
                position: 'relative',
                transition: 'background-color 150ms ease, border-color 150ms ease',
                backgroundColor: isActive
                  ? 'rgba(0,229,255,0.06)'
                  : 'transparent',
                borderLeft: `2px solid ${isActive ? 'var(--color-electric-blue)' : 'transparent'}`,
              }}
              onMouseEnter={(e) => {
                if (!isActive)
                  (e.currentTarget as HTMLAnchorElement).style.backgroundColor =
                    'rgba(255,255,255,0.03)'
              }}
              onMouseLeave={(e) => {
                if (!isActive)
                  (e.currentTarget as HTMLAnchorElement).style.backgroundColor =
                    'transparent'
              }}
            >
              {/* Icon */}
              <Icon
                size={14}
                style={{
                  color: isActive
                    ? 'var(--color-electric-blue)'
                    : 'var(--color-text-secondary)',
                  flexShrink: 0,
                  transition: 'color 150ms ease',
                }}
              />

              {/* Labels */}
              <div style={{ overflow: 'hidden' }}>
                <p
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 11,
                    fontWeight: isActive ? 600 : 400,
                    color: isActive
                      ? 'var(--color-text-primary)'
                      : 'var(--color-text-secondary)',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    transition: 'color 150ms ease',
                  }}
                >
                  {label}
                </p>
                <p
                  style={{
                    fontSize: 10,
                    color: 'var(--color-text-muted)',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {sublabel}
                </p>
              </div>

              {/* Active glow edge */}
              {isActive && (
                <div
                  aria-hidden
                  style={{
                    position: 'absolute',
                    left: 0,
                    top: 0,
                    bottom: 0,
                    width: 2,
                    backgroundColor: 'var(--color-electric-blue)',
                    boxShadow: '2px 0 8px rgba(0,229,255,0.5)',
                  }}
                />
              )}
            </NavLink>
          )
        })}
      </nav>

      {/* ── System status footer ─────────────────────────────────────────── */}
      <footer
        style={{
          padding: '12px 16px',
          borderTop: '1px solid var(--color-border-subtle)',
          flexShrink: 0,
        }}
      >
        <p className="section-label" style={{ marginBottom: 8 }}>System Status</p>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
          {[
            { label: 'Kafka Broker', state: 'online' },
            { label: 'TimescaleDB',  state: 'online' },
            { label: 'WebSocket',    state: wsDisconnected ? 'offline' : 'online' },
          ].map(({ label, state }) => (
            <div
              key={label}
              style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}
            >
              <span style={{ fontSize: 10, color: 'var(--color-text-muted)' }}>{label}</span>
              <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                <span
                  className={`status-dot ${state === 'online' ? 'green' : 'red'} ${state === 'online' ? 'animate-pulse-glow' : ''}`}
                />
                <span
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 9,
                    color:
                      state === 'online'
                        ? 'var(--color-neon-green)'
                        : 'var(--color-crimson)',
                    letterSpacing: '0.06em',
                    textTransform: 'uppercase',
                  }}
                >
                  {state}
                </span>
              </div>
            </div>
          ))}
        </div>
      </footer>
    </aside>
  )
}
