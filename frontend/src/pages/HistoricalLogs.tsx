import { ScrollText, Search, Download } from 'lucide-react'
import { PageHeader } from './LiveSystemMap'

/**
 * PAGE: Historical Logs  ( route: "/audit" )
 *
 * Phase 5 stub — audit trail table.
 * Will stream event history from the TimescaleDB analytics endpoint with
 * server-side pagination and time-range filtering.
 */

// Placeholder rows to validate table layout
const PLACEHOLDER_ROWS = [
  { ts: '2026-06-24 06:02:06', level: 'WARN',  source: 'timescaledb', message: 'failed to launch job 3 "Job History Log Retention Policy": out of background workers' },
  { ts: '2026-06-24 06:01:00', level: 'INFO',  source: 'kafka',       message: 'Cached leader info LeaderAndIsrPartitionState' },
  { ts: '2026-06-24 06:00:45', level: 'INFO',  source: 'celery',      message: 'Task pulse.tasks.persist_batch[abc-123] received' },
  { ts: '2026-06-24 05:59:33', level: 'INFO',  source: 'api',         message: 'POST /api/v1/events — 201 Created (3.2ms)' },
  { ts: '2026-06-24 05:58:11', level: 'ERROR', source: 'kafka',       message: 'Consumer group rebalance triggered — max.poll.interval exceeded' },
]

const LEVEL_COLOR: Record<string, string> = {
  INFO:  'var(--color-electric-blue)',
  WARN:  '#FFB800',
  ERROR: 'var(--color-crimson)',
  DEBUG: 'var(--color-text-muted)',
}

export default function HistoricalLogs() {
  return (
    <div
      id="page-historical-logs"
      style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}
    >
      <PageHeader
        icon={<ScrollText size={14} style={{ color: 'var(--color-electric-blue)' }} />}
        title="Historical Logs"
        badge="AUDIT"
        badgeColor="var(--color-electric-blue)"
        description="Paginated audit trail from TimescaleDB — ingested events, pipeline errors, system warnings"
      />

      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', padding: 16, gap: 12 }}>

        {/* Filter bar */}
        <div
          className="panel"
          style={{ padding: '8px 12px', display: 'flex', alignItems: 'center', gap: 10 }}
        >
          {/* Search stub */}
          <Search size={12} style={{ color: 'var(--color-text-muted)', flexShrink: 0 }} />
          <div
            style={{
              flex: 1,
              height: 28,
              border: '1px solid var(--color-border-subtle)',
              backgroundColor: 'var(--color-black)',
              display: 'flex',
              alignItems: 'center',
              paddingInline: 10,
            }}
          >
            <span style={{ fontSize: 11, color: 'var(--color-text-muted)', fontFamily: 'var(--font-mono)' }}>
              Filter logs…
            </span>
          </div>

          {/* Level filter stubs */}
          <div style={{ display: 'flex', gap: 4 }}>
            {['ALL', 'INFO', 'WARN', 'ERROR'].map((lvl) => (
              <button
                id={`filter-level-${lvl.toLowerCase()}`}
                key={lvl}
                type="button"
                style={{
                  padding: '3px 8px',
                  backgroundColor: lvl === 'ALL' ? 'rgba(0,229,255,0.1)' : 'transparent',
                  border: `1px solid ${lvl === 'ALL' ? 'var(--color-electric-blue)' : 'var(--color-border)'}`,
                  color: lvl === 'ALL' ? 'var(--color-electric-blue)' : 'var(--color-text-secondary)',
                  fontFamily: 'var(--font-mono)',
                  fontSize: 9,
                  fontWeight: 600,
                  letterSpacing: '0.08em',
                  cursor: 'pointer',
                }}
              >
                {lvl}
              </button>
            ))}
          </div>

          {/* Export stub */}
          <button
            id="btn-export-logs"
            type="button"
            style={{
              display: 'flex', alignItems: 'center', gap: 5,
              padding: '4px 10px',
              backgroundColor: 'transparent',
              border: '1px solid var(--color-border)',
              color: 'var(--color-text-secondary)',
              fontFamily: 'var(--font-mono)',
              fontSize: 9, fontWeight: 600, letterSpacing: '0.06em',
              cursor: 'pointer',
            }}
          >
            <Download size={10} />
            EXPORT
          </button>
        </div>

        {/* Log table */}
        <div
          className="panel"
          style={{ flex: 1, overflow: 'auto', minHeight: 0 }}
        >
          {/* Table header */}
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: '160px 60px 110px 1fr',
              gap: 0,
              padding: '6px 12px',
              borderBottom: '1px solid var(--color-border)',
              position: 'sticky',
              top: 0,
              backgroundColor: 'var(--color-card)',
              zIndex: 1,
            }}
          >
            {['Timestamp', 'Level', 'Source', 'Message'].map((col) => (
              <span key={col} className="section-label">{col}</span>
            ))}
          </div>

          {/* Table rows */}
          {PLACEHOLDER_ROWS.map((row, i) => (
            <div
              key={i}
              style={{
                display: 'grid',
                gridTemplateColumns: '160px 60px 110px 1fr',
                gap: 0,
                padding: '7px 12px',
                borderBottom: '1px solid var(--color-border-subtle)',
                alignItems: 'start',
              }}
              onMouseEnter={(e) => {
                (e.currentTarget as HTMLDivElement).style.backgroundColor = 'rgba(255,255,255,0.02)'
              }}
              onMouseLeave={(e) => {
                (e.currentTarget as HTMLDivElement).style.backgroundColor = 'transparent'
              }}
            >
              {/* Timestamp */}
              <span
                className="tabular-nums"
                style={{ fontSize: 10, color: 'var(--color-text-muted)' }}
              >
                {row.ts}
              </span>

              {/* Level */}
              <span
                style={{
                  fontFamily: 'var(--font-mono)',
                  fontSize: 9,
                  fontWeight: 700,
                  color: LEVEL_COLOR[row.level] ?? 'var(--color-text-secondary)',
                  letterSpacing: '0.06em',
                }}
              >
                {row.level}
              </span>

              {/* Source */}
              <span
                style={{
                  fontFamily: 'var(--font-mono)',
                  fontSize: 10,
                  color: 'var(--color-text-secondary)',
                }}
              >
                {row.source}
              </span>

              {/* Message */}
              <span
                style={{
                  fontSize: 10,
                  color: 'var(--color-text-primary)',
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                }}
                title={row.message}
              >
                {row.message}
              </span>
            </div>
          ))}

          {/* Pagination stub */}
          <div
            style={{
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              gap: 8,
              padding: '10px',
              borderTop: '1px solid var(--color-border-subtle)',
            }}
          >
            {['«', '‹', '1', '›', '»'].map((pg) => (
              <button
                id={`page-btn-${pg}`}
                key={pg}
                type="button"
                style={{
                  width: 26, height: 26,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  backgroundColor: pg === '1' ? 'rgba(0,229,255,0.1)' : 'transparent',
                  border: `1px solid ${pg === '1' ? 'var(--color-electric-blue)' : 'var(--color-border)'}`,
                  color: pg === '1' ? 'var(--color-electric-blue)' : 'var(--color-text-secondary)',
                  fontFamily: 'var(--font-mono)',
                  fontSize: 10,
                  cursor: 'pointer',
                }}
              >
                {pg}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
