import { createContext, useContext, useState, ReactNode } from 'react'

export interface LiveMetrics {
  eventsSec: number | string
  kafkaLag: number | string
  dbWrites: number | string
  dlqErrors: number | string
}

interface MetricsContextType {
  metrics: LiveMetrics
  setMetrics: (metrics: LiveMetrics | ((prev: LiveMetrics) => LiveMetrics)) => void
}

const MetricsContext = createContext<MetricsContextType | null>(null)

export function MetricsProvider({ children }: { children: ReactNode }) {
  const [metrics, setMetrics] = useState<LiveMetrics>({
    eventsSec: '—',
    kafkaLag: '—',
    dbWrites: '—',
    dlqErrors: '—',
  })

  return (
    <MetricsContext.Provider value={{ metrics, setMetrics }}>
      {children}
    </MetricsContext.Provider>
  )
}

export function useMetrics() {
  const ctx = useContext(MetricsContext)
  if (!ctx) throw new Error('useMetrics must be used within MetricsProvider')
  return ctx
}
