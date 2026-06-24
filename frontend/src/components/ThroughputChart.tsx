import { useEffect, useState } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts'

interface DataPoint {
  time: string
  accepted: number
  blocked: number
}

export function ThroughputChart() {
  const [data, setData] = useState<DataPoint[]>([])

  useEffect(() => {
    const handleWsMessage = (e: Event) => {
      const customEvent = e as CustomEvent
      const payload = customEvent.detail

      // We approximate the RPS since the window is 100ms
      const accepted = (payload.accepted || 0) * 10
      const blocked = (payload.blocked || 0) * 10

      const now = new Date()
      const timeStr = `${now.getHours()}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`

      setData(prev => {
        const newData = [...prev, { time: timeStr, accepted, blocked }]
        // Keep last 30 data points for the chart window
        if (newData.length > 30) {
          return newData.slice(newData.length - 30)
        }
        return newData
      })
    }

    window.addEventListener('ws:message', handleWsMessage)
    return () => {
      window.removeEventListener('ws:message', handleWsMessage)
    }
  }, [])

  return (
    <div className="w-full h-full flex flex-col items-center justify-center relative p-4">
      {data.length === 0 ? (
        <p className="text-xs text-text-secondary font-mono tracking-widest absolute">
          WAITING FOR METRICS...
        </p>
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
            <XAxis
              dataKey="time"
              stroke="#666"
              tick={{ fill: '#888', fontSize: 10, fontFamily: 'var(--font-mono)' }}
              tickMargin={10}
              minTickGap={20}
            />
            <YAxis
              stroke="#666"
              tick={{ fill: '#888', fontSize: 10, fontFamily: 'var(--font-mono)' }}
              width={40}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'var(--color-black)',
                borderColor: 'var(--color-border)',
                fontFamily: 'var(--font-mono)',
                fontSize: 12
              }}
              itemStyle={{ fontFamily: 'var(--font-mono)' }}
            />
            <Legend
              wrapperStyle={{ fontFamily: 'var(--font-mono)', fontSize: 11, paddingTop: 10 }}
              iconType="circle"
            />
            <Line
              type="monotone"
              dataKey="accepted"
              stroke="var(--color-electric-blue)"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
              name="Accepted (RPS)"
            />
            <Line
              type="monotone"
              dataKey="blocked"
              stroke="var(--color-crimson)"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
              name="Blocked (RPS)"
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
