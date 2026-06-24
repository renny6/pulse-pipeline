import { Info } from 'lucide-react'
import { PageHeader } from './LiveSystemMap'

export default function About() {
  return (
    <div
      id="page-about"
      className="flex flex-col h-full overflow-hidden"
    >
      <PageHeader
        icon={<Info size={14} className="text-electric-blue" />}
        title="About Pulse"
        badge="INFO"
        badgeColor="var(--color-electric-blue)"
        description="Project architecture and technical overview"
      />

      <div className="flex-1 overflow-auto p-6 flex flex-col gap-8">
        {/* The Challenge Section */}
        <section className="panel p-6 glow-blue max-w-4xl">
          <h2 className="text-xl font-mono font-semibold text-text-primary mb-4 border-b border-border-subtle pb-2">
            The Challenge
          </h2>
          <p className="text-sm text-text-secondary leading-relaxed">
            Real-time event streaming at scale presents profound engineering challenges. When systems experience massive traffic spikes, traditional synchronous architectures can easily become overwhelmed, leading to dropped requests, database lockups, and complete service degradation. Maintaining high-throughput ingestion while ensuring data durability and providing real-time visibility into the system's state requires a specialized, decoupled approach to backpressure and scaling.
          </p>
        </section>

        {/* Our Solution Section */}
        <section className="panel p-6 max-w-4xl">
          <h2 className="text-xl font-mono font-semibold text-text-primary mb-4 border-b border-border-subtle pb-2">
            Our Solution
          </h2>
          <p className="text-sm text-text-secondary leading-relaxed mb-4">
            Pulse Pipeline is a resilient stream processing engine designed to absorb immense traffic loads without faltering. By separating ingestion from persistence, we ensure that data is safely buffered and efficiently stored.
          </p>
          <ul className="list-disc list-outside ml-5 text-sm text-text-secondary flex flex-col gap-3">
            <li>
              <strong className="text-text-primary font-mono">High-Throughput Ingestion:</strong> A FastAPI gateway rapidly accepts incoming telemetry, immediately validates it, and relies on an in-memory Redis layer for ultra-fast rate limiting and sliding-window throttling.
            </li>
            <li>
              <strong className="text-text-primary font-mono">Distributed Architecture:</strong> Apache Kafka serves as the elastic buffer, decoupling the fast API from the storage layer. A fleet of Celery workers consumes these partitions to execute bulk upserts into TimescaleDB, optimizing disk I/O for time-series data.
            </li>
            <li>
              <strong className="text-text-primary font-mono">Real-Time Observability:</strong> A tactical React dashboard connects via WebSockets to visualize the entire topology. It features anti-backpressure mechanisms to prevent browser lockups, causality diagnostics for immediate error surface, and an end-to-end latency tracker to measure actual pipeline performance.
            </li>
          </ul>
        </section>
      </div>
    </div>
  )
}
