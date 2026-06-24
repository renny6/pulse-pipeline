type Route = 'ingest' | 'check_rate' | 'queue' | 'persist'
type Status = 'success' | 'rate_limited' | 'processing'

interface Particle {
  id: number
  startX: number
  startY: number
  targetX: number
  targetY: number
  color: string
  progress: number
  speed: number
  length: number
}

export class CanvasEngine {
  private canvas: HTMLCanvasElement
  private ctx: CanvasRenderingContext2D
  private particles: Particle[] = []
  private animationFrameId: number = 0
  private nextId: number = 0

  constructor(canvas: HTMLCanvasElement) {
    this.canvas = canvas
    this.ctx = canvas.getContext('2d')!
    this.resize()
    window.addEventListener('resize', this.resize)
    this.loop()
  }

  public destroy() {
    window.removeEventListener('resize', this.resize)
    cancelAnimationFrame(this.animationFrameId)
  }

  private resize = () => {
    this.canvas.width = this.canvas.clientWidth
    this.canvas.height = this.canvas.clientHeight
  }

  public spawnParticle(route: Route, status: Status) {
    let startX = 0, startY = 0, endX = 0, endY = 0
    let color = '#00FF41' // Green for success/202

    if (status === 'rate_limited') color = '#FF003C' // Red for 429
    if (status === 'processing') color = '#00E5FF' // Blue for processing/Kafka

    // Coordinate mapping based on React Flow node layout:
    // Traffic Generator: (50, 200) width 180
    // FastAPI Gateway: (300, 200) width 180
    // Redis Rate Limiter: (300, 50) width 180
    // Kafka Event Bus: (550, 200) width 180
    // TimescaleDB: (800, 200) width 180
    // Node height is approx 46px.
    
    switch (route) {
      case 'ingest':
        startX = 230; startY = 223; endX = 300; endY = 223; break;
      case 'check_rate':
        // Shoot from Gateway up to Redis
        startX = 390; startY = 200; endX = 390; endY = 96; break;
      case 'queue':
        startX = 480; startY = 223; endX = 550; endY = 223; break;
      case 'persist':
        startX = 730; startY = 223; endX = 800; endY = 223; break;
    }

    this.particles.push({
      id: this.nextId++,
      startX,
      startY,
      targetX: endX,
      targetY: endY,
      color,
      progress: 0,
      // Randomize speed slightly for a more organic feel
      speed: 0.03 + Math.random() * 0.02,
      length: 15
    })
  }

  private loop = () => {
    this.update()
    this.draw()
    this.animationFrameId = requestAnimationFrame(this.loop)
  }

  private update() {
    for (let i = this.particles.length - 1; i >= 0; i--) {
      const p = this.particles[i]
      p.progress += p.speed
      if (p.progress >= 1) {
        this.particles.splice(i, 1)
      }
    }
  }

  private draw() {
    this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height)

    // Robustly sync Canvas coordinate space with React Flow's viewport transform.
    // This perfectly aligns our hardcoded logical coordinates regardless of
    // fitView scaling or window resizing.
    const viewport = document.querySelector('.react-flow__viewport') as HTMLElement
    if (viewport) {
      const style = window.getComputedStyle(viewport)
      const matrix = new DOMMatrixReadOnly(style.transform)
      this.ctx.setTransform(matrix.a, matrix.b, matrix.c, matrix.d, matrix.e, matrix.f)
    }

    for (const p of this.particles) {
      const currentX = p.startX + (p.targetX - p.startX) * p.progress
      const currentY = p.startY + (p.targetY - p.startY) * p.progress

      const dx = p.targetX - p.startX
      const dy = p.targetY - p.startY
      const dist = Math.sqrt(dx * dx + dy * dy)
      const dirX = dist === 0 ? 0 : dx / dist
      const dirY = dist === 0 ? 0 : dy / dist
      
      const tailX = currentX - dirX * p.length
      const tailY = currentY - dirY * p.length

      this.ctx.beginPath()
      this.ctx.moveTo(tailX, tailY)
      this.ctx.lineTo(currentX, currentY)
      this.ctx.strokeStyle = p.color
      this.ctx.lineWidth = 3
      this.ctx.lineCap = 'round'
      
      this.ctx.shadowBlur = 8
      this.ctx.shadowColor = p.color
      
      this.ctx.stroke()
      
      // Reset shadow
      this.ctx.shadowBlur = 0
    }
    
    // Reset transform to identity so the next clearRect covers the entire screen
    this.ctx.setTransform(1, 0, 0, 1, 0, 0)
  }
}
