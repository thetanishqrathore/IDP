import React, { useState, useEffect } from 'react'
import { FileStack, Activity, Server, Zap } from 'lucide-react'
import { api } from '../api/client.js'

function Sparkline({ data }) {
  if (!data || data.length === 0) return null
  // Simple SVG bar chart
  const max = Math.max(...data.map(d => d.count)) || 1
  const bars = data.slice(-12).map((d, i) => {
    const h = (d.count / max) * 24
    return (
      <rect 
        key={i}
        x={i * 6} 
        y={24 - h} 
        width={4} 
        height={h} 
        rx={1}
        className="fill-accent/50 group-hover:fill-accent transition-colors duration-300"
      />
    )
  })
  
  return (
    <div className="h-6 flex items-end gap-[2px] mt-2 opacity-60 group-hover:opacity-100 transition-opacity">
       <svg height="24" width="72" className="overflow-visible">
         {bars}
       </svg>
    </div>
  )
}

function MetricCard({ label, value, icon, trend, status, history, isChunk }) {
  return (
    <div className={`
      p-5 rounded-xl border relative overflow-hidden group 
      hover:scale-[1.02] hover:shadow-glow transition-all duration-300 cursor-default shadow-glass
      ${isChunk ? 'bg-gradient-to-br from-card-bg/90 to-surface/50 border-border/60 hover:border-purple-500/30' : 'bg-card-bg/80 border-border hover:border-accent/30'}
    `}>
      <div className={`absolute -top-2 -right-2 p-4 opacity-5 group-hover:opacity-20 group-hover:scale-110 group-hover:-rotate-6 transition-all duration-700 ${isChunk ? 'text-purple-500' : 'text-accent'}`}>
        {React.cloneElement(icon, { size: 64, strokeWidth: 1 })}
      </div>
      
      <div className="relative z-10">
        <div className="flex items-center gap-2 mb-3">
          <div className={`p-2 rounded-lg border group-hover:scale-110 transition-transform duration-500 ${isChunk ? 'bg-purple-500/10 border-purple-500/20 text-purple-400' : 'bg-accent/10 border-accent/20 text-accent'}`}>
            {React.cloneElement(icon, { size: 18, strokeWidth: 1.5 })}
          </div>
          <div className="text-text-muted text-[10px] font-bold uppercase tracking-widest">{label}</div>
        </div>
        
        <div className="text-2xl font-bold text-text-primary tracking-tight mb-1">{value}</div>
        
        {history ? (
          <div className="flex items-end justify-between">
             <span className="text-[10px] text-text-muted font-medium italic opacity-80">{trend}</span>
             <Sparkline data={history} />
          </div>
        ) : (status || trend) && (
           <div className="flex items-center gap-1.5 mt-2">
             {status ? (
               <div className="flex items-center gap-1.5">
                 <div className="w-1.5 h-1.5 rounded-full bg-success shadow-[0_0_8px_rgba(16,185,129,0.6)] animate-pulse" />
                 <span className="text-[10px] text-success font-bold uppercase tracking-wider">{status}</span>
               </div>
             ) : (
               <span className="text-[10px] text-text-muted font-medium italic opacity-80">{trend}</span>
             )}
           </div>
        )}
      </div>
    </div>
  )
}

export default function MetricsDeck() {
  const [metrics, setMetrics] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        const data = await api.get('/api/dashboard/stats')
        setMetrics(data)
      } catch (e) {
        console.error("Stats load fail", e)
      }
    }
    load()
    const interval = setInterval(load, 15000)
    return () => clearInterval(interval)
  }, [])

  if (!metrics) return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8 animate-pulse">
       {[1,2,3,4].map(i => <div key={i} className="h-32 bg-card-bg/50 border border-border rounded-xl" />)}
    </div>
  )

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
      <MetricCard 
        label="Knowledge Base" 
        value={`${metrics.documents || 0} Docs`} 
        icon={<FileStack />} 
        trend="24h Activity"
        history={metrics.ingestion_history}
      />
      <MetricCard 
        label="Vectors Indexed" 
        value={(metrics.chunks || 0).toLocaleString()} 
        icon={<Activity />} 
        trend="Context Chunks"
        isChunk
      />
      <MetricCard 
        label="Pipeline" 
        value="Universal" 
        icon={<Zap />} 
        trend="Direct URL & Webhook"
      />
      <MetricCard 
        label="Engine Health" 
        value="Operational"
        icon={<Server />} 
        status="Healthy"
      />
    </div>
  )
}