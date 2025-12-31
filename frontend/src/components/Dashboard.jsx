import React from 'react'
import MetricsDeck from './MetricsDeck.jsx'
import ActivityFeed from './ActivityFeed.jsx'
import { Sparkles, ArrowRight, LayoutDashboard, Cpu } from 'lucide-react'

export default function Dashboard({ onEnterChat }) {
  return (
    <div className="flex-1 h-full overflow-y-auto bg-app-bg p-4 md:p-6 lg:p-10 fade-in scrollbar-thin">
      <div className="max-w-7xl mx-auto space-y-6 md:space-y-10">
        
        {/* Header */}
        <header className="flex justify-between items-start border-b border-border pb-6 md:pb-8">
          <div className="flex gap-4 items-center">
            <div className="p-3 rounded-2xl bg-gradient-premium shadow-glow shrink-0">
              <LayoutDashboard size={24} className="text-white md:w-7 md:h-7" />
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2 mb-1 flex-wrap">
                <h1 className="text-2xl md:text-3xl font-bold text-text-primary tracking-tight">Systems Overview</h1>
                <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-surface border border-border text-text-muted shrink-0">v2.1.0-SOTA</span>
              </div>
              <p className="text-sm md:text-base text-text-secondary font-medium leading-relaxed">Monitoring the intelligent document processing pipeline & RAG engine.</p>
            </div>
          </div>
          
          <button 
            onClick={onEnterChat}
            className="hidden md:flex items-center gap-2 px-6 py-3 bg-gradient-premium hover:scale-105 active:scale-95 text-white rounded-xl font-bold text-sm transition-all shadow-glow"
          >
            <Sparkles size={18} />
            <span>Launch Chat</span>
          </button>
        </header>

        {/* Metrics Row */}
        <section>
          <div className="flex items-center gap-2 mb-4">
            <Cpu size={16} className="text-accent" />
            <h2 className="text-[11px] font-black uppercase tracking-[0.5em] text-text-muted">Metrics</h2>
          </div>
          <MetricsDeck />
          
          {/* Mobile Launch Chat Button (Moved from bottom) */}
          <div className="flex md:hidden mb-10">
            <button 
              onClick={onEnterChat}
              className="w-full flex items-center justify-center gap-2 px-8 py-4 bg-gradient-premium text-white rounded-2xl font-bold transition-all shadow-glow active:scale-95"
            >
              <Sparkles size={20} />
              <span>Ask a Question</span>
            </button>
          </div>
        </section>

        {/* Main Grid: Full Width Activity Feed */}
        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="inline-flex items-center gap-2.5 px-3 py-1.5 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 shadow-[0_0_15px_-3px_rgba(16,185,129,0.3)] backdrop-blur-sm">
              <div className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </div>
              <h2 className="text-[10px] font-black uppercase tracking-widest text-emerald-100/90">Execution Stream</h2>
            </div>
            <button 
              onClick={() => window.dispatchEvent(new CustomEvent('activity:refresh'))}
              className="text-[10px] font-bold text-accent hover:text-white transition-colors"
            >
              Refresh
            </button>
          </div>
          
          <div className="h-[400px] md:h-[550px] bg-card-bg/50 backdrop-blur-sm border border-border rounded-xl p-4 md:p-6 flex flex-col shadow-glass group hover:border-accent/20 transition-all">
            <ActivityFeed />
          </div>
        </section>

      </div>
    </div>
  )
}