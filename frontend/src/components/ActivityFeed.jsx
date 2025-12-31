import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { FileText, MessageSquare, RefreshCw, Clock, ChevronDown, ChevronRight, Layers } from 'lucide-react'
import { api } from '../api/client.js'
import { motion, AnimatePresence } from 'framer-motion'

function getStatusStyles(status) {
  const s = (status || '').toUpperCase()
  if (['OK', 'PROCESSED', 'INDEXED', 'EMBEDDED', 'STORED', 'SUCCESS', 'READY', 'DONE'].includes(s)) {
    return 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20'
  }
  if (['WARN', 'WARNING', 'REJECTED'].includes(s)) {
    return 'text-amber-400 bg-amber-400/10 border-amber-400/20'
  }
  if (['FAIL', 'ERROR', 'FAILED'].includes(s)) {
    return 'text-red-400 bg-red-400/10 border-red-400/20'
  }
  return 'text-zinc-400 bg-white/5 border-white/10'
}

function PipelineGroup({ group }) {
  const [expanded, setExpanded] = useState(false)
  const latest = group.latest
  const events = group.events
  const isError = events.some(e => ['FAIL', 'ERROR', 'FAILED'].includes((e.status || '').toUpperCase()))
  const isDone = latest.stage === 'EMBEDDED' && !isError
  
  // Pipeline progress estimation
  const stages = ['STORED', 'NORMALIZED', 'EXTRACTED', 'CHUNKED', 'EMBEDDED']
  const currentStageIdx = stages.indexOf(latest.stage)
  const progress = currentStageIdx >= 0 ? ((currentStageIdx + 1) / stages.length) * 100 : 100
  
  // Find best title from any event in the group
  const sourceEvent = events.find(e => e.document_name) || latest
  const title = sourceEvent.document_name || latest.title || 'Processing Document'
  
  return (
    <div className={`
      rounded-xl border transition-all duration-500 group overflow-hidden
      ${isError ? 'bg-red-500/5 border-red-500/20 shadow-[0_0_15px_-5px_rgba(239,68,68,0.2)]' : 
        isDone ? 'bg-zinc-900/40 border-white/10 shadow-glass' : 
        'bg-zinc-900/50 border-white/5 hover:border-accent/30 shadow-glass'}
    `}>
      {/* Header / Summary Card */}
      <div 
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-4 p-4 cursor-pointer hover:bg-white/5 transition-colors"
      >
        <div className={`mt-1 p-2 rounded-lg h-fit shrink-0 ring-1 ring-white/10 ${isError ? 'bg-red-500/10 text-red-400' : isDone ? 'bg-emerald-500/10 text-emerald-400' : 'bg-blue-500/10 text-blue-400'}`}>
          <Layers size={18} />
        </div>
        
        <div className="flex-1 min-w-0 font-mono">
          <div className="flex justify-between items-center mb-1">
            <h4 className={`font-bold text-sm truncate pr-2 ${isError ? 'text-red-300' : 'text-zinc-100'}`}>
              {title}
            </h4>
            <span className="text-[10px] text-zinc-500 opacity-60">
              {new Date(latest.created_at).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}
            </span>
          </div>
          
          <div className="flex items-center gap-3">
             <div className="flex-1 h-1 bg-zinc-800/50 rounded-full overflow-hidden">
               <div 
                 className={`h-full rounded-full transition-all duration-700 ease-out ${isError ? 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.5)]' : isDone ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.4)]' : 'bg-blue-500 shadow-[0_0_8px_rgba(59,130,246,0.5)]'}`} 
                 style={{ width: `${progress}%` }}
               />
             </div>
             <span className="text-[9px] font-black uppercase tracking-tighter opacity-70 whitespace-nowrap text-zinc-400">
               {latest.stage} • {progress.toFixed(0)}%
             </span>
          </div>
        </div>
        
        <div className={`transition-transform duration-300 ${expanded ? 'rotate-180' : ''} text-zinc-600`}>
          <ChevronDown size={16} />
        </div>
      </div>

      {/* Expanded History */}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0 }}
            animate={{ height: 'auto' }}
            exit={{ height: 0 }}
            className="border-t border-white/5 bg-black/40"
          >
            <div className="p-3 space-y-1.5 font-mono">
              {events.map((ev) => (
                <div key={ev.id} className="flex items-center gap-3 text-[11px] opacity-80 hover:opacity-100 transition-opacity">
                  <div className="text-zinc-600 shrink-0">
                    [{new Date(ev.created_at).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'})}]
                  </div>
                  <div className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${getStatusStyles(ev.status)}`}>
                    {ev.status}
                  </div>
                  <div className="flex-1 text-zinc-400 truncate">
                    CMD::{ev.stage} --source="{ev.details?.filename || 'system'}"
                  </div>
                </div>
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

function ActivityItem({ item }) {
  const isQuery = item.type === 'query'
  const icon = isQuery ? <MessageSquare size={18} className="text-purple-400" /> : <RefreshCw size={18} className="text-zinc-400" />
  const statusStyles = getStatusStyles(item.status)
  const details = item.details || {}
  
  return (
    <div className="flex gap-4 p-4 rounded-xl bg-zinc-900/30 border border-white/5 hover:border-accent/30 transition-all duration-300 group animate-fade-in-up font-mono">
      <div className="mt-1 p-2 rounded-lg bg-white/5 h-fit shrink-0 ring-1 ring-white/5 group-hover:ring-accent/20 transition-all">
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex justify-between items-start gap-4">
          <h4 className="text-sm font-bold text-zinc-300 truncate" title={item.title}>
            <span className="text-accent opacity-50 mr-2">&gt;</span>
            {item.title}
          </h4>
          <span className="text-[10px] text-zinc-600 shrink-0 whitespace-nowrap">
            {new Date(item.created_at).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'})}
          </span>
        </div>
        
        <div className="flex flex-wrap items-center gap-2 mt-2">
          <span className={`text-[9px] font-black px-2 py-0.5 rounded border ${statusStyles} uppercase tracking-widest`}>
            {item.status}
          </span>
          
          <div className="flex items-center gap-1.5 ml-auto">
            {details.latency_ms && (
              <span className="text-[10px] text-zinc-500">
                LATENCY::<span className="text-zinc-300">{details.latency_ms}ms</span>
              </span>
            )}
            {details.tokens_out && (
              <span className="text-[10px] text-zinc-500">
                TOKENS::<span className="text-purple-400">{details.tokens_out}</span>
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default function ActivityFeed() {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [filter, setFilter] = useState('ALL') // ALL | INGEST | QUERY | ERROR

  const load = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true)
    try {
      const data = await api.get(`/api/dashboard/activity?limit=100&filter=${filter}`) // Fetch more to allow grouping
      setItems(data || [])
    } catch (e) {
      console.error("Failed to load activity", e)
    } finally {
      setLoading(false)
      if (isRefresh) setRefreshing(false)
    }
  }, [filter])

  useEffect(() => {
    load()
    const interval = setInterval(() => load(), 5000)
    const onManualRefresh = () => load(true)
    window.addEventListener('activity:refresh', onManualRefresh)
    return () => {
      clearInterval(interval)
      window.removeEventListener('activity:refresh', onManualRefresh)
    }
  }, [load])

  // Grouping Logic
  const groupedItems = useMemo(() => {
    if (filter === 'QUERY') return items // Don't group queries
    
    const out = []
    const docMap = new Map() // doc_id -> index in out

    items.forEach(item => {
      // If it's a pipeline event with a doc_id
      if (item.doc_id && (item.type === 'ingest' || item.type === 'process')) {
        if (docMap.has(item.doc_id)) {
          // Add to existing group
          const idx = docMap.get(item.doc_id)
          out[idx].events.push(item)
        } else {
          // New group
          const group = {
            type: 'pipeline_group',
            id: `group-${item.doc_id}`,
            doc_id: item.doc_id,
            latest: item,
            events: [item]
          }
          out.push(group)
          docMap.set(item.doc_id, out.length - 1)
        }
      } else {
        // Standalone item (query, system error without doc_id)
        out.push(item)
      }
    })
    return out
  }, [items, filter])

  return (
    <div className="relative h-full flex flex-col min-h-0">
      {/* Filter Tabs */}
      <div className="flex items-center gap-2 mb-4 overflow-x-auto no-scrollbar">
        {['ALL', 'INGEST', 'QUERY', 'ERROR'].map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-3 py-1.5 rounded-lg text-[10px] font-bold uppercase tracking-wider transition-all border ${
              filter === f 
                ? 'bg-accent/20 border-accent/30 text-accent shadow-glow' 
                : 'bg-surface border-transparent text-zinc-500 hover:text-zinc-300 hover:bg-surface/80'
            }`}
          >
            {f === 'INGEST' ? 'Ingestion' : f === 'QUERY' ? 'Queries' : f === 'ERROR' ? 'Alerts' : 'All Events'}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto min-h-0 pr-2 space-y-3 scrollbar-thin scrollbar-thumb-white/10 scrollbar-track-transparent">
        {(loading || refreshing) && items.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-zinc-500 gap-3">
            <RefreshCw size={24} className="animate-spin opacity-50" />
            <p className="text-sm">Connecting to feed...</p>
          </div>
        ) : groupedItems.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-zinc-500 gap-2">
            <div className="p-4 rounded-full bg-white/5 text-zinc-600">
              <Clock size={24} />
            </div>
            <p className="text-sm">No recent activity found</p>
          </div>
        ) : (
          groupedItems.map((item) => (
            item.type === 'pipeline_group' 
              ? <PipelineGroup key={item.id} group={item} />
              : <ActivityItem key={item.id} item={item} />
          ))
        )}
      </div>
    </div>
  )
}
