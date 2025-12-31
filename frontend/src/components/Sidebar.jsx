import React, { useContext, useEffect, useMemo, useState, useRef } from 'react'
import { AppContext } from '../App.jsx'
import { api } from '../api/client.js'
import {
  UploadCloud,
  Trash2,
  ChevronsLeft,
  X,
  File,
  FileText,
  FileImage,
  FileArchive,
  FileVideo,
  FileAudio,
  FileCode,
  Layers,
  FolderOpen,
  Webhook,
  Terminal,
  Activity,
  Copy,
  Check,
  Globe,
  Zap,
  MoreHorizontal
} from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import FileExplorer from './FileExplorer.jsx'

export default function Sidebar({
  onSwitch,
  active,
  collapsed = false,
  onCollapse,
  isMobile = false,
  onClose,
}) {
  const {
    selectedDocs,
    setSelectedDocs,
    backendHealthy,
    setBackendHealthy,
    setPrefillText,
    pushToast,
    resetCounter,
  } = useContext(AppContext)
  const [docs, setDocs] = useState([])
  const [loading, setLoading] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [uploadQueue, setUploadQueue] = useState([])
  const [jobs, setJobs] = useState([])
  const dropRef = useRef(null)
  const [docNames, setDocNames] = useState(() => {
    try { return JSON.parse(localStorage.getItem('docNames') || '{}') } catch { return {} }
  })
  
  // File Explorer State
  const [explorerOpen, setExplorerOpen] = useState(false)

  function saveDocName(id, name) {
    setDocNames((prev) => {
      const next = { ...prev, [id]: name }
      try { localStorage.setItem('docNames', JSON.stringify(next)) } catch {}
      return next
    })
  }

  function labelFor(doc) {
    const name = (doc.uri || '').split('/').pop() || doc.doc_id
    return docNames[doc.doc_id] || name
  }

  function iconFor(name = '', mime = '') {
    const n = String(name).toLowerCase()
    const m = String(mime).toLowerCase()
    const has = (e) => n.endsWith(e)
    if (m.includes('pdf') || has('.pdf')) return <FileText size={16} strokeWidth={1.5} className="text-rose-500" />
    if (m.startsWith('image/') || ['.png', '.jpg', '.jpeg', '.gif', '.webp'].some(has))
      return <FileImage size={16} strokeWidth={1.5} className="text-purple-500" />
    if (['.json', '.xml', '.yml'].some(has)) return <FileCode size={16} strokeWidth={1.5} className="text-emerald-500" />
    return <File size={16} strokeWidth={1.5} className="text-zinc-400" />
  }

  // Detect source type for badge
  function sourceBadge(doc) {
    const src = (doc.meta?.source || '').toLowerCase()
    if (src.includes('url') || src.includes('http')) return { label: 'WEB', icon: <Globe size={10} strokeWidth={1.5} />, color: 'text-blue-400 bg-blue-400/10 border border-blue-400/20' }
    if (src.includes('n8n') || src.includes('webhook') || src.includes('zapier')) return { label: 'AUTO', icon: <Zap size={10} strokeWidth={1.5} />, color: 'text-orange-400 bg-orange-400/10 border border-orange-400/20' }
    return null // default manual
  }

  useEffect(() => {
    const init = async () => {
      try {
        const h = await api.health()
        setBackendHealthy(Boolean(h?.ok))
      } catch (e) {
        setBackendHealthy(false)
      }
      refresh()
    }
    init()
  }, [])

  useEffect(() => {
    if (!resetCounter) return
    setDocs([])
    setUploadQueue([])
    setJobs([])
    setNotice('')
    setError('')
    setDocNames(() => { try { localStorage.removeItem('docNames') } catch (_) {} return {} })
    refresh()
  }, [resetCounter])

  async function refresh() {
    setLoading(true)
    setError('')
    try {
      const res = await api.listDocs(200)
      setDocs(res.docs || [])
    } catch (e) {
      setError('Failed to load documents')
    } finally {
      setLoading(false)
    }
  }

  async function handleFilesJob(filesArr) {
    const files = Array.from(filesArr || [])
    if (!files.length) return
    setUploading(true)
    setError('')
    setNotice('')
    try {
      const res = await api.ingestJob(files)
      if (res?.job_id) {
        setJobs((js) => [{ job_id: res.job_id, status: 'PENDING', progress: 0, startedAt: Date.now() }, ...js])
        const seeds = (res.doc_ids || [])
          .map((doc_id, idx) => ({
            doc_id,
            name: files[idx]?.name || doc_id,
            status: 'QUEUED',
            stages: { normalized: false, extracted: false, embedded: false },
          }))
          .filter((seed) => Boolean(seed.doc_id))
        if (seeds.length) {
          setUploadQueue((q) => [...seeds, ...q].slice(0, 8))
          seeds.forEach((seed) => pollStatus(seed.doc_id))
          const first = seeds[0]
          if (first?.name) setPrefillText(`Summarize "${first.name}"`)
        }
        pollJob(res.job_id)
      } else {
        await refresh()
      }
    } catch (e) {
      setError('Upload failed')
    } finally {
      setUploading(false)
    }
  }

  async function onUpload(e) {
    const files = Array.from(e.target.files || [])
    await handleFilesJob(files)
    e.target.value = ''
  }

  useEffect(() => {
    const el = dropRef.current
    if (!el) return
    const onDrag = (e) => { e.preventDefault(); el.classList.add('border-accent', 'bg-accent/10') }
    const onLeave = (e) => { e.preventDefault(); el.classList.remove('border-accent', 'bg-accent/10') }
    const onDrop = (e) => {
      e.preventDefault()
      el.classList.remove('border-accent', 'bg-accent/10')
      handleFilesJob(e.dataTransfer.files)
    }
    el.addEventListener('dragover', onDrag); el.addEventListener('dragenter', onDrag)
    el.addEventListener('dragleave', onLeave); el.addEventListener('drop', onDrop)
    return () => {
      el.removeEventListener('dragover', onDrag); el.removeEventListener('dragenter', onDrag)
      el.removeEventListener('dragleave', onLeave); el.removeEventListener('drop', onDrop)
    }
  }, [])

  function pollStatus(doc_id) {
    let attempts = 0
    const maxAttempts = 180 // Increased to 180 (approx 6 mins) for slower high-quality parsing
    const iv = setInterval(async () => {
      attempts++
      try {
        const st = await api.docStatus(doc_id)
        const isError = (st.state || '').includes('FAIL') || (st.state || '').includes('ERROR')
        
        setUploadQueue((q) =>
          q.map((it) => {
            if (it.doc_id !== doc_id) return it
            return { 
              ...it, 
              status: isError ? 'ERROR' : it.status,
              stages: { normalized: st.normalized, extracted: st.extracted, embedded: st.embedded } 
            }
          })
        )
        
        if (isError) {
          clearInterval(iv)
          pushToast(`Processing failed for doc ${doc_id.slice(0,8)}`, 'error')
        }
        else if (st.embedded) {
          clearInterval(iv)
          pushToast('Ready to ask: indexing complete', 'info', 3500)
          refresh()
        }
      } catch (_) {}
      if (attempts >= maxAttempts) clearInterval(iv)
    }, 2000)
  }

  function pollJob(job_id) {
    let attempts = 0
    const maxAttempts = 360
    const iv = setInterval(async () => {
      attempts++
      try {
        const st = await api.jobStatus(job_id)
        setJobs((arr) => arr.map((j) => (j.job_id === job_id ? { ...j, status: st.status, progress: Number(st.progress || 0) } : j)))
        if (st.status === 'DONE') {
          clearInterval(iv); pushToast('Indexing complete', 'info', 3000); setJobs((arr) => arr.filter((j) => j.job_id !== job_id)); refresh()
        }
        if (st.status === 'ERROR') {
          clearInterval(iv); 
          // pushToast('Indexing failed', 'error', 4000); 
          setJobs((arr) => arr.filter((j) => j.job_id !== job_id))
        }
      } catch (_) {}
      if (attempts >= maxAttempts) clearInterval(iv)
    }, 2000)
  }

  async function remove(doc) {
    if (!confirm(`Delete ${doc.uri || doc.doc_id}?`)) return
    try { await api.deleteDoc(doc.doc_id); await refresh() } catch (_) {}
  }

  const wrapperClass = isMobile
    ? 'block fixed inset-0 z-50 bg-black/60 backdrop-blur-sm'
    : 'hidden lg:block shrink-0 h-full'

  return (
    <aside className={wrapperClass}>
      {isMobile && <div className="absolute inset-0" onClick={onClose} />}
      {isMobile ? (
        <motion.div
          className="absolute left-0 top-0 bottom-0 w-[85%] max-w-sm p-4 bg-[#0A0A0A] border-r border-zinc-800 h-full shadow-2xl bg-noise"
          initial={{ x: -20, opacity: 0 }}
          animate={{ x: 0, opacity: 1 }}
          exit={{ x: -20, opacity: 0 }}
          transition={{ type: 'spring', stiffness: 260, damping: 24 }}
        >
          <SidebarContent {...{ dropRef, error, uploadQueue, setUploadQueue, jobs, onUpload, docs, loading, active, refresh, iconFor, labelFor, remove, onClose, isMobile, sourceBadge, setExplorerOpen }} />
          <FileExplorer open={explorerOpen} onClose={() => setExplorerOpen(false)} onDelete={remove} />
        </motion.div>
      ) : (
        <AnimatedSidebarShell onCollapse={onCollapse}>
          <div className="p-3 bg-[#080808] border-r border-white/10 h-full min-w-[260px] max-w-[300px] flex flex-col backdrop-blur-xl relative bg-noise">
            <div className="absolute inset-0 bg-gradient-to-b from-white/[0.02] to-transparent pointer-events-none" />
            <div className="relative z-10 h-full flex flex-col">
              <SidebarContent {...{ dropRef, error, uploadQueue, setUploadQueue, jobs, onUpload, docs, loading, active, refresh, iconFor, labelFor, remove, onClose, isMobile, sourceBadge, setExplorerOpen }} />
              <FileExplorer open={explorerOpen} onClose={() => setExplorerOpen(false)} onDelete={remove} />
            </div>
          </div>
        </AnimatedSidebarShell>
      )}
    </aside>
  )
}

function AnimatedSidebarShell({ children, onCollapse }) {
  const [animOut, setAnimOut] = useState(false)
  useEffect(() => {
    function onReqCollapse() { setAnimOut(true) }
    window.addEventListener('sidebar:collapse', onReqCollapse)
    return () => window.removeEventListener('sidebar:collapse', onReqCollapse)
  }, [])
  return (
    <motion.div
      initial={{ x: -14, opacity: 0 }}
      animate={animOut ? { x: -24, opacity: 0 } : { x: 0, opacity: 1 }}
      transition={{ type: 'spring', stiffness: 280, damping: 26 }}
      onAnimationComplete={() => { if (animOut) onCollapse?.() }}
      className="h-full"
    >
      {children}
    </motion.div>
  )
}

function ConnectionWidget() {
  const [expanded, setExpanded] = useState(false)
  const [copied, setCopied] = useState(null)

  const copy = (text, key) => {
    navigator.clipboard.writeText(text)
    setCopied(key)
    setTimeout(() => setCopied(null), 2000)
  }

  // Infer base URL from current window
  const baseUrl = window.location.origin

  return (
    <div className="mb-4 rounded-lg border border-border/50 bg-card-bg/30 overflow-hidden">
      <button 
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between p-2.5 hover:bg-white/5 transition-colors"
      >
        <div className="flex items-center gap-2 text-xs font-medium text-zinc-400">
          <Terminal size={14} strokeWidth={1.5} className="text-sidebar-accent" />
          <span>Integrations</span>
        </div>
        <div className={`text-[9px] px-1.5 py-0.5 rounded-full border ${expanded ? 'bg-accent/10 text-accent border-accent/20' : 'bg-surface text-zinc-500 border-white/5'}`}>
          {expanded ? 'Hide' : 'Connect'}
        </div>
      </button>
      
      <AnimatePresence>
        {expanded && (
          <motion.div 
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="border-t border-border/50 bg-black/20"
          >
            <div className="p-2 space-y-2">
              <div>
                <div className="flex items-center justify-between text-[9px] text-zinc-500 mb-1">
                  <span>Chat Base URL (OpenAI)</span>
                  {copied === 'chat' && <span className="text-green-400 flex items-center gap-1"><Check size={10} strokeWidth={1.5}/> Copied</span>}
                </div>
                <div 
                  onClick={() => copy(`${baseUrl}/v1`, 'chat')}
                  className="flex items-center justify-between p-1.5 rounded bg-app-bg border border-white/5 cursor-pointer hover:border-accent/30 group"
                >
                  <code className="text-[9px] text-zinc-300 truncate font-mono">{baseUrl}/v1</code>
                  <Copy size={12} strokeWidth={1.5} className="text-zinc-600 group-hover:text-accent" />
                </div>
              </div>
              
              <div>
                <div className="flex items-center justify-between text-[9px] text-zinc-500 mb-1">
                  <span>Ingest URL (Webhook)</span>
                  {copied === 'ingest' && <span className="text-green-400 flex items-center gap-1"><Check size={10} strokeWidth={1.5}/> Copied</span>}
                </div>
                <div 
                  onClick={() => copy(`${baseUrl}/ingest/url`, 'ingest')}
                  className="flex items-center justify-between p-1.5 rounded bg-app-bg border border-white/5 cursor-pointer hover:border-accent/30 group"
                >
                  <code className="text-[9px] text-zinc-300 truncate font-mono">POST /ingest/url</code>
                  <Webhook size={12} strokeWidth={1.5} className="text-zinc-600 group-hover:text-accent" />
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

function SidebarContent({ dropRef, error, jobs, onUpload, docs, loading, refresh, iconFor, labelFor, remove, onClose, isMobile, sourceBadge, uploadQueue, setUploadQueue, setExplorerOpen }) {
  const displayedDocs = docs.slice(0, 5)

  return (
    <>
      <div className="flex items-center justify-between mb-6 px-1 pt-1">
        <div className="flex items-center gap-3 text-white font-bold tracking-tight">
          <div className="w-8 h-8 rounded-lg bg-accent/20 border border-accent/30 flex items-center justify-center shadow-glow">
            <Layers size={18} strokeWidth={2} className="text-accent" />
          </div>
          <span className="text-sm uppercase tracking-wider text-zinc-100">Dashboard</span>
        </div>
        {!isMobile && (
          <button
            onClick={() => window.dispatchEvent(new Event('sidebar:collapse'))}
            className="p-1.5 rounded-md text-zinc-500 hover:text-white hover:bg-white/5 transition-colors"
            title="Collapse sidebar"
          >
            <ChevronsLeft size={16} strokeWidth={1.5} />
          </button>
        )}
        {isMobile && (
          <button onClick={onClose} className="p-1.5 rounded-md text-zinc-500 hover:text-white hover:bg-white/5 transition-colors">
            <X size={18} strokeWidth={1.5} />
          </button>
        )}
      </div>

      <ConnectionWidget />

      {/* Live Processing Queue */}
      {uploadQueue.length > 0 && (
        <div className="mb-6 space-y-2">
          <div className="flex items-center justify-between px-1 mb-1.5">
             <span className="text-[9px] font-bold text-zinc-500 uppercase tracking-wider">Ingestion Stream</span>
             <button onClick={() => setUploadQueue([])} className="text-[9px] text-zinc-600 hover:text-zinc-400 transition-colors">Clear</button>
          </div>
          {uploadQueue.map((item, idx) => {
            const isDone = item.stages?.embedded
            const isError = item.status === 'ERROR'
            return (
              <div key={item.doc_id || idx} className={`p-2 rounded-lg bg-surface/40 border flex flex-col gap-1.5 ${isError ? 'border-red-500/20 bg-red-500/5' : 'border-white/5'}`}>
                <div className="flex items-center gap-2">
                  {isError ? <X size={12} strokeWidth={1.5} className="text-red-400" /> : <FileText size={12} strokeWidth={1.5} className={isDone ? "text-emerald-400" : "text-zinc-500"} />}
                  <span className={`text-[11px] font-medium truncate flex-1 ${isError ? 'text-red-300' : 'text-zinc-300'}`}>{item.name}</span>
                  {isDone && <Check size={12} strokeWidth={1.5} className="text-emerald-400" />}
                </div>
                
                {/* Pipeline Stages Visualization */}
                <div className="flex items-center gap-1 opacity-80">
                   <div className={`h-0.5 flex-1 rounded-full transition-all duration-500 ${isError ? 'bg-red-500/30' : (item.stages?.normalized ? 'bg-indigo-500' : 'bg-zinc-800')}`} />
                   <div className={`h-0.5 flex-1 rounded-full transition-all duration-500 ${isError ? 'bg-red-500/30' : (item.stages?.extracted ? 'bg-purple-500' : (item.stages?.normalized ? 'bg-zinc-700 animate-pulse' : 'bg-zinc-800'))}`} />
                   <div className={`h-0.5 flex-1 rounded-full transition-all duration-500 ${isError ? 'bg-red-500/30' : (item.stages?.embedded ? 'bg-emerald-500' : (item.stages?.extracted ? 'bg-zinc-700 animate-pulse' : 'bg-zinc-800'))}`} />
                </div>
              </div>
            )
          })}
        </div>
      )}

      {/* Dropzone - Dense */}
      <div 
        ref={dropRef} 
        className="mb-6 relative group rounded-lg border border-dashed border-zinc-800 bg-surface/20 hover:bg-accent/5 hover:border-accent/30 transition-all duration-300 overflow-hidden"
      >
        <div className="flex flex-col items-center justify-center py-4 px-2 text-center cursor-pointer">
          <div className="w-8 h-8 rounded-lg bg-surface border border-white/5 flex items-center justify-center mb-2 group-hover:scale-110 group-hover:border-accent/20 transition-all">
            <UploadCloud size={16} strokeWidth={1.5} className="text-zinc-500 group-hover:text-accent transition-colors" />
          </div>
          <p className="text-[11px] font-medium text-zinc-400 group-hover:text-zinc-200 transition-colors">Click or drop to ingest</p>
          <input type="file" multiple className="absolute inset-0 opacity-0 cursor-pointer" onChange={onUpload} />
        </div>
        {error && <div className="absolute bottom-0 left-0 right-0 bg-red-500/20 border-t border-red-500/30 p-1 text-[9px] text-red-400 text-center font-bold tracking-wide uppercase">{error}</div>}
      </div>

      <div className="flex items-center justify-between px-1 mb-2 mt-2">
        <div className="text-[10px] font-bold text-zinc-500 uppercase tracking-widest flex items-center gap-1.5">
          <FolderOpen size={10} strokeWidth={1.5} />
          <span>Recent Uploads</span>
        </div>
        <div className="flex items-center gap-2">
           {docs.length > 5 && (
             <button 
               onClick={() => setExplorerOpen(true)} 
               className="text-[9px] text-zinc-500 hover:text-white transition-colors uppercase font-bold flex items-center gap-1"
             >
               View All <MoreHorizontal size={10} />
             </button>
           )}
           <button onClick={refresh} className="text-[9px] text-sidebar-accent hover:text-white transition-colors">
             Sync
           </button>
        </div>
      </div>

      {/* Docs List - Dense */}
      <div className="flex-1 overflow-y-auto min-h-0 -mx-2 px-2 scrollbar-thin scrollbar-thumb-zinc-800/80">
        {loading && <div className="text-center py-8 text-[10px] text-zinc-600 animate-pulse">Syncing...</div>}
        {!loading && docs.length === 0 && (
          <div className="text-center py-8 opacity-40">
            <p className="text-[10px] text-zinc-600">Repository empty.</p>
          </div>
        )}
        <ul className="space-y-0.5 pb-4">
          {displayedDocs.map((doc) => {
            const name = labelFor(doc)
            const icon = iconFor(name, doc.mime)
            const badge = sourceBadge(doc)
            return (
              <li
                key={doc.doc_id}
                className="group relative flex items-center justify-between p-2 rounded-lg hover:bg-zinc-800/80 border border-transparent hover:border-zinc-700/80 hover:translate-x-1 transition-all duration-200 cursor-pointer hover:shadow-lg"
              >
                <button
                  className="flex items-center gap-3 flex-1 min-w-0 text-left"
                  onClick={() => window.open(`/ui/open/${doc.doc_id}`, '_blank', 'noopener,noreferrer')}
                >
                  <div className="opacity-70 group-hover:opacity-100 transition-opacity scale-100 group-hover:scale-110 duration-200">{icon}</div>
                  <div className="truncate flex-1">
                    <div className="flex flex-col">
                      <span className="truncate text-[13px] font-medium text-zinc-400 group-hover:text-white transition-colors">{name}</span>
                      {badge && (
                        <div className="mt-0.5">
                          <span className={`inline-flex items-center gap-1 text-[8px] font-bold px-1 py-0 rounded uppercase tracking-tighter ${badge.color}`}>
                            {badge.icon} {badge.label}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </button>
                <button
                  onClick={(e) => { e.stopPropagation(); remove(doc) }}
                  className="p-1 rounded opacity-0 group-hover:opacity-100 hover:bg-red-500/10 hover:text-red-400 text-zinc-600 transition-all scale-90 group-hover:scale-100"
                  title="Delete"
                >
                  <Trash2 size={13} strokeWidth={1.5} />
                </button>
              </li>
            )
          })}
        </ul>
      </div>
    </>
  )
}