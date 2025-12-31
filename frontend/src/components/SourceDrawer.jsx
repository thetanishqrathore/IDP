import React, { useEffect, useMemo, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { X, Copy, ExternalLink, ChevronLeft, ChevronRight, Hash, FileText } from 'lucide-react'

export default function SourceDrawer({ open, onClose, citation }) {
  const [useBlock, setUseBlock] = useState(true)
  const [page, setPage] = useState(1)
  const [variant, setVariant] = useState('original') // 'original' | 'canonical'
  const hasBlock = (citation?.block_ids && citation.block_ids.length > 0)
  const iframeRef = useRef(null)

  useEffect(() => {
    setUseBlock(true)
    const p = Number(citation?.page_start || 1)
    setPage(Number.isFinite(p) && p > 0 ? p : 1)
    setVariant('original')
  }, [citation])

  const url = useMemo(() => {
    if (!citation?.doc_id) return ''
    const isPdf = String(citation?.mime || '').toLowerCase().includes('pdf')
    const base = `/ui/open/${citation.doc_id}?variant=${variant}`
    if (variant === 'canonical') {
      if (useBlock && hasBlock) {
        return `${base}#a=${encodeURIComponent(String(citation.block_ids[0]))}`
      }
      return `${base}#p-${page}`
    }
    if (isPdf && page > 0) return `${base}#page=${page}`
    return base
  }, [citation, useBlock, page, hasBlock, variant])

  function applyIframeDarkModeStyles() {
    try {
      const iframe = iframeRef.current
      if (!iframe) return
      const doc = iframe.contentDocument || iframe.contentWindow?.document
      if (!doc) return
      const el = doc.documentElement
      if (!el) return
      const style = doc.createElement('style')
      style.setAttribute('data-injected', 'true')
      style.textContent = `
        html, body { background: #050505 !important; color: #EDEDED !important; font-family: 'Inter', system-ui, sans-serif !important; line-height: 1.7 !important; }
        body { max-width: 72ch !important; margin: 0 auto !important; padding: 3rem 2rem !important; }
        body * { color: inherit !important; position: static !important; width: auto !important; height: auto !important; }
        a { color: #6366f1 !important; text-decoration: none; border-bottom: 1px dashed rgba(99, 102, 241, 0.4); }
        pre, code { color: #e4e4e7 !important; background: rgba(255,255,255,0.04) !important; padding: 0.2em 0.4em; border-radius: 6px; }
        pre { background: #0A0A0A !important; border: 1px solid rgba(255,255,255,0.05) !important; padding: 1.5rem !important; }
        table { border-collapse: collapse; width: 100%; margin: 2em 0; font-size: 0.9em; }
        th { background: rgba(255,255,255,0.02) !important; font-weight: 600 !important; }
        th, td { border: 1px solid #1F1F22 !important; padding: 0.75em !important; text-align: left !important; }
        img { max-width: 100% !important; height: auto !important; border-radius: 8px; border: 1px solid #1F1F22; }
        [id^="p-"] { scroll-margin-top: 2rem; border-left: 2px solid #6366f1; padding-left: 1rem; margin-left: -1rem; }
      `
      if (!doc.head.querySelector('style[data-injected="true"]')) {
        doc.head.appendChild(style)
      }
    } catch (_) {}
  }

  if (!citation) return null

  const filename = (citation.uri || '').split('/').pop() || citation.doc_id
  const openExtracted = async () => {
    const base = `/ui/open/${citation.doc_id}?variant=canonical`
    const fallback = (useBlock && hasBlock)
      ? `${base}#a=${encodeURIComponent(String(citation.block_ids[0]))}`
      : `${base}#p-${page}`
    window.open(fallback, '_blank', 'noopener,noreferrer')
  }
  const copyCitation = async () => {
    const text = `${filename} — p. ${page}`
    try { await navigator.clipboard.writeText(text) } catch {}
  }

  return (
    <AnimatePresence>
      {open && (
        <div className="fixed inset-0 z-50 overflow-hidden flex justify-end">
          <motion.div
            className="absolute inset-0 bg-black/60 backdrop-blur-sm z-0"
            onClick={onClose}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          />
          <motion.aside
            className="relative w-full sm:w-[580px] lg:w-[720px] h-full bg-app-bg border-l border-border shadow-glass flex flex-col z-10"
            initial={{ x: '100%' }}
            animate={{ x: 0 }}
            exit={{ x: '100%' }}
            transition={{ type: 'spring', stiffness: 300, damping: 30 }}
          >
            {/* Header */}
            <div className="h-16 flex items-center justify-between px-5 border-b border-border bg-card-bg/50 backdrop-blur-md">
              <div className="flex items-center gap-3 truncate">
                <div className="p-2 rounded-lg bg-accent/10 text-accent">
                  <FileText size={18} strokeWidth={1.5} />
                </div>
                <div className="truncate">
                  <div className="text-[13px] font-bold text-text-primary truncate tracking-tight" title={filename}>{filename}</div>
                  <div className="text-[10px] text-text-muted font-bold uppercase tracking-widest flex items-center gap-1.5">
                    <span>Page {page}</span>
                    {hasBlock && useBlock && <span className="flex items-center gap-1 text-accent"><Hash size={10} /> Anchored</span>}
                  </div>
                </div>
              </div>
              
              <div className="flex items-center gap-2">
                <button onClick={copyCitation} className="p-2 rounded-lg text-text-muted hover:text-text-primary hover:bg-white/5 transition-all" title="Copy Reference">
                  <Copy size={18} strokeWidth={1.5} />
                </button>
                <button onClick={openExtracted} className="p-2 rounded-lg text-text-muted hover:text-text-primary hover:bg-white/5 transition-all" title="Open Externally">
                  <ExternalLink size={18} strokeWidth={1.5} />
                </button>
                <div className="w-[1px] h-4 bg-border mx-1" />
                <button onClick={onClose} className="p-2 rounded-lg text-text-muted hover:text-error hover:bg-error/10 transition-all">
                  <X size={20} strokeWidth={1.5} />
                </button>
              </div>
            </div>

            {/* Toolbar */}
            <div className="flex items-center gap-4 px-5 py-3 border-b border-border bg-surface/20 text-[12px]">
              <div className="flex bg-card-bg p-1 rounded-lg border border-border">
                <button 
                  onClick={()=>setVariant('original')} 
                  className={`px-3 py-1.5 rounded-md font-bold transition-all ${variant==='original'?'bg-accent text-white shadow-glow':'text-text-muted hover:text-text-secondary'}`}
                >
                  Original
                </button>
                <button 
                  onClick={()=>setVariant('canonical')} 
                  className={`px-3 py-1.5 rounded-md font-bold transition-all ${variant==='canonical'?'bg-accent text-white shadow-glow':'text-text-muted hover:text-text-secondary'}`}
                >
                  Reader
                </button>
              </div>

              <label className={`flex items-center gap-2 cursor-pointer select-none font-bold text-text-muted transition-opacity ${variant!=='canonical' ? 'opacity-30 pointer-events-none' : ''}`}>
                <input 
                  type="checkbox" 
                  className="w-3.5 h-3.5 rounded border-border bg-surface text-accent focus:ring-accent/30"
                  checked={useBlock && hasBlock && variant==='canonical'} 
                  onChange={() => setUseBlock(v=>!v)} 
                />
                <span>Anchor to source</span>
              </label>

              <div className="ml-auto flex items-center gap-3 bg-card-bg px-2 py-1 rounded-lg border border-border">
                <button onClick={()=>setPage(p => Math.max(1, p-1))} className="p-1 rounded-md text-text-muted hover:text-text-primary hover:bg-white/5 transition-all">
                  <ChevronLeft size={16} strokeWidth={1.5} />
                </button>
                <div className="min-w-[40px] text-center font-mono font-bold text-text-secondary">{page}</div>
                <button onClick={()=>setPage(p => p+1)} className="p-1 rounded-md text-text-muted hover:text-text-primary hover:bg-white/5 transition-all">
                  <ChevronRight size={16} strokeWidth={1.5} />
                </button>
              </div>
            </div>

            {/* Iframe Container */}
            <div className="flex-1 bg-[#050505] relative">
              <iframe
                key={url}
                ref={iframeRef}
                title="source"
                src={url}
                className="w-full h-full border-none"
                referrerPolicy="no-referrer"
                onLoad={applyIframeDarkModeStyles}
              />
            </div>
          </motion.aside>
        </div>
      )}
    </AnimatePresence>
  )
}