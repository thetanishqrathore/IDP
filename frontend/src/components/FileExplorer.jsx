import React, { useState, useEffect, useMemo } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Search, FileText, FileImage, FileCode, File, Trash2, X, Download, Eye, Grid, List } from 'lucide-react'
import { api } from '../api/client.js'

export default function FileExplorer({ open, onClose, onDelete }) {
  const [docs, setDocs] = useState([])
  const [loading, setLoading] = useState(true)
  const [query, setQuery] = useState('')
  const [view, setView] = useState('list') // 'list' | 'grid'

  useEffect(() => {
    if (open) load()
  }, [open])

  async function load() {
    setLoading(true)
    try {
      const res = await api.listDocs(1000) // Fetch more
      setDocs(res.docs || [])
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }

  const filtered = useMemo(() => {
    if (!query) return docs
    const q = query.toLowerCase()
    return docs.filter(d => {
      const name = (d.uri || '').split('/').pop() || d.doc_id
      return name.toLowerCase().includes(q)
    })
  }, [docs, query])

  const iconFor = (mime) => {
    const m = (mime || '').toLowerCase()
    if (m.includes('pdf')) return <FileText className="text-rose-400" size={20} />
    if (m.includes('image')) return <FileImage className="text-purple-400" size={20} />
    if (m.includes('json') || m.includes('xml')) return <FileCode className="text-emerald-400" size={20} />
    return <File className="text-zinc-400" size={20} />
  }

  const sizeStr = (bytes) => {
    if (!bytes) return '0 B'
    const k = 1024
    if (bytes < k) return bytes + ' B'
    if (bytes < k*k) return (bytes/k).toFixed(1) + ' KB'
    return (bytes/(k*k)).toFixed(1) + ' MB'
  }

  return (
    <AnimatePresence>
      {open && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
          <motion.div 
            initial={{ opacity: 0 }} 
            animate={{ opacity: 1 }} 
            exit={{ opacity: 0 }}
            className="absolute inset-0 bg-black/80 backdrop-blur-sm"
            onClick={onClose}
          />
          <motion.div
            initial={{ scale: 0.95, opacity: 0, y: 10 }}
            animate={{ scale: 1, opacity: 1, y: 0 }}
            exit={{ scale: 0.95, opacity: 0, y: 10 }}
            className="relative w-full max-w-4xl h-[80vh] bg-sidebar-bg border border-border rounded-xl shadow-2xl flex flex-col overflow-hidden"
          >
            {/* Header */}
            <div className="flex items-center justify-between p-4 border-b border-border bg-card-bg/50">
               <div className="flex items-center gap-3">
                 <h2 className="text-lg font-bold text-text-primary">File Explorer</h2>
                 <span className="text-xs px-2 py-0.5 rounded-full bg-surface border border-white/5 text-zinc-400">{docs.length} items</span>
               </div>
               <button onClick={onClose} className="p-2 hover:bg-white/5 rounded-lg text-zinc-400 hover:text-white">
                 <X size={20} />
               </button>
            </div>

            {/* Toolbar */}
            <div className="p-4 border-b border-border/50 bg-surface/10 flex gap-4">
              <div className="relative flex-1">
                <Search size={16} className="absolute left-3 top-2.5 text-zinc-500" />
                <input 
                  value={query}
                  onChange={e => setQuery(e.target.value)}
                  placeholder="Search files..." 
                  className="w-full bg-app-bg border border-border rounded-lg pl-9 pr-4 py-2 text-sm text-text-primary focus:border-accent/50 outline-none"
                />
              </div>
              <div className="flex bg-app-bg rounded-lg border border-border p-1">
                <button onClick={() => setView('list')} className={`p-1.5 rounded ${view === 'list' ? 'bg-surface text-white' : 'text-zinc-500 hover:text-zinc-300'}`}>
                  <List size={16} />
                </button>
                <button onClick={() => setView('grid')} className={`p-1.5 rounded ${view === 'grid' ? 'bg-surface text-white' : 'text-zinc-500 hover:text-zinc-300'}`}>
                  <Grid size={16} />
                </button>
              </div>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-y-auto p-4 bg-app-bg/50 scrollbar-thin">
              {loading ? (
                <div className="flex flex-col items-center justify-center h-full text-zinc-500 gap-2">
                   <div className="w-8 h-8 border-2 border-accent/30 border-t-accent rounded-full animate-spin" />
                   <span className="text-xs">Loading repository...</span>
                </div>
              ) : filtered.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-full text-zinc-600 gap-2">
                   <Search size={32} className="opacity-20" />
                   <p>No matching files found.</p>
                </div>
              ) : view === 'list' ? (
                <table className="w-full text-sm">
                  <thead className="text-xs font-bold text-zinc-500 uppercase tracking-wider text-left sticky top-0 bg-app-bg z-10">
                    <tr>
                      <th className="pb-3 pl-2">Name</th>
                      <th className="pb-3">Type</th>
                      <th className="pb-3">Size</th>
                      <th className="pb-3">Date</th>
                      <th className="pb-3 text-right pr-2">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/5">
                    {filtered.map(doc => {
                       const name = (doc.uri || '').split('/').pop() || doc.doc_id
                       return (
                         <tr key={doc.doc_id} className="group hover:bg-white/5 transition-colors">
                           <td className="py-2.5 pl-2">
                             <div className="flex items-center gap-3">
                               {iconFor(doc.mime)}
                               <span className="font-medium text-zinc-300 truncate max-w-[200px] md:max-w-xs" title={name}>{name}</span>
                             </div>
                           </td>
                           <td className="py-2.5 text-zinc-500 text-xs truncate max-w-[100px]">{doc.mime}</td>
                           <td className="py-2.5 text-zinc-500 text-xs font-mono">{sizeStr(doc.size_bytes)}</td>
                           <td className="py-2.5 text-zinc-500 text-xs">{new Date(doc.collected_at).toLocaleDateString()}</td>
                           <td className="py-2.5 text-right pr-2">
                             <div className="flex items-center justify-end gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                               <button 
                                 onClick={() => window.open(`/ui/open/${doc.doc_id}`, '_blank')}
                                 className="p-1.5 rounded bg-surface border border-white/5 text-zinc-400 hover:text-white hover:border-accent/50" 
                                 title="View"
                               >
                                 <Eye size={14} />
                               </button>
                               <button 
                                 onClick={() => onDelete?.(doc)}
                                 className="p-1.5 rounded bg-surface border border-white/5 text-zinc-400 hover:text-red-400 hover:border-red-500/30"
                                 title="Delete"
                               >
                                 <Trash2 size={14} />
                               </button>
                             </div>
                           </td>
                         </tr>
                       )
                    })}
                  </tbody>
                </table>
              ) : (
                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4">
                  {filtered.map(doc => {
                     const name = (doc.uri || '').split('/').pop() || doc.doc_id
                     return (
                       <div key={doc.doc_id} className="group p-4 rounded-xl border border-white/5 bg-surface/20 hover:bg-surface/50 hover:border-accent/30 hover:shadow-lg transition-all flex flex-col items-center text-center relative cursor-pointer"
                         onClick={() => window.open(`/ui/open/${doc.doc_id}`, '_blank')}
                       >
                         <div className="w-12 h-12 rounded-full bg-app-bg border border-white/5 flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                           {iconFor(doc.mime)}
                         </div>
                         <h3 className="text-xs font-medium text-zinc-300 line-clamp-2 mb-1 w-full break-words" title={name}>{name}</h3>
                         <span className="text-[10px] text-zinc-500">{sizeStr(doc.size_bytes)}</span>
                         
                         <button 
                           onClick={(e) => { e.stopPropagation(); onDelete?.(doc) }}
                           className="absolute top-2 right-2 p-1.5 rounded-full bg-black/40 text-white opacity-0 group-hover:opacity-100 hover:bg-red-500 hover:text-white transition-all"
                         >
                           <Trash2 size={12} />
                         </button>
                       </div>
                     )
                  })}
                </div>
              )}
            </div>
            
            <div className="p-3 bg-surface/20 border-t border-border/50 text-[10px] text-zinc-500 text-center uppercase tracking-widest font-bold">
              Secure Document Storage • IDP v2
            </div>
          </motion.div>
        </div>
      )}
    </AnimatePresence>
  )
}
