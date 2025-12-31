import React, { useRef, useState } from 'react'
import { api } from '../api/client.js'

export default function SearchPane() {
  const [q, setQ] = useState('')
  const [k, setK] = useState(12)
  const [loading, setLoading] = useState(false)
  const [results, setResults] = useState([])

  const inputRef = useRef(null)
  // Search across all documents for now.
  function filters() { return {} }

  async function runSearch() {
    if (!q.trim()) return
    setLoading(true)
    try {
      const res = await api.search(q, { k, filters: filters() })
      setResults(res.results || res.hits || [])
    } catch (_) {
      setResults([])
    } finally {
      setLoading(false)
    }
  }

  function onKeyDown(e){ if (e.key==='Enter') runSearch() }

  function escapeHtml(str=''){
    return str.replace(/[&<>"']/g, (c)=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;', "'":'&#39;' }[c]))
  }
  function highlight(text=''){ const t=q.trim(); if(!t) return escapeHtml(text); try{ const re=new RegExp(`(${t.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')})`,'ig'); return escapeHtml(text).replace(re,'<mark class="bg-yellow-300/30 text-inherit rounded px-0.5">$1</mark>') }catch{ return escapeHtml(text) } }

  // optional: show renamed labels from localStorage
  function labelForResult(r){
    try { const names = JSON.parse(localStorage.getItem('docNames')||'{}'); const fallback=(r.uri||'').split('/').pop()||r.doc_id; return names[r.doc_id]||fallback } catch { return (r.uri||'').split('/').pop()||r.doc_id }
  }

  return (
    <main className="flex-1 flex flex-col min-h-0">
      <div className="border-b border-[color:var(--border)] p-3 flex items-center justify-between bg-[color:var(--surface)] backdrop-blur">
        <h2 className="text-base font-semibold">Search</h2>
        <div className="flex items-center gap-2 text-sm">
          <label className="opacity-70">Top K</label>
          <input type="number" min={1} max={50} value={k} onChange={e=>setK(Number(e.target.value)||12)} className="w-16 bg-[color:var(--surface)] border border-[color:var(--border)] px-2 py-1 rounded" />
        </div>
      </div>
      <div className="p-3 flex flex-col sm:flex-row gap-2 max-w-4xl mx-auto w-full">
        <input ref={inputRef} value={q} onChange={e=>setQ(e.target.value)} onKeyDown={onKeyDown} placeholder="Find text across your knowledge base…" className="flex-1 bg-[color:var(--surface)] px-4 py-2 rounded-full border border-[color:var(--border)] focus:outline-none focus:ring-2 focus:ring-blue-400/40" />
        <button onClick={runSearch} className="sm:w-auto w-full px-5 py-2 rounded-full bg-gradient-to-br from-blue-500 to-cyan-400 text-white shadow-lg transition-transform hover:-translate-y-0.5 active:scale-95">Search</button>
      </div>
      <div className="flex-1 overflow-y-auto p-3 space-y-2 max-w-4xl mx-auto w-full">
        {loading && <div className="opacity-70">Searching…</div>}
        {!loading && results.length===0 && <div className="opacity-70">No results</div>}
        {results.map((r, i) => (
          <div key={i} className="bg-[color:var(--surface)] hover:bg-[color:var(--surface)] transition p-3 rounded-lg border border-[color:var(--border)]">
            <div className="flex items-center justify-between gap-3">
              <div className="text-sm font-semibold truncate">{labelForResult(r)}</div>
              <div className="flex items-center gap-3">
                {typeof r.score==='number' && (
                  <div className="text-xs opacity-60">{Math.round(r.score*100)}%</div>
                )}
                <button onClick={async()=>{ try{ const link=await api.linkForDoc(r.doc_id); if(link?.url) window.open(link.url, '_blank', 'noopener,noreferrer') }catch{} }} className="text-xs px-2 py-1 rounded bg-[color:var(--surface)] border border-[color:var(--border)] hover:bg-[color:var(--surface)]">Open</button>
              </div>
            </div>
            {r.text && <div className="text-sm mt-1 opacity-90" dangerouslySetInnerHTML={{ __html: highlight(r.text) }} />}
          </div>
        ))}
      </div>
    </main>
  )
}
