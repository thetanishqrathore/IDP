import React, { useEffect, useMemo, useRef, useState } from 'react'

export default function CommandPalette({ open, onClose, onAction }) {
  const [q, setQ] = useState('')
  const inputRef = useRef(null)

  const actions = useMemo(() => ([
    { id: 'chat', label: 'Ask Assistant', hint: 'Open chat', k: 'A' },
    { id: 'search', label: 'Search', hint: 'Open search', k: 'S' },
    { id: 'upload', label: 'Upload Files', hint: 'Open upload', k: 'U' },
    { id: 'scopes', label: 'Manage Scopes', hint: 'Open scopes manager', k: 'M' },
    { id: 'toggle-theme', label: 'Toggle Theme', hint: 'Dark/Light', k: 'T' },
  ]), [])

  const filtered = useMemo(() => {
    const v = q.trim().toLowerCase()
    if (!v) return actions
    return actions.filter(a => a.label.toLowerCase().includes(v) || a.id.includes(v))
  }, [q, actions])

  useEffect(() => {
    if (open) setTimeout(() => inputRef.current?.focus(), 0)
  }, [open])

  if (!open) return null
  return (
    <div className="fixed inset-0 z-[60]" onKeyDown={(e)=>{ if(e.key==='Escape') onClose?.() }}>
      <div className="absolute inset-0 bg-black/40" onClick={onClose} />
      <div className="absolute left-1/2 top-24 -translate-x-1/2 w-[92vw] max-w-xl rounded-xl border border-[color:var(--border)] glass shadow-xl">
        <div className="p-3 border-b border-[color:var(--border)]">
          <input ref={inputRef} value={q} onChange={e=>setQ(e.target.value)}
                 className="w-full bg-transparent outline-none text-sm px-1 py-1"
                 placeholder="Type a command… (e.g. Search, Upload)" />
        </div>
        <ul className="max-h-80 overflow-y-auto p-2">
          {filtered.map(a => (
            <li key={a.id}>
              <button onClick={()=>{ onAction?.(a.id); onClose?.() }}
                      className="w-full flex items-center justify-between text-left px-2 py-2 rounded-lg hover:bg-white/10">
                <div>
                  <div className="text-sm font-medium">{a.label}</div>
                  <div className="text-[12px] opacity-70">{a.hint}</div>
                </div>
                <div className="text-[12px] opacity-70">{a.k}</div>
              </button>
            </li>
          ))}
          {filtered.length===0 && (
            <li className="text-sm opacity-70 px-2 py-3">No commands</li>
          )}
        </ul>
      </div>
    </div>
  )
}

