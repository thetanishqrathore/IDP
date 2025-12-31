import React, { useEffect } from 'react'

export default function Toasts({ items = [], remove }) {
  useEffect(() => {
    if (!items?.length) return
    const timers = items.map(t => setTimeout(() => remove(t.id), t.ttl || 3500))
    return () => timers.forEach(clearTimeout)
  }, [items])
  if (!items?.length) return null
  return (
    <div className="fixed bottom-4 right-4 z-50 space-y-2">
      {items.map(t => (
        <div key={t.id} className={`toast px-3 py-2 rounded-md text-sm border ${t.kind==='error'?'bg-red-500/20 border-red-500/30':'bg-white/10 border-white/15'} backdrop-blur`}>{t.text}</div>
      ))}
    </div>
  )
}
