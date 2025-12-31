import React, { useEffect, useRef, useState } from 'react'

export default function KebabMenu({ items = [], onClose }) {
  const ref = useRef(null)
  useEffect(() => {
    function onDoc(e){ if (!ref.current?.contains(e.target)) onClose?.() }
    document.addEventListener('mousedown', onDoc)
    return () => document.removeEventListener('mousedown', onDoc)
  }, [onClose])
  return (
    <div ref={ref} className="absolute right-0 top-full mt-1 w-44 rounded-lg border border-[color:var(--border)] glass shadow-xl z-20">
      <ul className="p-1 text-sm">
        {items.map((it, i) => (
          <li key={i}>
            <button
              disabled={it.disabled}
              onClick={it.onClick}
              className={`w-full flex items-center gap-2 px-2 py-2 rounded hover:bg-white/10 disabled:opacity-50 ${it.danger?'hover:text-red-400':''}`}
            >
              <span className="shrink-0">{it.icon}</span>
              <span>{it.label}</span>
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}
