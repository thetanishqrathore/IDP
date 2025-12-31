import React, { useEffect, useMemo, useRef } from 'react'
import { marked } from 'marked'
import DOMPurify from 'dompurify'

marked.setOptions({
  breaks: true,
  gfm: true,
})

export default function Markdown({ text = '', citations = [], onCitationClick, showCursor = false }) {
  const containerRef = useRef(null)
  const html = useMemo(() => {
    try {
      const raw = marked.parse(text || '')
      return DOMPurify.sanitize(raw)
    } catch {
      return DOMPurify.sanitize(text)
    }
  }, [text])

  useEffect(() => {
    const root = containerRef.current
    if (!root) return

    // Build footnote map and label map
    const names = (() => {
      try { return JSON.parse(localStorage.getItem('docNames') || '{}') } catch { return {} }
    })()
    const byNum = new Map()
    for (const c of citations || []) {
      const n = Number(c?.n)
      if (!Number.isFinite(n)) continue
      const file = (c?.uri || '').split('/').pop() || c?.doc_id || `Doc ${n}`
      const label = names[c?.doc_id] || file
      byNum.set(n, { citation: c, label })
    }

    // Replace [^n] in text nodes (skip inside code/pre)
    const SKIP = new Set(['CODE', 'PRE', 'A'])
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT)
    const targets = []
    while (walker.nextNode()) {
      const node = walker.currentNode
      let el = node.parentElement
      let skip = false
      while (el) { if (SKIP.has(el.tagName)) { skip = true; break } el = el.parentElement }
      if (skip) continue
      // Match [^1] or [1] as citations
      if (/\[\^?(\d+)\]/.test(node.nodeValue || '')) targets.push(node)
    }
    for (const tn of targets) {
      const text = tn.nodeValue || ''
      const fr = document.createDocumentFragment()
      let lastIndex = 0
      const re = /\[\^?(\d+)\]/g
      let m
      while ((m = re.exec(text)) !== null) {
        const idx = m.index
        if (idx > lastIndex) fr.appendChild(document.createTextNode(text.slice(lastIndex, idx)))
        const n = Number(m[1])
        const info = byNum.get(n)
        if (info) {
          const a = document.createElement('a')
          a.href = '#'
          a.className = 'citation-pill'
          a.setAttribute('data-n', String(n))
          a.textContent = `${n}` 
          a.title = `Source: ${info.label}`
          fr.appendChild(a)
        } else {
          fr.appendChild(document.createTextNode(m[0]))
        }
        lastIndex = re.lastIndex
      }
      if (lastIndex < text.length) fr.appendChild(document.createTextNode(text.slice(lastIndex)))
      tn.parentNode.replaceChild(fr, tn)
    }

    const onClick = (e) => {
      const t = e.target
      if (t && t.closest && t.closest('.citation-pill')) {
        e.preventDefault()
        const a = t.closest('.citation-pill')
        const n = Number(a.getAttribute('data-n'))
        const info = byNum.get(n)
        if (info && typeof onCitationClick === 'function') {
          onCitationClick(info.citation)
        }
      }
    }
    root.addEventListener('click', onClick)
    return () => root.removeEventListener('click', onClick)
  }, [html, citations, onCitationClick])

  // Streaming caret injection (at end of last text node when possible)
  useEffect(() => {
    const root = containerRef.current
    if (!root) return
    // cleanup previous carets
    root.querySelectorAll('.stream-caret').forEach((el) => el.remove())
    if (!showCursor) return
    const SKIP = new Set(['CODE', 'PRE'])
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT)
    let lastNode = null
    while (walker.nextNode()) {
      const node = walker.currentNode
      let el = node.parentElement
      let skip = false
      while (el) { if (SKIP.has(el.tagName)) { skip = true; break } el = el.parentElement }
      if (!skip && (node.nodeValue || '').trim().length > 0) lastNode = node
    }
    const caret = document.createElement('span')
    caret.className = 'stream-caret'
    caret.setAttribute('aria-hidden', 'true')
    // Blinking underscore style
    caret.style.display = 'inline-block'
    caret.style.width = '0.6em'
    caret.style.height = '3px'
    caret.style.backgroundColor = 'currentColor'
    caret.style.marginLeft = '1px'
    caret.style.verticalAlign = '0px' // Align with baseline
    caret.style.opacity = '0.8'
    caret.style.animation = 'pulse 1s cubic-bezier(0.4, 0, 0.6, 1) infinite'
    
    if (lastNode && lastNode.parentNode) {
      lastNode.parentNode.insertBefore(caret, lastNode.nextSibling)
    } else {
      root.appendChild(caret)
    }
  }, [html, showCursor])

  return (
    <div
      ref={containerRef}
      className="md-content text-[15px] leading-relaxed"
      dangerouslySetInnerHTML={{ __html: html }}
    />
  )
}
