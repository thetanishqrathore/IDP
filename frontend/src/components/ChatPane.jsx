import React, { useContext, useEffect, useRef, useState } from 'react'
import { AppContext } from '../App.jsx'
import { api } from '../api/client.js'
import MessageBubble from './MessageBubble.jsx'
import SourceDrawer from './SourceDrawer.jsx'
import Composer from './Composer.jsx'
import WelcomeScreen from './WelcomeScreen.jsx'
import { Sparkles, MessageSquarePlus, ChevronLeft } from 'lucide-react'

function CitationsUI({ citations = [], onCitationClick }) {
  if (!citations?.length) return null
  const names = (() => { try { return JSON.parse(localStorage.getItem('docNames') || '{}') } catch { return {} } })()
  const labelFor = (c) => {
    const did = c?.doc_id
    if (did && names[did]) return names[did]
    const file = (c?.uri || '').split('/').pop() || did || `Doc ${c?.n ?? ''}`
    return file
  }
  
  // SHOW ONLY THE MOST RELEVANT SOURCE (Top 1)
  const topCitation = citations[0]

  return (
    <div className="mt-4 flex flex-wrap gap-2 pl-1">
        <button
          key={`chip-0`}
          onClick={() => onCitationClick(topCitation)}
          className="flex items-center gap-1.5 text-[11px] font-medium px-2.5 py-1 rounded-md 
                     bg-white/5 border border-white/10 text-zinc-400 hover:text-white hover:border-accent/50 hover:bg-accent/10 transition-all"
        >
          <span className="opacity-60">SOURCE</span>
          <span>{labelFor(topCitation)}</span>
        </button>
    </div>
  )
}

export default function ChatPane() {
  const { prefillText, setPrefillText, setView } = useContext(AppContext)

  const [messages, setMessages] = useState([])
  const [streaming, setStreaming] = useState(false)
  const [k, setK] = useState(8)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [activeCitation, setActiveCitation] = useState(null)

  const chatRef = useRef(null)
  const composerRef = useRef(null)

  const inWelcome = messages.length === 0

  // --- scrolling ---
  useEffect(() => {
    if (!chatRef.current) return
    chatRef.current.scrollTo({ top: chatRef.current.scrollHeight, behavior: 'smooth' })
  }, [messages])

  // --- streaming ask ---
  function filters() { return {} }

  async function ask(text) {
    const q = (text || '').trim()
    if (!q || streaming) return

    setMessages((m) => [
      ...m,
      { role: 'user', text: q },
      { role: 'assistant', text: '', citations: [], status: 'streaming' },
    ])

    setStreaming(true)
    const stream = api.streamAnswer(q, { k, filters: filters() })
    const CLIENT_STREAM_DELAY_MS = 40 
    let answer = ''

    try {
      for await (const evt of await stream.start()) {
        if (evt.type === 'meta') {
          setMessages((m) => {
            const copy = [...m]
            const idx = copy.findIndex(
              (x) => x.role === 'assistant' && (x.status === 'skeleton' || x.status === 'streaming'),
            )
            if (idx !== -1) {
              copy[idx] = { ...copy[idx], citations: evt.citations || [], status: 'streaming' }
            }
            return copy
          })
        } else if (evt.type === 'chunk') {
          answer += evt.text || ''
          setMessages((m) => {
            const copy = [...m]
            const last = copy[copy.length - 1]
            if (last && last.role === 'assistant') last.text = answer
            return copy
          })
          try { await new Promise((r) => setTimeout(r, CLIENT_STREAM_DELAY_MS)) } catch {}
        } else if (evt.type === 'done') {
          setMessages((m) => {
            const copy = [...m]
            const last = copy[copy.length - 1]
            if (last && last.role === 'assistant') last.status = 'done'
            return copy
          })
        }
      }
    } catch (e) {
      setMessages((m) => [...m, { role: 'assistant', text: 'I encountered an error processing your request.' }])
    } finally {
      setStreaming(false)
    }
  }

  // Handle prefill from sidebar/command palette
  useEffect(() => {
    if (prefillText && typeof prefillText === 'string') {
      if (composerRef.current) {
        composerRef.current.setInput(prefillText)
        composerRef.current.focus()
      }
      setPrefillText('') 
    }
  }, [prefillText])

  const openCitation = (c) => {
    setActiveCitation(c)
    setDrawerOpen(true)
  }

  const handleEdit = (text) => {
    if (composerRef.current) {
        composerRef.current.setInput(text)
        composerRef.current.focus()
    }
  }

  return (
    <main className="relative flex-1 min-h-0 flex flex-col bg-app-bg text-text-primary overflow-hidden">
      
      {/* Top Bar / Header */}
      <header className="h-14 shrink-0 border-b border-border bg-card-bg/50 backdrop-blur-sm flex items-center justify-between px-6 z-10">
        <div className="flex items-center">
          <button 
            onClick={() => setView('dashboard')} 
            className="flex items-center gap-2 px-3 py-1.5 -ml-2 text-zinc-400 hover:text-white hover:bg-white/5 rounded-lg transition-colors"
            title="Back to Dashboard"
          >
            <ChevronLeft size={16} />
            <span className="text-sm font-medium">Dashboard</span>
          </button>
        </div>

        {/* Centered Title */}
        <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 flex items-center gap-2">
          <Sparkles size={16} className="text-accent" />
          <span className="text-sm font-bold tracking-tight text-text-primary uppercase">Second Brain</span>
        </div>

        <div className="flex items-center gap-3">
            {!inWelcome && (
            <button
              onClick={() => setMessages([])}
              className="p-2 text-zinc-400 hover:text-white hover:bg-white/5 rounded-lg transition-colors"
              title="Clear Chat"
            >
              <MessageSquarePlus size={18} />
            </button>
            )}
        </div>
      </header>

      {/* Chat Area */}
      <div className="flex-1 min-h-0 relative">
        <div 
          ref={chatRef} 
          className={`h-full overflow-y-auto overflow-x-hidden scroll-smooth ${inWelcome ? 'flex items-center justify-center' : 'px-4 py-6'}`}
        >
          {inWelcome ? (
            <WelcomeScreen onSend={ask} />
          ) : (
            /* Message List */
            <div className="max-w-3xl mx-auto space-y-10 pb-32">
              {messages.map((m, i) => (
                <div key={i} className="group relative">
                  {m.role === 'assistant' && (
                     <div className="absolute -left-10 top-0 hidden md:flex items-center justify-center w-8 h-8">
                        <Sparkles size={28} className="text-[#9B72CB] animate-pulse-slow" />
                     </div>
                  )}
                  <MessageBubble
                    role={m.role}
                    text={m.text}
                    status={m.status}
                    citations={m.citations}
                    onCitationClick={openCitation}
                    onEdit={() => handleEdit(m.text)}
                    onRegenerate={() => {
                        if (m.role === 'assistant') {
                          const prevUser = messages.slice(0, i).reverse().find(x => x.role === 'user')
                          if (prevUser) ask(prevUser.text)
                        }
                    }}
                  />
                  {m.role === 'assistant' && <CitationsUI citations={m.citations} onCitationClick={openCitation} />}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Floating Composer Layer (only if not welcome) */}
        {!inWelcome && (
          <div className="absolute bottom-0 left-0 right-0 p-6 bg-gradient-to-t from-app-bg via-app-bg to-transparent">
            <div className="max-w-3xl mx-auto">
               <Composer ref={composerRef} onSend={ask} streaming={streaming} placeholder="Ask a follow up..." />
            </div>
          </div>
        )}
      </div>

      <SourceDrawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        citation={activeCitation}
      />
    </main>
  )
}
