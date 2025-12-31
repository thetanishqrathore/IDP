import React, { useCallback, useEffect, useMemo, useState } from 'react'
import Sidebar from './components/Sidebar.jsx'
import Toasts from './components/Toasts.jsx'
import ChatPane from './components/ChatPane.jsx'
import Dashboard from './components/Dashboard.jsx'
import CommandPalette from './components/CommandPalette.jsx'
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels'
import { ChevronsLeftRight, ChevronRight } from 'lucide-react'
import { api } from './api/client.js'

export const AppContext = React.createContext(null)

export default function App() {
  const [view, setView] = useState('dashboard') // 'dashboard' | 'chat'
  const [selectedDocs, setSelectedDocs] = useState([]) // [{doc_id, uri, ...}]
  const [backendHealthy, setBackendHealthy] = useState(false)
  const [savedScopes, setSavedScopes] = useState(() => {
    try { return JSON.parse(localStorage.getItem('savedScopes') || '[]') } catch { return [] }
  }) // [{id,name,doc_ids,createdAt}]
  const [activeScopeId, setActiveScopeId] = useState(() => {
    try { return localStorage.getItem('activeScopeId') || null } catch { return null }
  }) // null | 'selected' | scope.id
  const [toasts, setToasts] = useState([])
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false)
  const [isDesktop, setIsDesktop] = useState(() => {
    if (typeof window === 'undefined') return true
    return window.matchMedia('(min-width: 1024px)').matches
  })
  const [cmdOpen, setCmdOpen] = useState(false)
  const [theme, setTheme] = useState(() => (typeof localStorage!== 'undefined' ? (localStorage.getItem('theme') || 'dark') : 'dark'))
  const [prefillText, setPrefillText] = useState('')
  const [resetCounter, setResetCounter] = useState(0)

  const pushToast = useCallback((text, kind = 'info', ttl = 3500) => {
    const id = Math.random().toString(36).slice(2)
    setToasts((ts) => [...ts, { id, text, kind, ttl }])
    return id
  }, [])

  const removeToast = useCallback((id) => {
    setToasts((ts) => ts.filter((t) => t.id !== id))
  }, [])

  const clearAll = useCallback(async () => {
    try {
      await api.clearAll()
      setSelectedDocs([])
      setSavedScopes([])
      setActiveScopeId(null)
      setPrefillText('')
      try {
        localStorage.removeItem('savedScopes')
        localStorage.removeItem('activeScopeId')
        localStorage.removeItem('docNames')
      } catch (_) {}
      setResetCounter((c) => c + 1)
      pushToast('Workspace cleared', 'info')
      return true
    } catch (e) {
      pushToast('Failed to clear workspace', 'error')
      throw e
    }
  }, [pushToast])

  const ctx = useMemo(() => ({
    view, setView,
    selectedDocs, setSelectedDocs,
    backendHealthy, setBackendHealthy,
    savedScopes, setSavedScopes,
    activeScopeId, setActiveScopeId,
    prefillText, setPrefillText,
    isDesktop,
    collapseSidebar: () => { if (isDesktop) setSidebarCollapsed(true); setMobileSidebarOpen(false) },
    pushToast,
    removeToast,
    clearAll,
    resetCounter,
  }), [view, selectedDocs, backendHealthy, savedScopes, activeScopeId, prefillText, isDesktop, pushToast, removeToast, clearAll, resetCounter])

  // persist scopes
  useEffect(() => {
    try { localStorage.setItem('savedScopes', JSON.stringify(savedScopes || [])) } catch {}
  }, [savedScopes])

  useEffect(() => {
    try { if (activeScopeId) localStorage.setItem('activeScopeId', activeScopeId); else localStorage.removeItem('activeScopeId') } catch {}
  }, [activeScopeId])

  useEffect(() => {
    const onKey = (e) => {
      const isMac = navigator.platform.toUpperCase().indexOf('MAC')>=0
      if ((isMac && e.metaKey && e.key.toLowerCase()==='k') || (!isMac && e.ctrlKey && e.key.toLowerCase()==='k')) {
        e.preventDefault(); setCmdOpen(true)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  useEffect(() => {
    document.body.classList.toggle('light', theme==='light')
    try { localStorage.setItem('theme', theme) } catch {}
  }, [theme])

  useEffect(() => {
    const mq = window.matchMedia('(min-width: 1024px)')
    const handler = (e) => setIsDesktop(e.matches)
    mq.addEventListener?.('change', handler)
    setIsDesktop(mq.matches)
    return () => mq.removeEventListener?.('change', handler)
  }, [])

  const toggleSidebar = () => {
    if (isDesktop) setSidebarCollapsed(v=>!v)
    else setMobileSidebarOpen(v=>!v)
  }

  return (
    <AppContext.Provider value={ctx}>
      <div className="h-screen flex flex-col overflow-hidden">
        <Toasts items={toasts} remove={removeToast} />
        <div className="flex-1 min-h-0">
          <PanelGroup direction="horizontal" className="h-full min-h-0">
            {isDesktop && !sidebarCollapsed && view === 'dashboard' && (
              <Panel minSize={18} defaultSize={23} collapsible className="min-w-0 min-h-0">
                <Sidebar
                  collapsed={sidebarCollapsed}
                  onCollapse={() => setSidebarCollapsed(true)}
                />
              </Panel>
            )}
            {/* Removed resize handle to prevent dragging beyond limits */}
            <Panel minSize={30} className="min-w-0 min-h-0 overflow-hidden bg-app-bg">
              <div className="h-full min-h-0 flex flex-col">
                {view === 'dashboard' ? (
                  <Dashboard onEnterChat={() => setView('chat')} />
                ) : (
                  <ChatPane />
                )}
              </div>
            </Panel>
          </PanelGroup>
        </div>
        {isDesktop && sidebarCollapsed && view === 'dashboard' && (
          <button
            title="Show sidebar"
            onClick={() => setSidebarCollapsed(false)}
            className="fixed left-2 top-16 z-40 p-2 rounded-full bg-[color:var(--surface)] border border-[color:var(--border)] shadow hover:bg-[color:var(--surface)]"
          >
            <ChevronRight size={16} />
          </button>
        )}
      </div>
      <CommandPalette
        open={cmdOpen}
        onClose={() => setCmdOpen(false)}
        onAction={(id) => {
          if (id === 'chat') setView('chat')
          if (id === 'dashboard') setView('dashboard')
          if (id === 'toggle-theme') setTheme((t) => (t === 'dark' ? 'light' : 'dark'))
          if (id === 'upload') {
            if (isDesktop) setSidebarCollapsed(false)
            else setMobileSidebarOpen(true)
          }
        }}
      />
      {!isDesktop && mobileSidebarOpen && (
        <Sidebar
          isMobile
          onClose={() => setMobileSidebarOpen(false)}
        />
      )}
    </AppContext.Provider>
  )
}
