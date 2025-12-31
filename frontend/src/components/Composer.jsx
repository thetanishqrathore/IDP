import React, { useState, useRef, useLayoutEffect, forwardRef, useImperativeHandle } from 'react'
import { Send } from 'lucide-react'
import { motion } from 'framer-motion'

const Composer = forwardRef(({ onSend, streaming, placeholder = "Ask me anything... ", className = "" }, ref) => {
  const [input, setInput] = useState('')
  const [focused, setFocused] = useState(false)
  const inputRef = useRef(null)

  useImperativeHandle(ref, () => ({
    setInput: (val) => setInput(val),
    focus: () => inputRef.current?.focus()
  }))

  useLayoutEffect(() => {
    const el = inputRef.current
    if (!el) return
    el.style.height = 'auto' 
    const h = Math.min(el.scrollHeight, 200)
    el.style.height = h + 'px'
  }, [input])

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      submit()
    }
  }

  const submit = () => {
    if (!input.trim() || streaming) return
    onSend(input)
    setInput('')
  }

  return (
    <div className={`relative w-full ${className}`}>
      <motion.div 
        layout 
        className={`
          relative bg-card-bg/90 backdrop-blur-2xl border border-border
          rounded-[32px] shadow-glass flex items-center gap-2 p-1.5 md:p-2
          transition-all duration-500 ease-in-out
          ${focused ? 'border-accent/40 shadow-glow' : 'hover:border-white/10'}
        `}
      >
        <textarea
          ref={inputRef}
          rows={1}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          placeholder={placeholder}
          disabled={streaming}
          className="w-full bg-transparent border-none focus:ring-0 resize-none max-h-[200px] py-2 md:py-3 px-5 text-[14px] md:text-[16px] text-text-primary placeholder-text-muted font-normal leading-relaxed outline-none disabled:opacity-50 overflow-hidden"
        />
        <button
          onClick={submit}
          disabled={streaming || !input.trim()}
          className={`
            shrink-0 w-10 h-10 md:w-12 md:h-12 flex items-center justify-center rounded-full transition-all duration-300
            ${input.trim() && !streaming 
              ? 'bg-gradient-premium text-white shadow-glow hover:scale-105 active:scale-95' 
              : 'bg-surface text-zinc-600 cursor-not-allowed'}
          `}
        >
          <Send size={18} strokeWidth={2} className="md:w-5 md:h-5" />
        </button>
      </motion.div>
      <div className="flex items-center justify-center gap-4 mt-3">
        <p className="text-[10px] text-text-muted font-bold uppercase tracking-widest opacity-60">
          AI can make mistakes. So double check it. 
        </p>
      </div>
    </div>
  )
})

export default Composer