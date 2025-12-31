import React from 'react'
import Markdown from './Markdown.jsx'
import { Copy, RefreshCw, ThumbsDown, ThumbsUp, Pencil } from 'lucide-react'

export default function MessageBubble({
  role = 'assistant',
  text = '',
  citations = [],
  status,
  onCopy,
  onRegenerate,
  onEdit,
  onFeedback,
  onCitationClick,
}) {
  const isUser = role === 'user'

  const handleCopy = () => {
    if (onCopy) {
      onCopy()
    } else {
      navigator.clipboard.writeText(text)
    }
  }

  return (
    <div
      className={`animate-fade-in-up group relative flex items-start gap-3 ${
        isUser ? 'justify-end' : 'justify-start'
      }`}
    >
      {/* Content Wrapper */}
      <div
        className={[
          'relative w-fit max-w-[85%] md:max-w-[44rem] shadow-sm transition-all',
          isUser
            ? 'bg-[#1E1F20] text-[#E3E3E3] px-6 py-3.5 rounded-[24px] font-normal' // Gemini: Fully rounded pill, #1E1F20 bg
            : 'text-text-primary md-content bg-transparent px-0 py-2 hover:bg-transparent transition-colors',
        ].join(' ')}
      >
        {/* Content */}
        {status === 'skeleton' ? (
           <div className="flex gap-1.5 py-1 pl-1">
             <div className="w-2 h-2 rounded-full bg-gradient-to-r from-[#4285F4] to-[#9B72CB] animate-[pulse-dot_1.2s_ease-in-out_infinite]" style={{animationDelay: '0ms'}} />
             <div className="w-2 h-2 rounded-full bg-gradient-to-r from-[#4285F4] to-[#9B72CB] animate-[pulse-dot_1.2s_ease-in-out_infinite]" style={{animationDelay: '200ms'}} />
             <div className="w-2 h-2 rounded-full bg-gradient-to-r from-[#4285F4] to-[#9B72CB] animate-[pulse-dot_1.2s_ease-in-out_infinite]" style={{animationDelay: '400ms'}} />
           </div>
        ) : isUser ? (
          <div className="text-[16px] leading-7 whitespace-pre-wrap break-words font-sans antialiased tracking-normal">{text}</div>
        ) : (
          <Markdown
            text={text}
            citations={citations}
            onCitationClick={onCitationClick}
            showCursor={!isUser && status === 'streaming'}
          />
        )}

        {/* User Actions (Edit/Copy) - Only visible on hover */}
        {isUser && (
          <div className="absolute top-1/2 -left-16 -translate-y-1/2 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-all duration-200">
             <button
              title="Edit"
              onClick={onEdit}
              className="p-1.5 rounded-full text-zinc-500 hover:text-white hover:bg-white/10 transition-colors"
            >
              <Pencil size={14} strokeWidth={1.5} />
            </button>
            <button
              title="Copy"
              onClick={handleCopy}
              className="p-1.5 rounded-full text-zinc-500 hover:text-white hover:bg-white/10 transition-colors"
            >
              <Copy size={14} strokeWidth={1.5} />
            </button>
          </div>
        )}

        {/* Floating actions (bottom-right of the bubble for assistant) */}
        {!isUser && status !== 'streaming' && (
          <div className="flex items-center gap-1 mt-4 pt-3 border-t border-white/5 justify-end opacity-0 group-hover:opacity-100 transition-all duration-200 transform translate-y-1 group-hover:translate-y-0">
            <button
              title="Regenerate"
              onClick={onRegenerate}
              className="p-1.5 rounded-md text-zinc-500 hover:text-accent hover:bg-accent/10 transition-colors"
            >
              <RefreshCw size={13} strokeWidth={1.5} />
            </button>
            <button
              title="Copy"
              onClick={handleCopy}
              className="p-1.5 rounded-md text-zinc-500 hover:text-accent hover:bg-accent/10 transition-colors"
            >
              <Copy size={13} strokeWidth={1.5} />
            </button>
            <div className="w-[1px] h-3 bg-white/5 mx-1" />
            <button
              title="Helpful"
              onClick={() => onFeedback?.('up')}
              className="p-1.5 rounded-md text-zinc-500 hover:text-success hover:bg-success/10 transition-colors"
            >
              <ThumbsUp size={13} strokeWidth={1.5} />
            </button>
            <button
              title="Not helpful"
              onClick={() => onFeedback?.('down')}
              className="p-1.5 rounded-md text-zinc-500 hover:text-error hover:bg-error/10 transition-colors"
            >
              <ThumbsDown size={13} strokeWidth={1.5} />
            </button>
          </div>
        )}
      </div>
    </div>
  )
}