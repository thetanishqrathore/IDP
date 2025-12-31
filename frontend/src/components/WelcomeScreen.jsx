import React from 'react'
import { Sparkles, Terminal, Shield, Zap } from 'lucide-react'
import Composer from './Composer.jsx'

export default function WelcomeScreen({ onSend }) {
  return (
    <div className="w-full max-w-2xl px-6 -mt-16 md:-mt-24 fade-in-up">
      <div className="flex justify-center mb-10">
        <div className="relative group">
          <div className="absolute inset-0 bg-accent blur-[60px] opacity-20 rounded-full group-hover:opacity-30 transition-opacity duration-700"></div>
          <div className="relative w-20 h-20 bg-gradient-premium rounded-2xl shadow-glow flex items-center justify-center transform -rotate-3 group-hover:rotate-3 transition-transform duration-700 ease-out">
            <Sparkles size={48} className="text-white" />
          </div>
        </div>
      </div>
      
      <div className="space-y-4 mb-12">
        <h1 className="text-4xl md:text-6xl font-bold text-center tracking-tight text-transparent bg-clip-text bg-gradient-to-b from-white via-zinc-200 to-zinc-500 pb-1">
          Your Second Brain <br/>
        </h1>
        <p className="text-center text-text-secondary text-lg md:text-xl max-w-lg mx-auto leading-relaxed font-medium">
          Ask me anthing!
        </p>
      </div>

      <Composer onSend={onSend} className="mb-12" />

      <div className="grid grid-cols-3 gap-4 opacity-40">
        <FeatureHint icon={<Terminal size={14}/>} label="API Ready" />
        <FeatureHint icon={<Shield size={14}/>} label="Enterprise" />
        <FeatureHint icon={<Zap size={14}/>} label="SOTA RAG" />
      </div>
    </div>
  )
}

function FeatureHint({ icon, label }) {
  return (
    <div className="flex flex-col items-center gap-2">
      <div className="p-2 rounded-full bg-white/5 border border-white/10 text-zinc-400">
        {icon}
      </div>
      <span className="text-[10px] font-black uppercase tracking-widest text-zinc-500">{label}</span>
    </div>
  )
}