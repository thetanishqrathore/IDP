/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{js,jsx,ts,tsx}',
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        // "True Black" & Deep Charcoal Foundation
        'app-bg': '#050505',      // Deepest black
        'sidebar-bg': '#020202',  // Almost pure black
        'card-bg': '#0A0A0A',     // Rich charcoal
        'surface': '#121212',     // Interactive surface
        'surface-hover': '#1E1E1E', 
        'surface-active': '#27272A', // Active state
        'border': '#1F1F22',      // Subtle, high-end border
        
        // Typography
        'text-primary': '#FFFFFF', // Pure White for max contrast
        'text-secondary': '#E4E4E7', // Zinc 200 - Very bright grey
        'text-muted': '#A1A1AA', // Zinc 400 - Readable subtext

        // Vibrant Accents (Gradient-ready)
        'accent': '#6366f1',      // Indigo Base
        'accent-glow': '#4f46e5', // Violet Glow
        'sidebar-accent': '#818cf8',
        
        // Semantic
        'success': '#10b981',
        'warning': '#f59e0b',
        'error': '#ef4444',
      },
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'monospace'],
      },
      boxShadow: {
        'glow': '0 0 25px -5px rgba(99, 102, 241, 0.25)', // Softer, wider glow
        'glass': '0 8px 32px 0 rgba(0, 0, 0, 0.5)',
        'inner-light': 'inset 0 1px 0 0 rgba(255, 255, 255, 0.03)', // Subtle top highlight
      },
      backgroundImage: {
        'gradient-premium': 'linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%)', // Indigo -> Violet
        'sidebar-gradient': 'linear-gradient(to bottom, rgba(5,5,5,0.95), rgba(5,5,5,1))',
        'glass-gradient': 'linear-gradient(145deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.01) 100%)',
      },
      keyframes: {
        'fade-in-up': {
          '0%': { opacity: '0', transform: 'translateY(10px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        'pulse-dot': {
          '0%, 100%': { opacity: '0.4', transform: 'scale(0.8)' },
          '50%': { opacity: '1', transform: 'scale(1)' },
        }
      },
      animation: {
        'fade-in-up': 'fade-in-up 0.4s ease-out forwards',
        'pulse-dot': 'pulse-dot 1.2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      }
    },
  },
  plugins: [
    require('tailwind-scrollbar'),
    require('@tailwindcss/typography'),
  ],
}