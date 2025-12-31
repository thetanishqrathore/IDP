import { defineConfig } from 'vite';

// If you prefer to proxy during dev, uncomment and set target
// const API_TARGET = process.env.VITE_API_URL || 'http://localhost:8000';

export default defineConfig({
  server: {
    port: 5173,
    // proxy: {
    //   '/healthz': { target: API_TARGET, changeOrigin: true },
    //   '/ingest': { target: API_TARGET, changeOrigin: true },
    //   '/search': { target: API_TARGET, changeOrigin: true },
    //   '/answer': { target: API_TARGET, changeOrigin: true },
    //   '/answer_stream': { target: API_TARGET, changeOrigin: true },
    //   '/ui': { target: API_TARGET, changeOrigin: true },
    // },
  },
});

