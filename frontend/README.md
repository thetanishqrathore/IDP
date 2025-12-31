# Second Brain React UI

A modern, elegant React UI for the FastAPI backend.

## Dev Setup

- Ensure the API is running and reachable at `http://localhost:8000` (default).
- In this `frontend/` folder:
  - Install deps: `npm install`
  - Run dev server: `npm run dev`
  - Optionally set a custom API URL: `VITE_API_URL=http://localhost:8000 npm run dev`

Tailwind is integrated via PostCSS (`@tailwind` directives in `src/index.css`) and configured with `tailwind.config.js`. Vite handles module bundling and HMR.

## Features

- Centered welcome → conversation transition like ChatGPT
- Resizable/collapsible sidebar with modern icons
- Document ingest (multi-file upload), recent uploads, and listing
- Clean kebab menus for file actions (view, delete)
- Chat with streaming answers (`/answer_stream`) in polished bubbles + avatars
- Copy/regenerate/feedback actions on assistant responses
- Citations panel with document preview via presigned links
- Scoped questions to selected documents with saved scopes
- Search with highlighted snippets
- Command palette (⌘K / Ctrl+K)

## Notes

- CORS is permissive by default in the backend; adjust `CORS_ALLOW_ORIGINS` as needed.
- For production, use `npm run build:copy` to copy the built UI into `app/ui` for serving via the backend.

## Libraries

- Icons: `lucide-react`
- Resizable panels: `react-resizable-panels`
