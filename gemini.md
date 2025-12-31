# IDP-v2 Codebase Summary

**Date:** December 26, 2025
**Project:** Intelligent Document Processing (IDP) Pipeline & RAG Engine

## 1. System Overview
IDP-v2 is a production-grade, modular RAG (Retrieval-Augmented Generation) platform designed to process complex documents (PDF, DOCX, Images) into structured knowledge for AI querying. It operates as both a **standalone web application** and a **headless API engine** suitable for automation workflows (n8n, Zapier).

## 2. Core Architecture

### **Backend (FastAPI + Python)**
The backend is structured as a series of decoupled services:
*   **Ingestion Service:** Handles file uploads and URL downloads. Supports parallel processing for high throughput.
    *   *Feature:* **Universal Ingestion** (`/ingest/url`) allows fetching files directly from webhooks.
*   **Normalization Service:** Converts raw files into a "Canonical" format.
    *   *Strategy:* **Markdown-First**. Prefers Markdown tables for token efficiency but generates clean XHTML for the UI "Reader View".
*   **Extraction Service:** Identifies semantic blocks (headers, paragraphs, tables) from the canonical output.
*   **Chunking Service:** Splits text intelligently using a **Context-Aware** strategy.
    *   *Feature:* Injects document filenames and header paths (e.g., `[Doc: Report.pdf] / Section 1`) into chunks to improve vector retrieval accuracy.
*   **Embedding Service:** Generates vectors using OpenAI or local models. Supports incremental upserts via checksums.
*   **Retrieval Service:** Implements **Hybrid Search** (Vector + Keyword) with Reciprocal Rank Fusion (RRF) and Graph-based expansion.
*   **Generation Service:** Orchestrates the final LLM answer generation with citations.

### **Frontend (React + Tailwind + Vite)**
A modern, "Premium Dark" interface focused on readability and control.
*   **ChatPane:** OpenAI-style chat interface with streaming responses, glassmorphism effects, and floating composer.
*   **Sidebar (Control Center):**
    *   **Integration Deck:** Exposes API endpoints and keys for n8n/Zapier.
    *   **Execution Stream:** Visualizes background processing jobs.
    *   **Source Badges:** Distinguishes between files uploaded manually vs. via Webhook (`WEB`, `AUTO`).
*   **SourceDrawer (Reader View):** Displays retrieved context in a clean, responsive "Reader Mode" rather than a broken visual clone of the PDF.

### **Infrastructure**
*   **PostgreSQL:** Stores metadata, processing events, extraction blocks, and structured data (Invoices/Contracts).
*   **MinIO (S3):** Stores raw files (blobs) and canonical HTML/JSON artifacts.
*   **Qdrant:** Vector database for semantic search.
*   **Docker Compose:** Orchestrates the entire stack.

## 3. Key Capabilities

### **A. Headless Engine & Automation**
The system is designed to be "n8n-friendly":
1.  **OpenAI-Compatible Endpoint:** `POST /v1/chat/completions`. Allows the RAG engine to be used as a drop-in replacement for GPT-4 in any chatbot tool.
2.  **API Key Security:** Protected by `IDP_API_KEY` (Bearer Token).
3.  **URL Ingestion:** Can ingest files directly from URLs passed by automation triggers (Email, Slack, Drive).

### **B. Advanced RAG Techniques**
1.  **Canonical Markdown:** Prevents token bloat by stripping HTML tags before embedding, while preserving table structure.
2.  **Contextual Header Injection:** Solves the "lost context" problem by prepending document metadata to every chunk.
3.  **Hybrid Retrieval with Graph:** Boosts retrieval relevance by checking structural neighbors (e.g., the paragraph *after* a relevant header).

## 4. Directory Structure Highlights

```text
/
├── app/
│   ├── api/            # Endpoints (v1_openai.py, ingest.py, etc.)
│   ├── services/       # Core Logic (ingestion.py, chunking.py, etc.)
│   ├── infra/          # DB & Storage Clients
│   └── main.py         # App Entrypoint
├── frontend/
│   ├── src/components/ # ChatPane, Sidebar, SourceDrawer
│   └── src/index.css   # Tailwind & Premium Theme Definitions
├── deploy/             # Dockerfiles & Cloud Scripts
└── docker-compose.yml  # Stack Definition
```

## 5. Usage

**Start the Stack:**
```bash
./run.sh up
```

**Access Points:**
*   **Web UI:** `http://localhost:8000` (or Port 80 in prod)
*   **API Docs:** `http://localhost:8000/docs`
*   **OpenAI Base URL:** `http://localhost:8000/v1`

**Environment Variables (.env):**
*   `IDP_API_KEY`: Secures the headless API.
*   `OPENAI_API_KEY`: For embeddings/generation.
*   `MINERU_DOWNLOAD_MODELS`: Controls local OCR model usage.
