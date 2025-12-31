# IDP: Enterprise-Grade Intelligent Document Processing & RAG Engine

![Status](https://img.shields.io/badge/Status-Production%20Ready-success)
![Python](https://img.shields.io/badge/Backend-FastAPI-blue)
![React](https://img.shields.io/badge/Frontend-React%20%2B%20Vite-61DAFB)
![Docker](https://img.shields.io/badge/Deploy-Docker-2496ED)

**IDP** is a modular, high-performance platform designed to transform complex, unstructured documents (PDFs, DOCX, Images) into structured, queryable knowledge. It combines a state-of-the-art **RAG (Retrieval-Augmented Generation)** pipeline with a **headless API** architecture, making it the perfect engine for building custom AI knowledge bases and automation workflows (n8n, Zapier).

---

## 🚀 Key Features

### 🧠 Advanced RAG Architecture
- **Hybrid Search:** Combines **Vector Search** (semantic understanding) with **Keyword Search** (BM25) using Reciprocal Rank Fusion (RRF) for superior retrieval accuracy. This ensures specific identifiers (like Invoice IDs or Dates) are retrieved just as effectively as semantic concepts.
- **Context-Aware Chunking:** Intelligently splits documents based on layout and semantic structure, preserving header hierarchies to solve the "lost context" problem.
- **Graph-Based Expansion:** Boosts relevance by retrieving structurally related content (e.g., finding the paragraph *immediately following* a relevant header).
- **Citations & Grounding:** Generates answers with direct citations to source documents and calculates a "Groundedness Score" to detect hallucinations.
- **Visual Citations:** Interactive citation badges in the UI link directly to the source page and text block.
- **Hallucination Control:** Implements a strict *"Groundedness Scoring"* system. Every answer includes direct citations to source paragraphs and a confidence score.

### ⚡ Powerful Processing Pipeline
- **Universal Ingestion:** Accepts files via UI upload, API, or direct URL fetch (webhooks ready).
- **Canonical Normalization:** Converts all inputs into a standardized **Markdown-first** format, optimizing for LLM token usage while preserving table structures.
- **Asynchronous Processing:** Built-in lightweight job worker ensures high-volume document processing happens in the background without blocking the UI.
- **Robust Agents:** Dedicated services for Ingestion, Normalization, Extraction, Chunking, and Embedding.

### 🛠️ Developer & Automation Friendly
- **Headless API Engine:** Fully documented FastAPI backend secured with Bearer tokens (`IDP_API_KEY`).
- **OpenAI-Compatible Endpoint:** Drop-in replacement for GPT-4 in existing tools (`/v1/chat/completions`).
- **"Lego-Like" Modularity:** Designed for low-code automation.
    - **Google Drive / OneDrive:** Trigger ingestion when a file is added to a specific folder.
    - **Email:** Automatically process attachments sent to a dedicated inbox.
    - **Telegram / Slack:** Build chat bots that query your knowledge base in real-time.
- **Integration Ready:** Dedicated endpoints (`/ingest/url`) for n8n, Zapier, and active automation platforms.
- **Local LLM Support:** Fully compatible with Ollama and local embedding models for privacy-focused deployments.

### 🎨 Premium UI Experience
- **Modern chat Design:** Dark mode interface with premium glassmorphism, rounded pill-shaped messages, and refined typography.
- **Reader View:** A clean, responsive "Source Drawer" that renders retrieved context in readable HTML, not just raw text.
- **Real-time Insights:** Visual execution streams, job tracking, and pipeline stage visualization.

---

## 🏗️ Architecture

IDP follows a streamlined architecture managed via a single Docker Compose definition:

| Service | Tech Stack | Role |
| :--- | :--- | :--- |
| **API & UI** | Python, FastAPI | Serves both the headless API and the React frontend |
| **Vector DB** | Qdrant | Semantic Search & Embeddings |
| **Storage** | MinIO (S3 Compatible) | Blob storage for raw files & artifacts |
| **Database** | PostgreSQL | Metadata, structured data, job history |
| **Worker** | Python Threading | Background async processing (OCR, Chunking) |

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- API Key (OpenAI or Gemini) **OR** Local LLM (Ollama)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/thetanishqrathore/IDP.git
    cd IDP
    ```

2.  **Configure Environment:**
    Copy the example environment file and add your credentials.
    ```bash
    cp env-example.txt .env
    ```
    Edit `.env` to set your `POSTGRES_PASSWORD`, `MINIO_ROOT_PASSWORD`, and API keys.

3.  **Launch the Stack:**
    You can use the helper script or standard Docker Compose commands.
    ```bash
    # Using helper script (handles rebuilds easily)
    ./deploy/cloud/run.sh up

    # OR standard Docker Compose
    docker compose up -d
    ```
    *Note: The first run may take a few minutes to download the OCR and embedding models.*

4.  **Access the Platform:**
    - **Frontend & API:** `http://localhost` (Port 80)
    - **API Docs:** `http://localhost/docs`
    - **MinIO API:** `http://localhost:9000`

---

## 🔍 Deep Dive: Algorithmic Pipeline

1.  **Ingestion:** Files are hashed (SHA256) for deduplication and MIME-typed using `python-magic`.
2.  **Normalization:**
    - **Advanced Parsing (SOTA):** Integrated support for **MinerU (2.0+)** and **Docling** for high-fidelity PDF-to-Markdown conversion. These parsers excel at reconstructing complex tables, mathematical formulas, and multi-column layouts.
    - **Hybrid Fallback:** Automatically switches between parsers based on content type or user preference (`PARSE_METHOD=auto|mineru|docling`).
    - **Legacy Support:** Robust fallbacks using PyMuPDF and Tesseract OCR for standard documents.
    - **Office:** `mammoth` for DOCX, `python-pptx` for slides.
3.  **Extraction:** Content is parsed into semantic blocks (Header, Paragraph, Table) with unique IDs.
4.  **Chunking:** Strategies vary by content type (e.g., "Layout-Aware" for tables, "Section-Based" for narrative text). Metadata is injected into every chunk.
5.  **Embedding:** Supports OpenAI `text-embedding-3-large` or local models (e.g., `BAAI/bge-m3`). Incremental upserts ensure efficiency.
6.  **Retrieval & Generation:** Queries are expanded, searched across vectors and keywords, re-ranked, and passed to the LLM for final answer synthesis.

---

## 🤝 Services & Support

**Need a Custom Enterprise Deployment?**

IDP is the open-source core of our document intelligence platform. For custom integrations, high-availability setups, or managed support, contact the core engineering team at **RaeonLabs**.

* **Custom Connectors:** Integration with SAP, Salesforce, or proprietary data lakes.
* **Managed Infrastructure:** Fully managed AWS/Azure deployments.
* **SLA Support:** 24/7 enterprise support.

[**Contact RaeonLabs**](https://www.raeonlabs.com)

## 🛡️ License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.