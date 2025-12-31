
FROM node:20-alpine AS ui
WORKDIR /frontend
COPY frontend/package.json ./
# Install deps
RUN npm install --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM python:3.10-slim-bookworm AS api
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_INSTALL_DIR="/usr/local/bin" \
    PATH="/app/.venv/bin:$PATH"

# System deps: build tools + runtime libs for OCR/Office conversion
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential wget ca-certificates curl \
    libreoffice \
    tesseract-ocr libtesseract-dev \
    fonts-noto-core fonts-noto-cjk \
    poppler-utils \
    libglib2.0-0 libgl1 libsm6 libxrender1 libxext6 \
  && rm -rf /var/lib/apt/lists/*

# Install uv using official script
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Create venv and install deps with disk space awareness
COPY app/requirements.txt ./requirements.txt
ENV TMPDIR="/app"
RUN uv venv /app/.venv && \
    uv pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

RUN uv pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/uv /app/.cache

ENV MINERU_HOME=/models/mineru \
    MINERU_CACHE_DIR=/models/mineru/.cache

RUN mkdir -p "$MINERU_HOME"

# Copy backend app code
COPY app/ .

COPY deploy/cloud/init_parsers.sh /app/init_parsers.sh
RUN chmod +x /app/init_parsers.sh

# Copy built UI into app/ui served by FastAPI
COPY --from=ui /frontend/dist /app/ui

EXPOSE 8000
CMD ["/app/init_parsers.sh"]
