#!/usr/bin/env bash
set -euo pipefail

if [ "${MINERU_DOWNLOAD_MODELS:-true}" != "false" ]; then
  MINERU_BIN="/app/.venv/bin/mineru"
  if [ -f "$MINERU_BIN" ]; then
    mkdir -p "${MINERU_HOME:-/models/mineru}"
    export MINERU_HOME="${MINERU_HOME:-/models/mineru}"
    export MINERU_CACHE_DIR="${MINERU_CACHE_DIR:-${MINERU_HOME}/.cache}"
    if [ ! -f "$MINERU_HOME/metadata.json" ]; then
      echo "[init] Triggering MinerU model download by running on dummy file..."
      
      # Create a dummy empty PDF to trigger model download
      DUMMY_PDF="/tmp/warmup.pdf"
      # Create a valid minimal PDF using python to avoid 'invalid pdf' errors
      /app/.venv/bin/python3 -c "from reportlab.pdfgen import canvas; c=canvas.Canvas('$DUMMY_PDF'); c.drawString(100,100,'warmup'); c.save()"
      
      # Run mineru on it; this triggers model download if missing. 
      # We ignore the output/errors related to the file itself, we just want the download.
      "$MINERU_BIN" -p "$DUMMY_PDF" -o /tmp/warmup_out -m auto >/dev/null 2>&1 || true
      
      echo "[init] Model download trigger complete."
    fi
  else
    echo "[init] mineru executable not found at $MINERU_BIN; skipping model bootstrap" >&2
  fi
fi

exec uvicorn main:app --host 0.0.0.0 --port 8000
