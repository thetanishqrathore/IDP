#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# project root is two levels up from this script (deploy/cloud)
repo="$(cd "$here/../.." && pwd)"

usage() {
  cat << USAGE >&2
Cloud runner for API + Postgres + MinIO + Qdrant

Usage:
  $0 up [--rebuild]
  $0 down
  $0 restart [--rebuild]
  $0 ps|logs

Notes:
  - Run from any directory; script switches to repo root.
  - Docker Compose automatically reads .env from the repo root.
  - Set your EXTERNAL_IP-dependent values directly in .env.
USAGE
}

ensure_tools() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is required but not found" >&2; exit 1
  fi
  if ! docker compose version >/dev/null 2>&1; then
    echo "Docker Compose plugin is required (docker compose)." >&2; exit 1
  fi
}

cmd="${1:-up}"; shift || true
rebuild="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rebuild) rebuild="true"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1;;
  esac
done

ensure_tools
cd "$repo"

compose_files=(
  -f docker-compose.yml
)

case "$cmd" in
  up)
    if [[ "$rebuild" == "true" ]]; then buildflag=("--build"); else buildflag=(); fi
    docker compose "${compose_files[@]}" up -d "${buildflag[@]}"
    ;;
  down)
    docker compose "${compose_files[@]}" down
    ;;
  restart)
    docker compose "${compose_files[@]}" down || true
    if [[ "$rebuild" == "true" ]]; then buildflag=("--build"); else buildflag=(); fi
    docker compose "${compose_files[@]}" up -d "${buildflag[@]}"
    ;;
  ps)
    docker compose "${compose_files[@]}" ps
    ;;
  logs)
    docker compose "${compose_files[@]}" logs -f
    ;;
  *)
    usage; exit 1;;
esac
