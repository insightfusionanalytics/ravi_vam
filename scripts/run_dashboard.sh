#!/usr/bin/env bash
set -euo pipefail

# Run from the project root no matter where this script is called from.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${1:-8000}"

echo ""
echo "  RAVI VAM Strategy Platform"
echo "  ──────────────────────────────────────────"
echo "  Strategy selector:  http://localhost:${PORT}/"
echo "  Direct dashboard:   http://localhost:${PORT}/dashboard/?strategy=step2_upro_tqqq_6state"
echo "  API docs:           http://localhost:${PORT}/docs"
echo ""
echo "  Old static dashboard still available:"
echo "  python3 -m http.server 8081  →  http://localhost:8081/delivery/dashboard/index.html"
echo "──────────────────────────────────────────"
echo ""

# Use .venv if available, otherwise fall back to system uvicorn
if [ -f "$ROOT_DIR/.venv/bin/uvicorn" ]; then
  UVICORN="$ROOT_DIR/.venv/bin/uvicorn"
else
  UVICORN="uvicorn"
fi

"$UVICORN" app.main:app --reload --host 0.0.0.0 --port "$PORT"
