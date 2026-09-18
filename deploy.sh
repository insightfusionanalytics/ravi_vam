#!/usr/bin/env bash
set -euo pipefail

# Run from the project root no matter where this is called from.
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "==> git pull"
git pull

echo "==> installing dependencies"
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements-dev.txt

echo "==> running test suite (deploy aborts if this fails)"
.venv/bin/pytest

echo "==> regenerating dashboard precomputed snapshots (so the default view is never stale)"
.venv/bin/python3 scripts/generate_precomputed_snapshots.py

echo "==> restarting service"
sudo systemctl restart ravi-vam

echo "==> health check"
sleep 2
# The systemd unit (deploy/ravi-vam.service) runs uvicorn on 8005, not 8000 —
# this was checking the wrong port entirely, so a 404 here didn't actually
# mean the app was broken. Fixed 2026-09-18.
printf "  /                -> "; curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8005/
printf "  /api/strategies  -> "; curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8005/api/strategies
printf "  /docs            -> "; curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8005/docs

echo "==> done"
