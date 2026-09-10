#!/usr/bin/env bash
set -euo pipefail

# Run from the project root no matter where this is called from.
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "==> git pull"
git pull

echo "==> installing dependencies"
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements.txt

echo "==> restarting service"
sudo systemctl restart ravi-vam

echo "==> health check"
sleep 2
printf "  /                -> "; curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/
printf "  /api/strategies  -> "; curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/api/strategies
printf "  /docs            -> "; curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/docs

echo "==> done"
