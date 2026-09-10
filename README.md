# Ravi VAM Strategy Platform

Config-driven backtesting platform for regime-rotation trading strategies — a single FastAPI service that serves both the JSON API (`/api/*`) and its own frontend (strategy selector + interactive dashboard) as static files. There is no separate frontend build or separate backend service: one process, one deploy.

## Stack

- **FastAPI + Uvicorn** (Python) — serves the API under `/api/*`, its own interactive docs at `/docs`/`/redoc`/`/openapi.json`, and the static frontend (`/`, `/dashboard/*`) from the same process
- **Plain HTML/JS frontend** (`frontend/`) — no build step, no Node.js required
- **pandas / numpy / scipy** — backtest engines (`app/engines/`, `scripts/`)
- **yfinance** — automatic fallback market-data source when DataBento CSVs aren't present locally
- **Nginx** — SSL termination + reverse proxy to Uvicorn
- **Let's Encrypt (Certbot)**

## 1) Local development

Requirements

- Python 3.11 (3.10–3.12 all work; 3.11 matches production/render.yaml)
- pip

Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./scripts/run_dashboard.sh          # or: uvicorn app.main:app --reload --port 8000
```

Open:

- Strategy selector — http://localhost:8000/
- Dashboard — http://localhost:8000/dashboard/?strategy=step2_upro_tqqq_6state
- API docs — http://localhost:8000/docs

There's no separate dev server or proxy to configure — unlike a split frontend/backend setup, one process serves the API and the static pages together, so what you run locally is exactly what runs in production.

## 2) Environment variables

Nothing is required to run the app — on first start it looks for DataBento CSVs, and if it can't find any, downloads equivalent data from Yahoo Finance into `./data` automatically.

| Variable | Default | Used by |
|---|---|---|
| `RAVI_DATA_DIR` | unset → falls back to `./data` (auto-populated via yfinance) | `app/config.py: find_data_dir()` — point this at a directory containing `databento/equities/*.csv` to use real DataBento data instead of the Yahoo Finance fallback |

## 3) Ubuntu production setup

### 3.1 Prerequisites

- Ubuntu 22.04+ VM with SSH + sudo access
- `backtestravi.insightfusionanalytics.com` pointed at the VM — see §3.2
- Outbound internet access from the VM (needed for the first-run Yahoo Finance data download, unless `RAVI_DATA_DIR` points at pre-existing CSVs)

Install base tools:

```bash
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx python3-venv python3-pip git
python3 --version   # 3.10+ is fine; 3.11 recommended to match render.yaml
```

### 3.2 DNS

In the DNS provider for `insightfusionanalytics.com`, add:

- **A record:** `backtestravi` → `<VM_PUBLIC_IP>`
- (optional) **AAAA record** if the VM has IPv6

Wait for propagation before running Certbot — it validates over HTTP and fails if the domain doesn't resolve to this box yet.

### 3.3 App setup — clone, venv, systemd service

The app runs as `chirag` (has sudo on this box) rather than a dedicated no-login system user — simpler, and fine for a single-operator deploy:

```bash
sudo mkdir -p /home/chirag/ravi_vam
sudo chown -R chirag:chirag /home/chirag/ravi_vam

git clone <repo-url> /home/chirag/ravi_vam
cd /home/chirag/ravi_vam
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

Install the systemd unit — `deploy/ravi-vam.service` is checked into this repo, copy it as-is:

```bash
sudo cp deploy/ravi-vam.service /etc/systemd/system/ravi-vam.service
sudo systemctl daemon-reload
sudo systemctl enable --now ravi-vam
sudo systemctl status ravi-vam
```

Confirm it's up **before** touching nginx — the app should already be answering on localhost:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8005/
curl -s http://127.0.0.1:8005/api/strategies | head -c 200
```

### 3.4 nginx — bootstrap FIRST, Certbot SECOND

This order matters and skipping it is the most common way to break nginx on a box that's already serving other sites. Certbot's SSL block references certificate files (`/etc/letsencrypt/live/.../fullchain.pem`) that don't exist until Certbot has actually issued them. If a `listen 443 ssl` block pointing at those paths is live before that happens, nginx refuses to start or reload:

```
nginx: [emerg] cannot load certificate ".../fullchain.pem": No such file or directory
```

...and every other site nginx serves on this box goes down with it, not just this one — `nginx -t` tests the entire config, and one broken `sites-enabled` file fails the whole reload. So:

**Step 1** — the app is already running on `127.0.0.1:8005` from §3.3. Nginx will proxy to it directly; there's no static bundle to `rsync` first (the app serves its own frontend).

**Before Step 2** — check nothing already claims this `server_name`:
```bash
grep -rl "server_name backtestravi.insightfusionanalytics.com" /etc/nginx/sites-available/
```
If that returns a file already, **stop** — don't create a second `sites-available`/`sites-enabled` entry for the same domain under a different filename. Two files with the same `server_name` don't merge; nginx silently picks one for each `listen` directive and ignores the other (`conflicting server name ... ignored` in `nginx -t`/reload output), which is exactly how this domain ended up half-working from one file and half from another. If a config already exists, skip straight to editing/using that file instead of bootstrapping a new one.

**Step 2** — install the HTTP-only bootstrap config, `deploy/nginx-backtestravi-bootstrap.conf`. It has no `listen ... ssl` and no `ssl_certificate` line, so there is nothing in it that can fail to load:

```bash
sudo cp deploy/nginx-backtestravi-bootstrap.conf /etc/nginx/sites-available/backtestravi
sudo ln -sfn /etc/nginx/sites-available/backtestravi /etc/nginx/sites-enabled/backtestravi
sudo nginx -t
sudo systemctl reload nginx
```

Confirm `http://backtestravi.insightfusionanalytics.com` serves the site before continuing.

**Step 3** — run Certbot, only now:

```bash
sudo certbot --nginx -d backtestravi.insightfusionanalytics.com
```

Certbot's `--nginx` plugin issues the certificate, then rewrites `/etc/nginx/sites-available/backtestravi` itself — adding the HTTP→HTTPS redirect and the `listen 443 ssl` server block, with `# managed by Certbot` markers. You don't hand-write that part.

Test renewal (does not actually renew, just validates the process works):

```bash
sudo certbot renew --dry-run
```

**Step 4** — check the result. The file should now match `deploy/nginx-backtestravi.conf`, a checked-in copy of the live config kept for reference and disaster recovery — restore that file (not the bootstrap one) if `/etc/nginx/sites-available/backtestravi` is ever lost, since it already has both the SSL block and the `/api` + `/docs` routing below.

If you ever need to update `server_name` or add a `www` alias, edit `/etc/nginx/sites-available/backtestravi` directly and re-run `sudo nginx -t && sudo systemctl reload nginx` — Certbot only re-runs its own rewrite when you re-run `certbot --nginx`.

### 3.5 What the nginx config actually routes — `/api`, `/docs`, and everything else

Unlike a setup with a separately built static frontend and a separate backend API, this app is **one FastAPI process** that serves the JSON API, its own interactive docs, *and* the static frontend all from the same place. So there's no path-rewriting to get wrong here (nothing like stripping an `/api/v2` prefix), and no need to split routes into separate `location` blocks either — a single catch-all is correct and sufficient, because FastAPI itself routes `/api/*`, `/docs`, `/redoc`, and `/openapi.json` internally; nginx just needs to forward everything through unchanged:

```nginx
location / {
    proxy_pass         http://127.0.0.1:8005;
    proxy_http_version 1.1;
    proxy_set_header   Host              $host;
    proxy_set_header   X-Real-IP         $remote_addr;
    proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto $scheme;
    proxy_set_header   Upgrade           $http_upgrade;
    proxy_set_header   Connection        "upgrade";
    proxy_read_timeout 300s;   # a full backtest or optimizer run can exceed nginx's default 60s
    proxy_connect_timeout 30s;
}
```

`/api/strategies`, `/docs`, `/dashboard/*`, and `/` all flow through this same block — that's expected, not a gap. See `deploy/nginx-backtestravi.conf` for the full file, including the `:80` redirect server block and the ACME renewal webroot path Certbot needs.

### 3.6 If Certbot / Let's Encrypt isn't an option (GoDaddy manual SSL)

You do not obtain a "root CA certificate" yourself — root CAs are already trusted by every browser and OS. You only need a server certificate issued by a trusted CA, plus its intermediate chain. Prefer Certbot (§3.4); use this only if it's genuinely unavailable.

Generate a private key and CSR on the server:

```bash
sudo openssl req -new -newkey rsa:2048 -nodes \
  -keyout /etc/ssl/private/backtestravi.insightfusionanalytics.com.key \
  -out /tmp/backtestravi.insightfusionanalytics.com.csr
```

Submit `/tmp/backtestravi.insightfusionanalytics.com.csr` to GoDaddy's SSL product and complete domain validation.

Download the issued server certificate and intermediate bundle, place them on the server, e.g.:

```
/etc/ssl/certs/backtestravi.insightfusionanalytics.com.crt
/etc/ssl/certs/gd_bundle-g2-g1.crt
```

Build the full chain file:

```bash
sudo bash -c 'cat /etc/ssl/certs/backtestravi.insightfusionanalytics.com.crt /etc/ssl/certs/gd_bundle-g2-g1.crt > /etc/ssl/certs/backtestravi.insightfusionanalytics.com.fullchain.crt'
```

In the SSL server block, replace the Certbot-managed lines with:

```nginx
ssl_certificate /etc/ssl/certs/backtestravi.insightfusionanalytics.com.fullchain.crt;
ssl_certificate_key /etc/ssl/private/backtestravi.insightfusionanalytics.com.key;
```

Same rule as §3.4 applies: don't enable this block until those exact files exist on disk.

Validate and reload:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

Verify the chain is served correctly, with no trust errors:

```bash
openssl s_client -connect backtestravi.insightfusionanalytics.com:443 -servername backtestravi.insightfusionanalytics.com -showcerts </dev/null
```

## 4) Deploying updates

`deploy.sh` does git pull + dependency install + service restart in one step:

```bash
git pull
./deploy.sh
```

No nginx reload is needed for a normal deploy — nginx isn't serving any app files itself, it only proxies to the running service. You only need `sudo nginx -t && sudo systemctl reload nginx` after changing the **nginx config** itself (the SSL/routing blocks), not after a normal code deploy.

**Rollback:** this repo does not currently keep prior releases — `deploy.sh` restarts the service against whatever's on disk after `git pull`. To roll back:

```bash
git checkout <previous-commit-or-tag>
sudo systemctl restart ravi-vam
git checkout main   # once done
```

## 5) One service, not two

Everything — frontend, API, docs — is the single `ravi-vam` systemd unit. Restart it after any code or dependency change:

```bash
sudo systemctl restart ravi-vam
sudo journalctl -u ravi-vam -f    # tail logs / see startup errors
```

It's bound to `127.0.0.1:8005` only (see `deploy/ravi-vam.service`) — never reachable directly from the internet, only through nginx.

## 6) Troubleshooting

**`nginx: [emerg] cannot load certificate ".../fullchain.pem": No such file or directory`**
The SSL server block is enabled before Certbot has issued the certificate. Fix: install `deploy/nginx-backtestravi-bootstrap.conf` (no SSL directives), confirm `nginx -t` passes and reload, then run `certbot --nginx` — see §3.4.

**502 Bad Gateway**
The app isn't running or isn't listening on `127.0.0.1:8005`. Check:
```bash
sudo systemctl status ravi-vam
sudo journalctl -u ravi-vam -n 50
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8005/
```

**Logs show `DataBento data not found or invalid ... falling back to Yahoo Finance`**
Expected on a fresh install — not an error. The app auto-downloads data via `yfinance` into `./data` on first run. Set `RAVI_DATA_DIR` to point at real DataBento CSVs if/when available (see §2).

**A backtest or optimizer run times out through nginx but works when curled directly against `127.0.0.1:8005`**
Confirm `proxy_read_timeout 300s;` is present in the `location /` block — see §3.5.

**`conflicting server name "backtestravi.insightfusionanalytics.com" ... ignored` in `nginx -t`/reload output**
Two files in `/etc/nginx/sites-available/` (both symlinked into `sites-enabled/`) declare the same `server_name`. nginx picks one per `listen` directive and silently ignores the other — which can mean port 80 and port 443 end up served by *different* files with different (possibly stale) backend ports. Find both:
```bash
sudo nginx -T 2>/dev/null | grep -n "server_name backtestravi"
ls -la /etc/nginx/sites-enabled/ | grep -i backtestravi
```
Pick the correct/complete one (check for a valid `ssl_certificate` and the right `proxy_pass` port), then disable the other by removing its symlink from `sites-enabled/` only — leave the file in `sites-available/` untouched in case you need it back:
```bash
sudo rm /etc/nginx/sites-enabled/<the-duplicate-one>
sudo nginx -t
sudo systemctl reload nginx
```

**`nginx disable` doesn't work**
Not a real nginx command. Manage sites via the symlink in `/etc/nginx/sites-enabled/` — remove the symlink to disable, then `sudo nginx -t && sudo systemctl reload nginx`.

## 7) Helpful checks

```bash
# service + nginx status
sudo systemctl status ravi-vam
sudo systemctl status nginx
sudo nginx -t

# what's listening
sudo ss -tulpn | grep -E ':80|:443|:8005'

# logs
sudo journalctl -u ravi-vam -f
sudo tail -f /var/log/nginx/error.log
sudo tail -f /var/log/nginx/access.log

# certificate status + expiry
sudo certbot certificates
```

## 8) Security basics (recommended, not currently enabled)

```bash
sudo apt install -y ufw
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
```

Keep Ubuntu packages updated regularly. The app itself only listens on `127.0.0.1`, so it's never exposed directly regardless of firewall state — nginx is the only public entry point.
