# TennisXML Deployment

This project runs a Flask + Flask-SocketIO server with templates under `templates/`.

## Docker (recommended)

Build and run with Docker:

```bash
# From project root
docker build -t tennisxml .
docker run --name tennisxml -p 5000:5000 --restart unless-stopped tennisxml
```

Using docker-compose for convenience:

```bash
docker compose up --build -d
```

The app listens on port 5000. Visit `http://localhost:5000`.

### With PostgreSQL (production)

The compose file includes an optional `postgres` service (profile: `local-db`).

- If you use external Postgres (RDS/Aurora), set `DATABASE_URL` in your shell/env and run only `app`.
- If you want local Postgres in compose, enable profile `local-db`.

Start app with local Postgres:

```bash
docker compose --profile local-db up --build -d
```

Persisted data lives in the `postgres-data` volume. To inspect:

```bash
docker compose exec postgres psql -U tennisxml -d tennisxml
```

Start app with external Postgres (or SQLite fallback):

```bash
# Example with external DB
export DATABASE_URL=postgresql://user:pass@host:5432/db
docker compose up --build -d app
```

## Runtime

- Server entry: `server.py` exporting `app`
- WebSocket: Flask-SocketIO via `gevent` worker in Gunicorn
- Templates: `templates/`

## Environment variables

- `FLASK_ENV`: `production` by default in compose
- `TZ`: timezone (defaults to UTC)
- `PORT`: HTTP port (defaults to `5000`)
- `TOURNAMENT_ID`: startup tournament id (defaults to `7140`)
- `SCRAPE_INTERVAL`: startup scrape interval in seconds (defaults to `5`)
- `ENABLE_SCRAPER`: start background XML polling loop (`true` by default)
- `SQLITE_DB_PATH`: sqlite filename/path when not using `DATABASE_URL`

## AWS App Runner

This repo is prepared for App Runner in two common modes:

- Source-based deploy via `apprunner.yaml`
- Container deploy via `Dockerfile`

### Source-based deploy

1. In AWS App Runner, create service from source repository.
2. Runtime config file: use `apprunner.yaml` from project root.
3. Set environment variables in App Runner:
	- `PORT=5000`
	- `TZ=UTC`
	- `DATABASE_URL=postgresql://...` (recommended for production)
	- Optional: `TOURNAMENT_ID`, `SCRAPE_INTERVAL`, `ENABLE_SCRAPER`
4. Health check path: `/health`

### Container deploy

1. Build and push this image to ECR.
2. Create App Runner service from ECR image.
3. Container port: `5000` (or set `PORT` and match App Runner port setting).
4. Health check path: `/health`

### Scaling note

The app runs an internal scraper thread per instance. If App Runner scales to multiple instances,
each instance will poll and process XML.

For predictable behavior, one of these is recommended:

- Keep App Runner min/max size at `1` for this service, or
- Set `ENABLE_SCRAPER=false` on read-only web instances and run scraper in a dedicated single worker/service.

## Development (without Docker)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python server.py
```

### Run modes

- Dev (SQLite): do not set `DATABASE_URL`. The app uses `casparcg_match_cache.db`.
- Prod (Postgres): set `DATABASE_URL` (provided by compose) and the app connects to Postgres.

### Migrate data from SQLite to Postgres

Export or run the provided helper script:

```bash
# Ensure DATABASE_URL is set to your Postgres
export DATABASE_URL=postgresql://tennisxml:tennisxml@localhost:5432/tennisxml
python scripts/migrate_sqlite_to_postgres.py casparcg_match_cache.db
```

The script copies rows from `matches` to Postgres, creating tables if missing.

## Notes

- If you need RTMP input, convert it to HLS (.m3u8) with a media server (nginx-rtmp) or FFmpeg, then embed the HLS URL in the template.
- For production behind a reverse proxy, enable `proxy_set_header Upgrade` and `Connection` headers to support WebSocket upgrades.
 - For database, SQLite is fine for dev; PostgreSQL is provided via compose for production.

## AWS EC2 update flow (low-memory friendly)

If `docker compose up -d --build` causes your EC2 host to become unstable, it is usually memory pressure from on-host builds and/or running local Postgres on small instances.

Recommended update flow:

```bash
git pull

# Restart app without rebuilding (works for most code/template changes)
docker compose up -d --no-build --force-recreate app
```

Only rebuild when `Dockerfile` or `requirements.txt` changed:

```bash
docker compose build app
docker compose up -d app
```

If you need local Postgres too:

```bash
docker compose --profile local-db up -d app postgres
```

### One-command EC2 deploy (with local Postgres)

Use the helper script:

```bash
./scripts/deploy_ec2.sh --pull
```

Behavior:

- Always deploys `app` + local `postgres` (`local-db` profile)
- Rebuilds image only when `Dockerfile` or `requirements.txt` changed
- Supports manual full rebuild with:

```bash
./scripts/deploy_ec2.sh --rebuild
```
