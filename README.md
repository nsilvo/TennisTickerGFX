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

The compose file includes a `postgres` service. The app receives `DATABASE_URL=postgresql://tennisxml:tennisxml@postgres:5432/tennisxml`.

Start both services:

```bash
docker compose up --build -d
```

Persisted data lives in the `postgres-data` volume. To inspect:

```bash
docker compose exec postgres psql -U tennisxml -d tennisxml
```

## Runtime

- Server entry: `server.py` exporting `app`
- WebSocket: Flask-SocketIO via `gevent` worker in Gunicorn
- Templates: `templates/`

## Environment variables

- `FLASK_ENV`: `production` by default in compose
- `TZ`: timezone (defaults to UTC)

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
