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

## Notes

- If you need RTMP input, convert it to HLS (.m3u8) with a media server (nginx-rtmp) or FFmpeg, then embed the HLS URL in the template.
- For production behind a reverse proxy, enable `proxy_set_header Upgrade` and `Connection` headers to support WebSocket upgrades.
