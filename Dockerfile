# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install build deps for gevent
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY . /app

# Expose Flask port
EXPOSE 5000

# Use gevent.pywsgi with WebSocket support
CMD ["python", "-c", "from gevent import pywsgi; from geventwebsocket.handler import WebSocketHandler; from server import app; server = pywsgi.WSGIServer(('0.0.0.0', 5000), app, handler_class=WebSocketHandler); server.serve_forever()"]
