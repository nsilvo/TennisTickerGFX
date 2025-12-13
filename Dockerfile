# syntax=docker/dockerfile:1
# File: Dockerfile
# Description: Builds the Flask + Socket.IO app image for tennis XML scraper.
# Author: Nathan Silveston
# Contact: nathan@nkpa.co.uk | +44 7515 018048
# Copyright (c) 2025 Nathan Silveston. All rights reserved.

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

# Run server.py which starts the scraper thread and SocketIO server
CMD ["python", "server.py"]
