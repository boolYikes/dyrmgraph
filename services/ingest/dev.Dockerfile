FROM python:3.12-slim

WORKDIR /app

RUN mkdir ingest

COPY services/ingest ./ingest

RUN pip install -r ingest/requirements.txt --no-cache-dir

ENV PYTHONPATH=/app

