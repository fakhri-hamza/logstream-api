FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pyproject.toml .
COPY src ./src
RUN pip install --no-cache-dir --no-deps .

# run as an unprivileged user; /data holds the detector state (mount a volume there)
RUN useradd --create-home appuser && mkdir /data && chown appuser /data
USER appuser
VOLUME /data

ENV LOGSTREAM_WARMUP=500 LOGSTREAM_SPIKE_K=4 \
    LOGSTREAM_STATE=/data/state.json LOGSTREAM_SAVE_EVERY=30 \
    LOGSTREAM_MAX_SOURCES=100
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"
CMD ["uvicorn", "logstream.api:app", "--host", "0.0.0.0", "--port", "8000"]
