"""HTTP API around the online detectors (one per log source).

Endpoints
  POST /ingest            {"lines": [...], "source": "web-1"}  -> alerts raised by this batch
  GET  /alerts?limit=&kind=&source=   most recent alerts (newest first)
  GET  /templates?top=&source=        most frequent templates of one source
  GET  /sources           sources seen so far, with their counters
  GET  /stats             totals + per-source counters for the dashboard
  GET  /metrics           Prometheus text format
  POST /snapshot          save the state now (if persistence is on)
  GET  /health            liveness probe
  GET  /                  live dashboard (static HTML)

Persistence (optional): set LOGSTREAM_STATE=/path/state.json. The state is loaded at startup,
saved at most every LOGSTREAM_SAVE_EVERY seconds while ingesting (default 30), and on shutdown.

Run:  uvicorn logstream.api:app --reload
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel, Field

from logstream import otel
from logstream.detector import StreamDetector
from logstream.pool import DEFAULT_SOURCE, DetectorPool, TooManySources

STATIC = Path(__file__).parent / "static"
log = logging.getLogger("logstream")


class Batch(BaseModel):
    lines: list[str] = Field(..., max_length=10_000, description="raw log lines")
    timestamp: float | None = Field(None, description="event time (unix s); default: now")
    source: str = Field(DEFAULT_SOURCE, pattern=r"^[A-Za-z0-9_.:-]{1,64}$",
                        description="which system the lines come from; one detector per source")


def _settings() -> dict:
    return {"warmup_lines": int(os.getenv("LOGSTREAM_WARMUP", "500")),
            "spike_k": float(os.getenv("LOGSTREAM_SPIKE_K", "4")),
            "max_sources": int(os.getenv("LOGSTREAM_MAX_SOURCES", "100"))}


def _load_or_fresh(state_path: Path | None) -> tuple[DetectorPool, bool]:
    """Restore from the state file if there is one. A corrupt file is set aside, not fatal."""
    if state_path is None or not state_path.exists():
        return DetectorPool(**_settings()), False
    try:
        pool = DetectorPool.load(state_path, **_settings())
        log.info("restored state: %s sources", len(pool.detectors))
        return pool, True
    except Exception as exc:                                    # noqa: BLE001
        bad = state_path.with_suffix(state_path.suffix + ".corrupt")
        os.replace(state_path, bad)
        log.warning("could not load %s (%s); moved it to %s and started fresh", state_path, exc, bad)
        return DetectorPool(**_settings()), False


def create_app(detector: StreamDetector | DetectorPool | None = None,
               state_path: str | Path | None = None,
               save_every: float | None = None,
               telemetry=None) -> FastAPI:
    tel = telemetry if telemetry is not None else otel.from_env()
    if state_path is None and os.getenv("LOGSTREAM_STATE"):
        state_path = os.environ["LOGSTREAM_STATE"]
    state_path = Path(state_path) if state_path else None
    if save_every is None:
        save_every = float(os.getenv("LOGSTREAM_SAVE_EVERY", "30"))

    if isinstance(detector, DetectorPool):
        pool, restored = detector, False
    elif isinstance(detector, StreamDetector):          # single detector = the default source
        pool, restored = DetectorPool(), False
        pool.detectors[DEFAULT_SOURCE] = detector
    else:
        pool, restored = _load_or_fresh(state_path)
    lock = Lock()                                   # Drain is not thread-safe
    last_save = [time.monotonic()]

    def save() -> bool:                             # caller holds the lock
        if state_path is None:
            return False
        pool.save(state_path)
        pool.saves += 1
        pool.last_save_unix = time.time()
        last_save[0] = time.monotonic()
        return True

    @asynccontextmanager
    async def lifespan(_app):
        yield
        with lock:
            if save():
                log.info("state saved to %s on shutdown", state_path)
        tel.shutdown()

    app = FastAPI(title="logstream", version="0.4.0", lifespan=lifespan,
                  description="Real-time log anomaly detection (Drain + severity, new-template "
                              "and rate-spike alerts), one detector per source, Prometheus metrics, "
                              "optional OpenTelemetry export, optional state persistence")

    @app.post("/ingest")
    def ingest(batch: Batch):
        now = batch.timestamp if batch.timestamp is not None else time.time()
        with tel.ingest_span(batch.source, len(batch.lines)), lock:
            t0 = time.perf_counter()
            try:
                alerts = [a.to_dict() for a in pool.process(batch.source, batch.lines, now)]
            except TooManySources as exc:
                pool.rejected["too_many_sources"] += 1
                raise HTTPException(429, str(exc)) from exc
            elapsed = time.perf_counter() - t0
            pool.batches += 1
            pool.ingest_seconds += elapsed
            tel.record(batch.source, len(batch.lines), alerts, elapsed)
            if state_path is not None and time.monotonic() - last_save[0] >= save_every:
                save()
        return {"accepted": len(batch.lines), "source": batch.source, "alerts": alerts}

    @app.get("/alerts")
    def alerts(limit: int = Query(50, ge=1, le=500), kind: str | None = None,
               source: str | None = None):
        with lock:
            return [a.to_dict() for a in pool.recent_alerts(source, kind)[:limit]]

    @app.get("/templates")
    def templates(top: int = Query(20, ge=1, le=200), source: str = DEFAULT_SOURCE):
        with lock:
            det = pool.get(source, create=False)
            if det is None:
                raise HTTPException(404, f"unknown source {source!r}")
            return det.templates(top)

    @app.get("/sources")
    def sources():
        with lock:
            return pool.stats()["sources"]

    @app.get("/stats")
    def stats():
        with lock:
            return {**pool.stats(), "persistence": state_path is not None,
                    "restored_from_state": restored}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        with lock:
            body = pool.prometheus()
        return PlainTextResponse(body, media_type="text/plain; version=0.0.4")

    @app.post("/snapshot")
    def snapshot():
        if state_path is None:
            raise HTTPException(409, "persistence is off (set LOGSTREAM_STATE)")
        with lock:
            save()
        return {"saved_to": str(state_path), "sources": len(pool.detectors)}

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(STATIC / "index.html")

    return app


app = create_app()
