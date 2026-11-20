"""End-to-end OTLP check + throughput overhead of the OpenTelemetry export.

Starts a tiny HTTP server that plays the OTLP receiver (it only counts POSTs to /v1/traces and
/v1/metrics), runs the app in-process with and without telemetry, and reports lines/s.

    python scripts/otel_overhead.py
"""
import os
import statistics
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

received = {"/v1/traces": 0, "/v1/metrics": 0, "bytes": 0}


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):                                  # noqa: N802
        n = int(self.headers.get("Content-Length", 0))
        self.rfile.read(n)
        received[self.path] = received.get(self.path, 0) + 1
        received["bytes"] += n
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):
        pass


def run(enabled: bool, lines: list[str], batch: int = 500) -> float:
    from fastapi.testclient import TestClient

    from logstream import otel
    from logstream.api import create_app
    from logstream.pool import DetectorPool

    tel = otel.Telemetry() if enabled else otel.NoTelemetry()
    client = TestClient(create_app(DetectorPool(), telemetry=tel))
    t0 = time.perf_counter()
    for i in range(0, len(lines), batch):
        client.post("/ingest", json={"lines": lines[i:i + batch], "source": "bgl", "timestamp": i})
    dt = time.perf_counter() - t0
    tel.shutdown()                                       # flush pending spans/metrics
    return len(lines) / dt


def main():
    srv = HTTPServer(("127.0.0.1", 0), Receiver)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"http://127.0.0.1:{srv.server_port}"
    os.environ["OTEL_METRIC_EXPORT_INTERVAL"] = "1000"

    sample = Path(__file__).resolve().parents[1] / "data" / "BGL_2k.log"
    lines = sample.read_text(errors="ignore").splitlines() if sample.exists() else \
        [f"user{i % 50} logged in from 10.0.0.{i % 255}" for i in range(2000)]
    lines = (lines * (20_000 // len(lines) + 1))[:20_000]

    off, on = [], []
    for _ in range(3):
        off.append(run(False, lines))
        on.append(run(True, lines))
    print(f"lines/s through HTTP API, OTel off: {statistics.median(off):,.0f}")
    print(f"lines/s through HTTP API, OTel on : {statistics.median(on):,.0f} "
          f"({100 * (1 - statistics.median(on) / statistics.median(off)):.1f}% slower)")
    print(f"OTLP requests received: traces={received['/v1/traces']} metrics={received['/v1/metrics']} "
          f"({received['bytes'] / 1024:.0f} KiB)")
    srv.shutdown()


if __name__ == "__main__":
    main()
