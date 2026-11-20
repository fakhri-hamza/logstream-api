"""OpenTelemetry export: spans and metrics are produced (captured in memory, no network)."""
from fastapi.testclient import TestClient
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from logstream import otel
from logstream.api import create_app
from logstream.pool import DetectorPool


def make_client():
    spans = InMemorySpanExporter()
    tp = TracerProvider()
    tp.add_span_processor(SimpleSpanProcessor(spans))
    reader = InMemoryMetricReader()
    mp = MeterProvider(metric_readers=[reader])
    tel = otel.Telemetry(tracer_provider=tp, meter_provider=mp)
    client = TestClient(create_app(DetectorPool(warmup_lines=1), telemetry=tel))
    return client, spans, reader


def metric_points(reader):
    out = {}
    data = reader.get_metrics_data()
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                out[m.name] = list(m.data.data_points)
    return out


def test_ingest_creates_span_with_alert_events():
    client, spans, _ = make_client()
    r = client.post("/ingest", json={"lines": ["KERNEL FATAL data TLB error interrupt"],
                                     "source": "node-7", "timestamp": 0})
    assert r.status_code == 200 and r.json()["alerts"]
    (span,) = [s for s in spans.get_finished_spans() if s.name == "logstream.ingest"]
    assert span.attributes["logstream.source"] == "node-7"
    assert span.attributes["logstream.lines"] == 1
    assert span.attributes["logstream.alerts"] == len(r.json()["alerts"])
    kinds = {e.attributes["kind"] for e in span.events if e.name == "logstream.alert"}
    assert "SEVERITY" in kinds


def test_metrics_count_lines_and_alerts_per_source():
    client, _, reader = make_client()
    client.post("/ingest", json={"lines": ["user alice logged in"] * 3, "source": "web", "timestamp": 0})
    client.post("/ingest", json={"lines": ["KERNEL FATAL data TLB error interrupt"], "source": "hpc", "timestamp": 0})
    pts = metric_points(reader)
    lines = {dict(p.attributes)["source"]: p.value for p in pts["logstream.lines.ingested"]}
    assert lines == {"web": 3, "hpc": 1}
    alerts = {(dict(p.attributes)["source"], dict(p.attributes)["kind"]): p.value for p in pts["logstream.alerts"]}
    assert alerts.get(("hpc", "SEVERITY"), 0) >= 1
    assert sum(p.count for p in pts["logstream.ingest.duration"]) == 2


def test_disabled_by_default(monkeypatch):
    for k in ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "LOGSTREAM_OTEL"]:
        monkeypatch.delenv(k, raising=False)
    assert isinstance(otel.from_env(), otel.NoTelemetry)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    assert otel.enabled_from_env()
    monkeypatch.setenv("LOGSTREAM_OTEL", "off")
    assert not otel.enabled_from_env()
