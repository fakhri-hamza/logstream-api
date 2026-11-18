"""Per-source detectors and the Prometheus endpoint."""
import json
import re

from fastapi.testclient import TestClient

from logstream.api import create_app
from logstream.detector import StreamDetector
from logstream.pool import DetectorPool

WARM = [f"user {i} logged in from host {i}" for i in range(10)]


def client(**kw):
    return TestClient(create_app(DetectorPool(warmup_lines=5, **kw)))


def test_sources_are_isolated():
    c = client()
    c.post("/ingest", json={"lines": WARM + ["disk controller reset on bus 3"], "source": "storage", "timestamp": 0})
    # the same message is new for a different system: a shared detector would have hidden it
    r = c.post("/ingest", json={"lines": WARM + ["disk controller reset on bus 9"], "source": "web", "timestamp": 0})
    new = [a for a in r.json()["alerts"] if a["kind"] == "NEW_TEMPLATE"]
    assert len(new) == 1 and new[0]["source"] == "web"
    assert set(c.get("/sources").json()) == {"storage", "web"}
    assert c.get("/stats").json()["lines_seen"] == 22


def test_alert_filters_by_source_and_templates_404():
    c = client()
    c.post("/ingest", json={"lines": ["kernel FATAL panic"], "source": "a", "timestamp": 0})
    c.post("/ingest", json={"lines": ["disk FATAL failure"], "source": "b", "timestamp": 0})
    assert {a["source"] for a in c.get("/alerts").json()} == {"a", "b"}
    assert [a["source"] for a in c.get("/alerts?source=b").json()] == ["b"]
    assert c.get("/templates?source=nope").status_code == 404


def test_bad_source_name_and_source_limit():
    c = client(max_sources=2)
    assert c.post("/ingest", json={"lines": ["x"], "source": "bad name!"}).status_code == 422
    for s in ("a", "b"):
        assert c.post("/ingest", json={"lines": ["x"], "source": s}).status_code == 200
    assert c.post("/ingest", json={"lines": ["x"], "source": "c"}).status_code == 429
    assert 'logstream_rejected_batches_total{reason="too_many_sources"} 1' in c.get("/metrics").text


def test_metrics_are_valid_prometheus_text():
    c = client()
    c.post("/ingest", json={"lines": ["kernel FATAL panic"] * 3, "source": "node-1", "timestamp": 0})
    r = c.get("/metrics")
    assert r.headers["content-type"].startswith("text/plain")
    sample = re.compile(r'^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[a-zA-Z_][a-zA-Z0-9_]*="[^"]*"(,[a-zA-Z_][a-zA-Z0-9_]*="[^"]*")*\})? -?[0-9.e+]+$')
    for line in r.text.strip().splitlines():
        assert line.startswith("# HELP ") or line.startswith("# TYPE ") or sample.match(line), line
    assert 'logstream_lines_total{source="node-1"} 3' in r.text
    assert 'logstream_alerts_total{source="node-1",kind="SEVERITY"} 1' in r.text
    assert "logstream_ingest_batches_total 1" in r.text


def test_v1_state_file_loads_as_default_source(tmp_path):
    det = StreamDetector(warmup_lines=5)
    for line in WARM:
        det.process(line, 0)
    path = tmp_path / "state.json"
    det.save(path)                                       # logstream 0.2 format: one detector
    assert json.loads(path.read_text())["version"] == 1
    with TestClient(create_app(state_path=path)) as c:
        stats = c.get("/stats").json()
        assert stats["restored_from_state"] is True
        assert stats["sources"]["default"]["lines_seen"] == len(WARM)
    assert json.loads(path.read_text())["version"] == 2  # rewritten in the new format on shutdown
