"""One detector per log source, plus the counters exported on /metrics.

Why per source: each system has its own vocabulary and its own normal rates. With one shared
detector, a template learned from service A hides a genuinely new message in service B, one
noisy source's warm-up delays alerts for all the others, and alerts don't say where they came
from. A pool keeps every source isolated and creates its detector on first use.

State file format v2: {"version": 2, "sources": {name: <detector state>}}.
v1 files (a single detector, written by logstream 0.2) load as the source "default".
"""
import json
import os
import re
from collections import Counter
from pathlib import Path

from logstream.detector import Alert, StreamDetector

POOL_STATE_VERSION = 2
SOURCE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
DEFAULT_SOURCE = "default"


class TooManySources(Exception):
    pass


class DetectorPool:
    def __init__(self, max_sources: int = 100, **detector_kwargs):
        self.max_sources = max_sources
        self.detector_kwargs = detector_kwargs
        self.detectors: dict[str, StreamDetector] = {}
        # operational counters for /metrics (not persisted: Prometheus handles counter resets)
        self.batches = 0
        self.ingest_seconds = 0.0
        self.saves = 0
        self.last_save_unix = 0.0
        self.rejected = Counter()

    def get(self, source: str, create: bool = True) -> StreamDetector | None:
        det = self.detectors.get(source)
        if det is None and create:
            if len(self.detectors) >= self.max_sources:
                raise TooManySources(f"limit of {self.max_sources} sources reached")
            det = self.detectors[source] = StreamDetector(**self.detector_kwargs)
        return det

    def process(self, source: str, lines: list[str], now: float) -> list[Alert]:
        det = self.get(source)
        out = []
        for line in lines:
            for a in det.process(line, now):
                a.source = source
                out.append(a)
        return out

    def recent_alerts(self, source: str | None = None, kind: str | None = None) -> list[Alert]:
        items = [a for name, det in self.detectors.items() if source in (None, name)
                 for a in det.alerts if kind in (None, a.kind)]
        return sorted(items, key=lambda a: a.time, reverse=True)

    def stats(self) -> dict:
        per = {name: det.stats() for name, det in sorted(self.detectors.items())}
        levels, kinds = Counter(), Counter()
        for s in per.values():
            levels.update(s["levels"])
            kinds.update(s["alerts_by_kind"])
        return {"lines_seen": sum(s["lines_seen"] for s in per.values()),
                "templates": sum(s["templates"] for s in per.values()),
                "levels": dict(levels), "alerts_by_kind": dict(kinds),
                "warming_up": any(s["warming_up"] for s in per.values()),
                "sources": per}

    # ------------------------------------------------------------------ persistence
    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        state = {"version": POOL_STATE_VERSION,
                 "sources": {name: det.to_state() for name, det in self.detectors.items()}}
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        os.replace(tmp, path)

    def restore(self, state: dict) -> None:
        if state.get("version") == 1:                        # logstream 0.2: one detector
            state = {"version": POOL_STATE_VERSION, "sources": {DEFAULT_SOURCE: state}}
        if state.get("version") != POOL_STATE_VERSION:
            raise ValueError(f"unsupported state version {state.get('version')!r}")
        self.detectors = {}
        for name, s in state["sources"].items():
            det = StreamDetector(**self.detector_kwargs)
            det.restore(s)
            for a in det.alerts:
                a.source = name
            self.detectors[name] = det

    @classmethod
    def load(cls, path: str | Path, **kwargs) -> "DetectorPool":
        pool = cls(**kwargs)
        pool.restore(json.loads(Path(path).read_text(encoding="utf-8")))
        return pool

    # ------------------------------------------------------------------ metrics
    def prometheus(self) -> str:
        """Prometheus text exposition format (v0.0.4), written by hand: no extra dependency."""
        def esc(v: str) -> str:
            return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

        out = []

        def metric(name, kind, help_, samples):
            out.append(f"# HELP {name} {help_}")
            out.append(f"# TYPE {name} {kind}")
            for labels, value in samples:
                lab = ",".join(f'{k}="{esc(str(v))}"' for k, v in labels.items())
                out.append(f"{name}{{{lab}}} {value}" if lab else f"{name} {value}")

        dets = sorted(self.detectors.items())
        metric("logstream_lines_total", "counter", "Log lines processed.",
               [({"source": n}, d.lines_seen) for n, d in dets])
        metric("logstream_alerts_total", "counter", "Alerts raised.",
               [({"source": n, "kind": k}, c) for n, d in dets for k, c in sorted(d.alert_totals.items())])
        metric("logstream_templates", "gauge", "Distinct log templates learned.",
               [({"source": n}, len(d.miner.drain.clusters)) for n, d in dets])
        metric("logstream_warming_up", "gauge", "1 while a source is still in its warm-up period.",
               [({"source": n}, int(d.lines_seen <= d.warmup_lines)) for n, d in dets])
        metric("logstream_sources", "gauge", "Sources with a detector.", [({}, len(dets))])
        metric("logstream_ingest_batches_total", "counter", "Ingest requests handled.", [({}, self.batches)])
        metric("logstream_ingest_seconds_total", "counter", "Time spent in detection.",
               [({}, round(self.ingest_seconds, 6))])
        metric("logstream_rejected_batches_total", "counter", "Ingest requests rejected.",
               [({"reason": r}, c) for r, c in sorted(self.rejected.items())])
        metric("logstream_state_saves_total", "counter", "State snapshots written.", [({}, self.saves)])
        metric("logstream_last_save_timestamp_seconds", "gauge", "Unix time of the last snapshot.",
               [({}, round(self.last_save_unix, 3))])
        return "\n".join(out) + "\n"
