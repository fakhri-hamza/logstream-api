"""OpenTelemetry export (traces + metrics), off by default.

Turn it on with the standard OTel environment variables, for example:

    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318          # an OTel Collector, Jaeger, Grafana Alloy...
    OTEL_SERVICE_NAME=logstream

or straight to Dynatrace (OTLP over HTTP, API token with the openTelemetryTrace.ingest
and metrics.ingest scopes; Dynatrace wants delta temporality for metrics):

    OTEL_EXPORTER_OTLP_ENDPOINT=https://<env-id>.live.dynatrace.com/api/v2/otlp
    OTEL_EXPORTER_OTLP_HEADERS=Authorization=Api-Token%20<token>
    OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta

What is exported
  * one span per POST /ingest  (attributes: source, lines, alerts), with one span event per alert
  * logstream.lines.ingested   counter   {source}
  * logstream.alerts           counter   {source, kind}
  * logstream.ingest.duration  histogram (seconds) {source}

The module never imports OpenTelemetry unless it is enabled, so the service runs without it.
"""
from __future__ import annotations

import os
from contextlib import contextmanager


def enabled_from_env() -> bool:
    if os.getenv("LOGSTREAM_OTEL", "").lower() in {"0", "false", "off"}:
        return False
    return bool(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
                or os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
                or os.getenv("LOGSTREAM_OTEL", "").lower() in {"1", "true", "on"})


class NoTelemetry:
    """Used when OTel is off: every call is a no-op."""
    enabled = False

    @contextmanager
    def ingest_span(self, source: str, n_lines: int):
        yield _NoSpan()

    def record(self, source: str, n_lines: int, alerts: list[dict], seconds: float) -> None:
        pass

    def shutdown(self) -> None:
        pass


class _NoSpan:
    def set_attribute(self, *_a, **_k):
        pass

    def add_event(self, *_a, **_k):
        pass


class Telemetry:
    """Thin wrapper over the OTel API. Providers can be injected (tests use in-memory ones)."""
    enabled = True

    def __init__(self, tracer_provider=None, meter_provider=None, service_name: str | None = None):
        from opentelemetry import metrics, trace
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create({"service.name": service_name or os.getenv("OTEL_SERVICE_NAME", "logstream"),
                                    "service.version": "0.4.0"})
        if tracer_provider is None:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            tracer_provider = TracerProvider(resource=resource)
            tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        if meter_provider is None:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            meter_provider = MeterProvider(resource=resource,
                                           metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())])
        self._tp, self._mp = tracer_provider, meter_provider
        self.tracer = tracer_provider.get_tracer("logstream")
        meter = meter_provider.get_meter("logstream")
        self.lines = meter.create_counter("logstream.lines.ingested", unit="{line}",
                                          description="log lines ingested")
        self.alerts = meter.create_counter("logstream.alerts", unit="{alert}",
                                           description="alerts raised, by kind")
        self.duration = meter.create_histogram("logstream.ingest.duration", unit="s",
                                               description="time spent processing one batch")
        self._trace = trace

    @contextmanager
    def ingest_span(self, source: str, n_lines: int):
        with self.tracer.start_as_current_span("logstream.ingest") as span:
            span.set_attribute("logstream.source", source)
            span.set_attribute("logstream.lines", n_lines)
            yield span

    def record(self, source: str, n_lines: int, alerts: list[dict], seconds: float) -> None:
        span = self._trace.get_current_span()
        span.set_attribute("logstream.alerts", len(alerts))
        for a in alerts:
            span.add_event("logstream.alert", {"kind": a.get("kind", ""), "template": str(a.get("template", ""))[:200],
                                               "detail": str(a.get("detail", ""))[:200]})
            self.alerts.add(1, {"source": source, "kind": a.get("kind", "")})
        self.lines.add(n_lines, {"source": source})
        self.duration.record(seconds, {"source": source})

    def shutdown(self) -> None:
        for p in (self._tp, self._mp):
            try:
                p.shutdown()
            except Exception:                       # noqa: BLE001 - never crash on exit
                pass


def from_env():
    return Telemetry() if enabled_from_env() else NoTelemetry()
