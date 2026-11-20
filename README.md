# logstream

[![ci](https://github.com/fakhri-hamza/logstream-api/actions/workflows/ci.yml/badge.svg)](https://github.com/fakhri-hamza/logstream-api/actions/workflows/ci.yml) ![Python 3.12](https://img.shields.io/badge/python-3.12-blue) [![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**A small real-time log anomaly detection service.** Send raw log lines over HTTP and get alerts back immediately, with a live dashboard. It turns the findings of my offline study [log-anomaly-bench](https://github.com/fakhri-hamza/log-anomaly-bench) into something you could actually deploy.

![Dashboard](docs/dashboard.jpg)
*Replaying the BlueGene/L supercomputer log into the service. Alerts arrive as lines stream in.*

## What it detects
| Alert | Rule | Why (from the offline study) |
|---|---|---|
| `SEVERITY` | line carries `FATAL` / `CRITICAL` / `FAILURE`; at most one alert per template per minute | on BGL this single signal reached F1 0.80, better than any ML model I tested |
| `NEW_TEMPLATE` | Drain creates a template never seen before (after a warm-up) | 67% of anomalies in the test period came from templates unseen in training (concept drift) |
| `RATE_SPIKE` | a template's count this minute > mean + 4σ of its past minutes (min. 20) | catches floods of "normal-looking" messages |

Templates are mined online with [Drain3](https://github.com/logpai/Drain3). Numbers and hex values are masked, so `generating core.2275` and `generating core.862` become one template.

## API
| Method | Path | |
|---|---|---|
| `POST` | `/ingest` | `{"lines": ["raw line", ...], "source": "web-1", "timestamp": 1117838570}` → alerts raised by this batch (`source` optional, default `default`) |
| `GET` | `/alerts?limit=50&kind=SEVERITY&source=web-1` | recent alerts, newest first, each tagged with its source |
| `GET` | `/templates?top=20&source=web-1` | most frequent templates of one source |
| `GET` | `/sources` | every source seen, with its counters |
| `GET` | `/stats` | totals + per-source counters for the dashboard |
| `GET` | `/metrics` | Prometheus metrics (text format) |
| `POST` | `/snapshot` | save the detector state now (when persistence is on) |
| `GET` | `/health` | liveness probe |
| `GET` | `/` | live dashboard (vanilla JS, polls every second) |

Interactive docs are at `/docs` (FastAPI / OpenAPI).

```bash
curl -X POST localhost:8000/ingest -H "Content-Type: application/json" \
     -d '{"lines": ["KERNEL FATAL data TLB error interrupt"]}'
```

## Run it
```bash
# local
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt && pip install -e .
uvicorn logstream.api:app --port 8000
python scripts/replay.py --speed 10        # in a 2nd terminal: stream a real log in
                                           # then open http://localhost:8000

# docker (state kept in a named volume, survives restarts and upgrades)
docker build -t logstream .
docker run -p 8000:8000 -v logstream-data:/data logstream
```
Configuration via environment variables:

| Variable | Default | |
|---|---|---|
| `LOGSTREAM_WARMUP` | 500 | lines before new-template alerts start |
| `LOGSTREAM_SPIKE_K` | 4 | σ multiplier for rate spikes |
| `LOGSTREAM_STATE` | unset locally, `/data/state.json` in Docker | where to persist state; unset = in-memory only |
| `LOGSTREAM_SAVE_EVERY` | 30 | max seconds between automatic saves while ingesting |
| `LOGSTREAM_MAX_SOURCES` | 100 | cap on distinct sources; more → HTTP 429 (protects memory from a misbehaving client) |

## Sources and metrics (v0.3)
**One detector per source.** Each batch can name its `source` (`web-1`, `hdfs`, `bgl-node`…). Every source gets its own template tree, rate statistics, warm-up and alert buffer; detectors are created on first use, up to `LOGSTREAM_MAX_SOURCES`. Alerts carry their source, so a page says *which* system broke.

I measured whether isolation also changes *detection* (`scripts/shared_vs_per_source.py`): 4 real Loghub logs (BGL, HDFS, Zookeeper, Hadoop; 8,000 lines interleaved as one shipper would forward them) were sent to one shared detector and to per-source detectors. **Result: no difference.** 200 templates either way, 0 templates mixing lines from two systems, 0 missed "new message type" alerts. Drain already separates systems whose lines look different. So the benefit is operational (attribution, independent warm-up, per-source limits and metrics), not better detection. Systems with similar message formats could still collide in a shared detector; that case isn't measured here.

**Prometheus `/metrics`**, written by hand in the text exposition format (no client library):

| Metric | Type | Labels |
|---|---|---|
| `logstream_lines_total` | counter | `source` |
| `logstream_alerts_total` | counter | `source`, `kind` |
| `logstream_templates` | gauge | `source` |
| `logstream_warming_up` | gauge | `source` |
| `logstream_sources` | gauge | |
| `logstream_ingest_batches_total`, `logstream_ingest_seconds_total` | counter | |
| `logstream_rejected_batches_total` | counter | `reason` |
| `logstream_state_saves_total`, `logstream_last_save_timestamp_seconds` | counter / gauge | |

Example alert rule: `increase(logstream_alerts_total{kind="NEW_TEMPLATE"}[10m]) > 5`, meaning several never-seen message types appeared within 10 minutes.

**State files:** v0.3 writes format v2 (all sources). A v0.2 file (one detector) is loaded as the source `default` and rewritten in the new format at the next save. This is tested.

## Persistence
Without it, a restart forgets every learned template: message types the service already knew get reported as "new" again, and rate statistics start from zero.

![Restart effect](docs/restart_effect.png)
*`scripts/restart_effect.py`: 200k BGL lines, restart at the halfway point. The in-memory detector raises 80 `NEW_TEMPLATE` alerts on the second half; the restored one raises 40, only for templates it had never seen.*

- **What is saved:** the Drain template tree, per-template rate statistics (Welford mean/variance), counters and recent alerts. The per-minute dedup sets are not saved, since they only matter within the current minute.
- **When:** at startup the state is restored; while ingesting it is saved at most every `LOGSTREAM_SAVE_EVERY` seconds; on graceful shutdown (SIGTERM, `docker stop`) it is saved once more. `POST /snapshot` forces a save.
- **Safety:** writes are atomic (temp file + rename), so a crash mid-save never leaves a half-written file. A corrupt state file is moved aside to `state.json.corrupt` and the service starts fresh instead of crash-looping.
- **Cost, measured** (`scripts/bench_state.py`, 200k BGL lines, 90 templates): 154 KiB state file, 7 ms to save, 6 ms to restore, restored stats identical.
- **Tested:** round-trip equality, no false `NEW_TEMPLATE` after restart, rate spikes still detected after restart, save on shutdown, corrupt-file handling. CI also restarts the real container on a volume and checks the state came back.
- **Caveat:** the template tree is encoded with jsonpickle (Drain3's own snapshot format), which can instantiate Python objects, so only load state files this service wrote.

## Engineering notes
- **Throughput:** the detector handles 100k+ lines/s on one laptop thread (`scripts/bench.py`, 200k BGL lines, detection logic only, without HTTP; 110–150k depending on machine load).
- **Deterministic and testable:** time is passed into the detector instead of read from the clock. Rate statistics use Welford's online mean/variance (O(1) memory per template).
- **Thread safety:** Drain3 isn't thread-safe, so a lock wraps detector access in the API.
- **Bounded memory:** the alert buffer is capped (500), as is the per-minute dedup set.
- **Input validation:** Pydantic models reject malformed payloads with 422 and cap batch size (10,000 lines).
- **Tests** (`tests/`, 13): each alert type, warm-up, per-minute dedup, rate-spike dedup, endpoint filters, bad payloads, persistence.
- **CI** (`.github/workflows/ci.yml`): runs the tests, builds the Docker image, starts it, smoke-tests `/health` and `/ingest`, then restarts the container on the same volume and checks the state was restored.
- **Container:** non-root user, healthcheck, slim base image.

## Limitations / next steps
- ~~State is in memory: a restart forgets learned templates.~~ Done in v0.2 (see Persistence). A file per instance is fine for one node; several replicas would need shared storage such as Redis.
- Single process: scaling out would need a message queue (e.g. Kafka) in front, sharded by source (the per-source detectors make that split natural).
- No ground-truth evaluation in this repo. The offline evaluation lives in log-anomaly-bench.

## Author
Hamza Fakhri, Computer Science engineering student, Polytech Nantes.

## License
MIT, see [LICENSE](LICENSE). The public datasets used here keep their own licenses (sources linked above).
