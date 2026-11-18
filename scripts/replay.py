"""Replay a real log file into the running service, to see the detector work live.

Uses Loghub's BGL sample (BlueGene/L supercomputer) by default. Event time comes from the
log itself (2nd field = unix timestamp), so rate-spike detection sees realistic timing.

Usage:
    uvicorn logstream.api:app            # terminal 1
    python scripts/replay.py --speed 20  # terminal 2  (batches per second)
"""
import argparse
import json
import time
import urllib.request
from pathlib import Path

SAMPLE = "https://raw.githubusercontent.com/logpai/loghub/master/BGL/BGL_2k.log"


def batches(path: Path, size: int):
    batch, ts = [], None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            # BGL: label ts date node time node type component level message
            parts = line.split(" ", 9)
            t = float(parts[1]) if len(parts) > 1 and parts[1].isdigit() else time.time()
            # send "component level message", like a log shipper would after parsing the
            # header; the ground-truth label column is dropped so the service never sees it
            text = " ".join(parts[7:]) if len(parts) == 10 else line
            ts = ts or t
            batch.append(text.rstrip("\n"))
            if len(batch) == size:
                yield batch, ts
                batch, ts = [], None
    if batch:
        yield batch, ts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", type=Path, default=Path("data/BGL_2k.log"))
    p.add_argument("--url", default="http://127.0.0.1:8000")
    p.add_argument("--batch", type=int, default=50)
    p.add_argument("--speed", type=float, default=10, help="batches per second")
    p.add_argument("--source", default="bgl", help="source name sent with every batch")
    a = p.parse_args()
    if not a.file.exists():
        a.file.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(SAMPLE, a.file)
    sent = alerts = 0
    for lines, ts in batches(a.file, a.batch):
        body = json.dumps({"lines": lines, "timestamp": ts, "source": a.source}).encode()
        req = urllib.request.Request(f"{a.url}/ingest", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            alerts += len(json.load(r)["alerts"])
        sent += len(lines)
        print(f"\r{sent:,} lines sent, {alerts:,} alerts", end="", flush=True)
        time.sleep(1 / a.speed)
    print()


if __name__ == "__main__":
    main()
