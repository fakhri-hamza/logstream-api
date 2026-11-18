"""Does one shared detector for several systems hide anything? Measured on real logs.

Streams 4 Loghub sample logs (2,000 lines each, interleaved line by line as if a log shipper
forwarded them together) into (a) one shared detector and (b) one detector per source.
Counts how many templates in the shared detector mix lines from different systems, and how many
"new message type" alerts the shared detector never raised because another system had already
taught it a similar template.

Usage:  python scripts/shared_vs_per_source.py
"""
import urllib.request
from collections import defaultdict
from itertools import zip_longest
from pathlib import Path

from logstream.detector import StreamDetector

LOGHUB = "https://raw.githubusercontent.com/logpai/loghub/master/{0}/{0}_2k.log"
SOURCES = ["BGL", "HDFS", "Zookeeper", "Hadoop"]
DATA = Path("data/loghub")


def fetch(name: str) -> list[str]:
    path = DATA / f"{name}_2k.log"
    if not path.exists():
        DATA.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(LOGHUB.format(name), path)
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def main():
    logs = {s: fetch(s) for s in SOURCES}
    stream = [(s, line) for group in zip_longest(*[[(s, l) for l in logs[s]] for s in SOURCES])
              for s, line in filter(None, group)]

    shared = StreamDetector(warmup_lines=0)
    per = {s: StreamDetector(warmup_lines=0) for s in SOURCES}
    members = defaultdict(set)                     # shared template id -> sources that used it
    new_shared = defaultdict(int)
    new_per = defaultdict(int)
    for t, (src, line) in enumerate(stream):
        for a in shared.process(line, t):
            new_shared[src] += a.kind == "NEW_TEMPLATE"
        cid = shared.miner.match(line)
        if cid is not None:
            members[cid.cluster_id].add(src)
        for a in per[src].process(line, t):
            new_per[src] += a.kind == "NEW_TEMPLATE"

    mixed = sum(len(v) > 1 for v in members.values())
    print(f"{len(stream):,} interleaved lines from {len(SOURCES)} systems")
    print(f"shared detector: {len(shared.miner.drain.clusters)} templates, {mixed} of them mix lines "
          f"from 2+ systems")
    print(f"per-source detectors: {sum(len(d.miner.drain.clusters) for d in per.values())} templates")
    print(f"{'source':10s} {'new-type alerts: shared':>24s} {'per-source':>11s}")
    for s in SOURCES:
        print(f"{s:10s} {new_shared[s]:24d} {new_per[s]:11d}")
    total_s, total_p = sum(new_shared.values()), sum(new_per.values())
    print(f"{'total':10s} {total_s:24d} {total_p:11d}   -> shared misses {total_p - total_s}")


if __name__ == "__main__":
    main()
