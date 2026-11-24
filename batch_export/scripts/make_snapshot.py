#!/usr/bin/env python3
"""Generate an immutable instrument JSONL snapshot for pagination tests.

The data deliberately includes:
  * many rows sharing the exact same timestamp (ties are the norm, not edge)
  * rows NOT pre-sorted by time in the file (ORDER BY must do the work)
  * a leading repeated timestamp and a trailing repeated timestamp
  * stable unique per-row ids so correctness can be cross-checked
Run with: python3 -I make_snapshot.py <output.jsonl> [rows]
"""
import json
import sys
from datetime import datetime, timedelta, timezone


def main() -> int:
    out = sys.argv[1]
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 37
    base = datetime(2026, 11, 24, 9, 0, 0, tzinfo=timezone.utc)
    records = []
    # Quantize to a small set of timestamps so ties are dense.
    for i in range(rows):
        bucket = i % 6
        ts = (base + timedelta(minutes=5 * bucket)).strftime("%Y-%m-%dT%H:%M:%SZ")
        records.append({
            "ts": ts,
            "meter_id": f"M{(i % 4) + 1:02d}",
            "reading_id": f"R{i:05d}",
            "kwh": round(((i * 7) % 13) * 1.5 + 0.25, 2),
            "quality": "OK" if i % 11 else "FLAG",
        })
    # Shuffle deterministically (no reliance on hash randomization): reverse
    # then interleave so the on-disk order differs from the (ts, num) order.
    shuffled = [None] * len(records)
    half = (len(records) + 1) // 2
    order = list(range(0, len(records), 2)) + list(range(1, len(records), 2))
    for slot, src in enumerate(order):
        shuffled[slot] = records[src]
    assert len(shuffled) == half + (len(records) - half)
    with open(out, "w", encoding="utf-8") as fh:
        for rec in shuffled:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
    print(f"wrote {len(shuffled)} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
