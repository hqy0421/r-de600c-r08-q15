#!/usr/bin/env python3
"""Verify batched pagination equals a single-shot time-ordered export.

Checks performed:
  1. trdsql one-shot export (no pagination) is the reference, ordered by
     (ts, num) with the same column list.
  2. Batched re-opens of the SAME snapshot concatenate to exactly the same row
     sequence, row by row (same order, same content, no gaps, no duplicates).
  3. Every emitted cursor actually points at the boundary row it claims, and
     the manifest's snapshot hash matches the file on disk.
  4. Rows sharing a timestamp are all present (tie handling).

Run:
    python3 -I verify_export.py --snapshot FILE --batch N --trdsql BIN --work DIR
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paged_export import (  # noqa: E402
    DEFAULT_COLUMNS,
    ROW_NUM_COLUMN,
    TIME_COLUMN,
    decode_cursor,
    export_all,
    snapshot_digest,
)


def oneshot_export(snapshot: str, batch_note: str, trdsql_bin: str) -> list[dict]:
    cols = ", ".join(DEFAULT_COLUMNS)
    query = (
        f"SELECT {cols} FROM `{snapshot}`"
        f" ORDER BY `{TIME_COLUMN}` ASC, `{ROW_NUM_COLUMN}` ASC"
    )
    proc = subprocess.run(
        [trdsql_bin, "-ijson", "-ojsonl", "-inum", query],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"{batch_note}: one-shot trdsql failed: {proc.stderr.strip()}"
        )
    return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]


def read_batches(out_dir: str, manifest: dict) -> list[dict]:
    rows = []
    for batch in manifest["batches"]:
        with open(os.path.join(out_dir, batch["file"]), encoding="utf-8") as fh:
            batch_rows = [json.loads(line) for line in fh if line.strip()]
        if len(batch_rows) != batch["rows"]:
            raise AssertionError(
                f"{batch['file']}: manifest says {batch['rows']} rows,"
                f" file has {len(batch_rows)}"
            )
        rows.extend(batch_rows)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--batch", type=int, required=True)
    ap.add_argument("--trdsql", required=True)
    ap.add_argument("--work", required=True)
    args = ap.parse_args()

    failures: list[str] = []

    reference = oneshot_export(args.snapshot, "reference", args.trdsql)
    print(f"reference one-shot export: {len(reference)} rows")

    out_dir = os.path.join(args.work, f"batches_b{args.batch}")
    manifest = export_all(
        args.snapshot, args.batch, out_dir, args.trdsql, DEFAULT_COLUMNS
    )
    batched = read_batches(out_dir, manifest)
    print(
        f"batched export: {manifest['batch_count']} batches,"
        f" {manifest['total_rows']} rows"
    )

    # --- check 1: identical count and exact row sequence -----------------
    if len(batched) != len(reference):
        failures.append(
            f"row count mismatch: batched={len(batched)} reference={len(reference)}"
        )
    for i, (got, want) in enumerate(zip(batched, reference)):
        if got != want:
            failures.append(f"first differing row at position {i}: {got} != {want}")
            break

    # --- check 2: no gaps / duplicates via the unique physical row number -
    nums = [row[ROW_NUM_COLUMN] for row in batched]
    if len(nums) != len(set(nums)):
        failures.append("duplicate num values across batches (rows re-emitted)")
    if sorted(nums) != list(range(1, len(nums) + 1)):
        failures.append("num values are not the complete 1..N set (rows lost)")

    # --- check 3: global (ts, num) ordering and timestamp tie coverage ----
    keys = [(row[TIME_COLUMN], row[ROW_NUM_COLUMN]) for row in batched]
    if keys != sorted(keys):
        failures.append("concatenated batches are not globally sorted by (ts, num)")
    tie_counts: dict[str, int] = {}
    for row in reference:
        tie_counts[row[TIME_COLUMN]] = tie_counts.get(row[TIME_COLUMN], 0) + 1
    got_ties: dict[str, int] = {}
    for row in batched:
        got_ties[row[TIME_COLUMN]] = got_ties.get(row[TIME_COLUMN], 0) + 1
    if got_ties != tie_counts:
        failures.append(f"per-timestamp row counts differ: {got_ties} vs {tie_counts}")
    tied = {ts: n for ts, n in tie_counts.items() if n > 1}
    print(f"timestamps with ties: {len(tied)} (largest tie group: "
          f"{max(tied.values(), default=1)} rows)")

    # --- check 4: every cursor_out equals the last key of that batch, and
    #     the following batch's first row is strictly after it -------------
    for j, batch in enumerate(manifest["batches"]):
        if not batch["rows"]:
            failures.append(f"batch {batch['index']} is unexpectedly empty")
            continue
        if batch["cursor_out"] is None:
            failures.append(f"batch {batch['index']} has rows but no cursor_out")
            continue
        decoded = decode_cursor(batch["cursor_out"])
        with open(os.path.join(out_dir, batch["file"]), encoding="utf-8") as fh:
            last = json.loads(list(fh)[-1])
        if (decoded["ts"], decoded["num"]) != (
            last[TIME_COLUMN],
            last[ROW_NUM_COLUMN],
        ):
            failures.append(
                f"batch {batch['index']}: cursor does not match its last row"
            )
        if j > 0:
            prev = decode_cursor(manifest["batches"][j - 1]["cursor_out"])
            with open(os.path.join(out_dir, batch["file"]), encoding="utf-8") as fh:
                first = json.loads(next(iter(fh)))
            before = (prev["ts"], prev["num"])
            after = (first[TIME_COLUMN], first[ROW_NUM_COLUMN])
            if not after > before:
                failures.append(
                    f"batch {batch['index']}: first key {after} not after"
                    f" previous cursor {before}"
                )

    # --- check 5: manifest snapshot hash matches the re-opened file -------
    on_disk = snapshot_digest(args.snapshot)
    if on_disk != manifest["snapshot_sha256"]:
        failures.append("manifest snapshot hash differs from file on disk")
    if hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest():
        pass  # digest only needs to be stable; content validated above.

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nPASS: batched export is identical to the one-shot export,")
    print("      with complete tie coverage and a valid cursor chain.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
