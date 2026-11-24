#!/usr/bin/env python3
"""Keyset-paginated, time-ordered export of an instrument JSONL snapshot.

Design (see README.md for the full rationale):

* The target program (trdsql) reads one immutable JSONL snapshot per batch and
  runs a single SQL statement; this driver only builds the statement and the
  cursor. No state other than the opaque cursor crosses batch boundaries.
* Rows are ordered by the composite key (ts ASC, num ASC). ``num`` is the
  1-based physical line number that trdsql adds with ``-inum``. It is unique
  and stable for a given snapshot, so rows sharing a timestamp never tie and
  no row can be skipped or duplicated.
* The cursor is the last emitted key, encoded as opaque base64url JSON. The
  next batch re-opens the SAME snapshot and asks for rows strictly after that
  key with the row-value predicate:

        WHERE ts > :ts OR (ts = :ts AND num > :num)

  Concatenating every batch therefore reproduces the single-shot
  ``ORDER BY ts, num`` export byte-for-byte at the row level.

Usage:
    python3 -I paged_export.py --snapshot FILE --batch N [--cursor TOKEN]
                               [--trdsql BIN] [--columns col,col]
    python3 -I paged_export.py --snapshot FILE --batch N --all --out DIR

Exit code is non-zero if trdsql fails. With --all, batch files and a
manifest are written to DIR.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass

# Columns selected from the snapshot, in output order. ``num`` is always
# selected because the next cursor needs it even if callers do not.
DEFAULT_COLUMNS = ["num", "ts", "meter_id", "reading_id", "kwh", "quality"]
TIME_COLUMN = "ts"
ROW_NUM_COLUMN = "num"


def encode_cursor(ts: str, num: int) -> str:
    """Encode the next-batch position as an opaque, self-describing token."""
    raw = json.dumps(
        {"v": 1, "ts": ts, "num": num}, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(token: str) -> dict:
    """Decode a cursor produced by encode_cursor (tokens are not user-authored)."""
    if not token:
        raise ValueError("empty cursor token")
    padded = token + "=" * (-len(token) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"malformed cursor token: {exc}") from exc
    if not isinstance(payload.get("ts"), str) or not isinstance(
        payload.get("num"), int
    ):
        raise ValueError("cursor is missing a string 'ts' or integer 'num'")
    if payload["num"] < 1:
        raise ValueError("cursor num must be a positive row number")
    return payload


@dataclass
class BatchResult:
    rows: list[dict]
    next_cursor: str | None
    cursor_of_last: tuple[str, int] | None
    # has_more is exact: the engine was asked for batch_size + 1 rows, so a
    # full answer plus one extra means another batch exists.
    has_more: bool


def _sql_quote_literal(value: str) -> str:
    """Quote a string for inclusion inside a SQLite text literal."""
    return "'" + value.replace("'", "''") + "'"


def build_query(
    table: str,
    columns: list[str],
    batch_size: int,
    cursor: dict | None,
) -> str:
    """Build the deterministic keyset-pagination query for one batch."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    select_cols = ", ".join(columns)
    where = ""
    if cursor is not None:
        ts_lit = _sql_quote_literal(cursor["ts"])
        where = (
            f" WHERE `{TIME_COLUMN}` > {ts_lit}"
            f" OR (`{TIME_COLUMN}` = {ts_lit}"
            f" AND `{ROW_NUM_COLUMN}` > {int(cursor['num'])})"
        )
    # ORDER BY must match the cursor predicate exactly; both use (ts, num).
    return (
        f"SELECT {select_cols} FROM `{table}`{where}"
        f" ORDER BY `{TIME_COLUMN}` ASC, `{ROW_NUM_COLUMN}` ASC"
        f" LIMIT {int(batch_size)}"
    )


def run_batch(
    snapshot: str,
    batch_size: int,
    cursor_token: str | None,
    trdsql_bin: str = "trdsql",
    columns: list[str] | None = None,
) -> BatchResult:
    """Open the snapshot fresh and read exactly one batch."""
    cols = columns or DEFAULT_COLUMNS
    if ROW_NUM_COLUMN not in cols or TIME_COLUMN not in cols:
        raise ValueError("columns must include both 'num' and 'ts'")
    cursor = decode_cursor(cursor_token) if cursor_token else None
    # Fetch one extra row: it is never returned, only used to decide whether
    # another batch exists, so no empty probe batch is ever emitted.
    query = build_query(snapshot, cols, batch_size + 1, cursor)

    proc = subprocess.run(
        [trdsql_bin, "-ijson", "-ojsonl", "-inum", query],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"trdsql failed (exit {proc.returncode}): {proc.stderr.strip()}\n"
            f"query was: {query}"
        )

    fetched: list[dict] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line:
            fetched.append(json.loads(line))

    has_more = len(fetched) > batch_size
    rows = fetched[:batch_size]

    next_cursor = None
    last_key = None
    if rows:
        last = rows[-1]
        last_key = (last[TIME_COLUMN], int(last[ROW_NUM_COLUMN]))
        next_cursor = encode_cursor(*last_key)
    return BatchResult(rows, next_cursor, last_key, has_more)


def snapshot_digest(path: str) -> str:
    """Stable content hash proving every batch read the same snapshot."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def export_all(
    snapshot: str,
    batch_size: int,
    out_dir: str,
    trdsql_bin: str,
    columns: list[str] | None,
) -> dict:
    """Repeatedly re-open the snapshot until exhausted, writing batch files."""
    os.makedirs(out_dir, exist_ok=True)
    digest = snapshot_digest(snapshot)
    total_rows = 0
    batch_index = 0
    cursor: str | None = None
    manifest_batches = []

    while True:
        result = run_batch(snapshot, batch_size, cursor, trdsql_bin, columns)
        batch_index += 1
        part = os.path.join(out_dir, f"batch_{batch_index:05d}.jsonl")
        with open(part, "w", encoding="utf-8") as fh:
            for row in result.rows:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
        manifest_batches.append(
            {
                "index": batch_index,
                "file": os.path.basename(part),
                "rows": len(result.rows),
                "cursor_in": cursor,
                "cursor_out": result.next_cursor,
            }
        )
        total_rows += len(result.rows)
        if not result.has_more:
            break
        cursor = result.next_cursor

    manifest = {
        "snapshot": os.path.abspath(snapshot),
        "snapshot_sha256": digest,
        "batch_size": batch_size,
        "columns": columns or DEFAULT_COLUMNS,
        "order": [TIME_COLUMN, ROW_NUM_COLUMN],
        "total_rows": total_rows,
        "batch_count": batch_index,
        "batches": manifest_batches,
    }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, help="immutable JSONL file")
    parser.add_argument("--batch", type=int, required=True, help="rows per batch")
    parser.add_argument(
        "--cursor", default=None, help="opaque next-position token from prior batch"
    )
    parser.add_argument(
        "--trdsql", default=os.environ.get("TRDSQL_BIN", "trdsql"), help="trdsql path"
    )
    parser.add_argument("--columns", default=None, help="comma-separated columns")
    parser.add_argument(
        "--all", action="store_true", help="loop until exhausted, writing --out dir"
    )
    parser.add_argument("--out", default=None, help="output directory with --all")
    args = parser.parse_args(argv)

    cols = (
        [c.strip() for c in args.columns.split(",") if c.strip()]
        if args.columns
        else None
    )
    try:
        if args.all:
            if not args.out:
                parser.error("--all requires --out DIR")
            manifest = export_all(
                args.snapshot, args.batch, args.out, args.trdsql, cols
            )
            json.dump(manifest, sys.stdout, indent=2)
            sys.stdout.write("\n")
            return 0

        result = run_batch(
            args.snapshot, args.batch, args.cursor, args.trdsql, cols
        )
    except (ValueError, RuntimeError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    for row in result.rows:
        sys.stdout.write(json.dumps(row, separators=(",", ":")) + "\n")
    envelope = {
        "rows_in_batch": len(result.rows),
        "next_cursor": result.next_cursor,
        "has_more": result.has_more,
    }
    sys.stderr.write(json.dumps(envelope) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
