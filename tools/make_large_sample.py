"""Generate a sample that exercises the bounded-memory delete scan.

The interesting file is ``delete-pos-0001.parquet``: it contains two million
rows *in a single Parquet row group*, while only a handful of rows refer to
the two tickets being checked.  A checker that materialises the whole delete
file (or even the whole row group) needs hundreds of MiB; the streaming
checker only keeps the matching rows.

The generated directory contains a manifest and Parquet replicas that can be
uploaded directly from the web form:

    python3 tools/make_large_sample.py
    python3 tools/make_large_sample.py --rows 5000000 --out /tmp/large-sample

Then upload ``manifest.csv`` plus every ``.parquet`` file and check the
default tickets, for example ``4242, 1003``.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]

BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"
DATA1_PATH = f"{BASE}/data/data-0001.parquet"
DELETE1_PATH = f"{BASE}/delete-pos/delete-pos-0001.parquet"
STALE_DELETE_PATH = f"{BASE}/delete-pos/delete-pos-stale-0001.parquet"
IRRELEVANT_PATH = f"{BASE}/data/data-9999.parquet"

DATA_SCHEMA = pa.schema(
    [
        ("ticket_no", pa.int64()),
        ("passenger_name", pa.string()),
        ("fare_cents", pa.int64()),
        ("booking_status", pa.string()),
        ("voyage_date", pa.string()),
    ]
)

DELETE_SCHEMA = pa.schema(
    [
        ("file_path", pa.string()),
        ("pos", pa.int64()),
    ]
)


def write_data(path: Path) -> None:
    """Five row groups; ticket 4242 is the last row (global row 99)."""
    tickets = (
        list(range(1000, 1020))
        + list(range(2000, 2020))
        + list(range(3000, 3020))
        + list(range(4000, 4020))
        + list(range(4200, 4219))
        + [4242]
    )
    rows = [
        {
            "ticket_no": ticket,
            "passenger_name": f"P-{ticket}",
            "fare_cents": 1500 + (ticket % 700),
            "booking_status": "checked_in",
            "voyage_date": "2026-11-05",
        }
        for ticket in tickets
    ]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=DATA_SCHEMA),
        path,
        row_group_size=20,
        compression="snappy",
    )


def write_large_delete(path: Path, rows: int) -> None:
    """One million-scale row group with only a few relevant records.

    The relevant records deliberately sit at the very end of the row group so
    a streaming reader must walk the whole group without accumulating the
    unrelated rows.
    """
    if rows < 8:
        raise SystemExit("--rows must be at least 8")

    relevant_paths = [
        DATA1_PATH,  # ticket 1003 at row 3
        DATA1_PATH,  # ticket 4242 at row 99
        DATA1_PATH,  # duplicate evidence for 4242, same row
        IRRELEVANT_PATH,  # different path; must never match
    ]
    relevant_positions = [3, 99, 99, 99]

    filler_rows = rows - len(relevant_paths)
    paths = pa.concat_arrays(
        [
            pa.repeat(pa.scalar(IRRELEVANT_PATH), filler_rows),
            pa.array(relevant_paths, type=pa.string()),
        ]
    )
    positions = pa.concat_arrays(
        [
            pa.array(range(filler_rows), type=pa.int64()),
            pa.array(relevant_positions, type=pa.int64()),
        ]
    )
    table = pa.table(
        {"file_path": paths, "pos": positions}, schema=DELETE_SCHEMA
    )
    # row_group_size >= rows keeps the entire file in one row group.
    pq.write_table(table, path, row_group_size=rows, compression="snappy")


def write_stale_delete(path: Path) -> None:
    table = pa.table(
        {"file_path": [DATA1_PATH], "pos": [99]}, schema=DELETE_SCHEMA
    )
    pq.write_table(table, path, compression="snappy")


def write_manifest(path: Path) -> None:
    lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number",
        f"{DATA1_PATH},data-0001.parquet,data,7",
        f"{DELETE1_PATH},delete-pos-0001.parquet,position_delete,12",
        f"{STALE_DELETE_PATH},delete-pos-stale-0001.parquet,position_delete,6",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=ROOT / "sample-large",
        help="output directory (default: sample-large next to the repository)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=2_000_000,
        help="rows in the single-row-group delete file (default: 2000000)",
    )
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    write_data(args.out / "data-0001.parquet")
    write_large_delete(args.out / "delete-pos-0001.parquet", args.rows)
    write_stale_delete(args.out / "delete-pos-stale-0001.parquet")
    write_manifest(args.out / "manifest.csv")

    parquet = pq.ParquetFile(args.out / "delete-pos-0001.parquet")
    metadata = parquet.metadata
    print(
        f"wrote {args.out / 'delete-pos-0001.parquet'}: "
        f"{metadata.num_rows} rows in {metadata.num_row_groups} row group(s)"
    )
    for name in (
        "data-0001.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-stale-0001.parquet",
        "manifest.csv",
    ):
        print(f"wrote {args.out / name}")
    print("Upload manifest.csv and all three Parquet files; check 4242, 1003.")


if __name__ == "__main__":
    main()
