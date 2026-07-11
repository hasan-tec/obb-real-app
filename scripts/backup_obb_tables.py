#!/usr/bin/env python3
"""
backup_obb_tables.py
====================
Snapshot / restore the tables the SKU-reconciliation plan can touch
(see VERACORE_SKU_RECONCILIATION_PLAN.md §6).

Tables: items, kits, kit_items, item_alternatives, shipments, shipment_items
(customers/decisions are never modified by the plan, so not included).

BACKUP (default):
  python scripts/backup_obb_tables.py
  -> backups/<UTC-timestamp>/<table>.json  (full rows, paginated fetch)
  -> backups/<UTC-timestamp>/manifest.json (row counts for verification)

RESTORE one table from a snapshot:
  python scripts/backup_obb_tables.py --restore backups/20260709T160000Z --table kit_items
  - link tables (kit_items, item_alternatives, shipment_items): DELETE ALL rows,
    re-insert snapshot in batches (rows are plain uuid pairs — safe).
  - entity tables (items, kits, shipments): UPSERT by id (no delete — FK children).

SELF-TEST the restore path (required before any write phase, §6.3):
  python scripts/backup_obb_tables.py --restore <dir> --table item_alternatives
  then confirm the printed row count matches the manifest.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""

ROOT = Path(__file__).parent.parent
BACKUP_ROOT = ROOT / "backups"

TABLES = ["items", "kits", "kit_items", "item_alternatives", "shipments", "shipment_items"]
LINK_TABLES = {"kit_items", "item_alternatives", "shipment_items"}   # restore = delete-all + insert
ZERO_UUID = "00000000-0000-0000-0000-000000000000"
# column used for the required PostgREST delete filter per link table
DELETE_FILTER_COL = {"kit_items": "kit_id", "item_alternatives": "item_id", "shipment_items": "shipment_id"}


def fetch_all(db, table: str) -> list:
    """Paginated SELECT * — Supabase caps a single response at 1000 rows."""
    rows, offset = [], 0
    while True:
        batch = db.table(table).select("*").range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            return rows
        offset += 1000


def do_backup(db) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = BACKUP_ROOT / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for t in TABLES:
        rows = fetch_all(db, t)
        (out_dir / f"{t}.json").write_text(json.dumps(rows, default=str), encoding="utf-8")
        manifest[t] = len(rows)
        print(f"  [BACKUP] {t:18} {len(rows):6} rows -> {t}.json")
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\nSnapshot complete: {out_dir}")
    print("Verify counts above against the app/DB before relying on this snapshot.")
    return out_dir


def do_restore(db, snap_dir: Path, table: str):
    if table not in TABLES:
        print(f"Unknown table '{table}'. Choose from: {TABLES}")
        sys.exit(1)
    f = snap_dir / f"{table}.json"
    if not f.exists():
        print(f"Snapshot file missing: {f}")
        sys.exit(1)
    rows = json.loads(f.read_text(encoding="utf-8"))
    manifest = json.loads((snap_dir / "manifest.json").read_text(encoding="utf-8"))
    print(f"[RESTORE] {table}: snapshot has {len(rows)} rows (manifest says {manifest.get(table)})")

    if table in LINK_TABLES:
        col = DELETE_FILTER_COL[table]
        print(f"  deleting ALL current rows of {table} ...")
        db.table(table).delete().neq(col, ZERO_UUID).execute()
        for i in range(0, len(rows), 500):
            db.table(table).insert(rows[i:i + 500]).execute()
        print(f"  re-inserted {len(rows)} rows")
    else:
        for i in range(0, len(rows), 200):
            db.table(table).upsert(rows[i:i + 200], on_conflict="id").execute()
        print(f"  upserted {len(rows)} rows by id")

    # verify
    live = fetch_all(db, table)
    status = "OK" if len(live) == len(rows) else "MISMATCH — investigate before proceeding!"
    print(f"[VERIFY] live rows now: {len(live)} vs snapshot {len(rows)} -> {status}")


def main():
    ap = argparse.ArgumentParser(description="Backup/restore OBB tables touched by the SKU reconciliation plan.")
    ap.add_argument("--restore", metavar="SNAP_DIR", help="Restore mode: path to a snapshot directory.")
    ap.add_argument("--table", help="Table to restore (required with --restore).")
    args = ap.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set in .env")
        sys.exit(1)
    db = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.restore:
        if not args.table:
            print("--table is required with --restore")
            sys.exit(1)
        do_restore(db, Path(args.restore), args.table)
    else:
        do_backup(db)


if __name__ == "__main__":
    main()
