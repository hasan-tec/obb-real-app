#!/usr/bin/env python3
"""
OBB Backfill Script — Fill missing shipment_items
==================================================
For each shipment that has kit_id but no shipment_items,
copy all kit_items into shipment_items.

This is exactly what import_history.py does for new shipments,
but ~657 shipments were imported before their kits had kit_items populated.

USAGE:
  python backfill_shipment_items.py --dry-run   # count only
  python backfill_shipment_items.py --execute    # actually write
"""

import os
import sys
import json
import logging
import argparse
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://tkcvvjxmzfjaesdhyfiy.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY in environment")

from supabase import create_client

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backfill")


def paginate_all(db, table_name, select_cols="*"):
    all_rows = []
    offset = 0
    while True:
        r = db.table(table_name).select(select_cols).range(offset, offset + 999).execute()
        all_rows.extend(r.data or [])
        if len(r.data or []) < 1000:
            break
        offset += 1000
    return all_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Count only, no writes")
    parser.add_argument("--execute", action="store_true", help="Actually write to DB")
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        parser.print_help()
        print("\nError: must specify --dry-run or --execute")
        sys.exit(1)

    db = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    # Load kit_items
    logger.info("[LOAD] Loading kit_items...")
    kit_items_raw = paginate_all(db, "kit_items", "kit_id,item_id")
    kit_items_by_kit = defaultdict(list)
    for ki in kit_items_raw:
        kit_items_by_kit[ki["kit_id"]].append(ki["item_id"])
    logger.info(f"[LOAD] {len(kit_items_by_kit)} kits have item definitions ({len(kit_items_raw)} total kit_items)")

    # Load all shipments
    logger.info("[LOAD] Loading shipments...")
    all_shipments = paginate_all(db, "shipments", "id,kit_id,kit_sku")
    logger.info(f"[LOAD] {len(all_shipments)} shipments total")

    # Load existing shipment_items — just need the set of shipment_ids that have items
    logger.info("[LOAD] Loading shipment_items (checking coverage)...")
    ship_items_raw = paginate_all(db, "shipment_items", "shipment_id")
    has_items = set(si["shipment_id"] for si in ship_items_raw)
    logger.info(f"[LOAD] {len(has_items)} shipments already have items")

    # Find shipments needing backfill
    to_backfill = []
    cannot_backfill = []
    for s in all_shipments:
        if s["id"] in has_items:
            continue  # already has items
        if not s.get("kit_id"):
            cannot_backfill.append(s)
            continue
        if s["kit_id"] not in kit_items_by_kit:
            cannot_backfill.append(s)
            continue
        to_backfill.append(s)

    logger.info(f"\n[ANALYSIS] Shipments needing backfill:     {len(to_backfill)}")
    logger.info(f"[ANALYSIS] Cannot backfill (no kit/items): {len(cannot_backfill)}")

    # Group by kit for summary
    by_kit = defaultdict(int)
    total_items = 0
    for s in to_backfill:
        n = len(kit_items_by_kit[s["kit_id"]])
        by_kit[s.get("kit_sku", "unknown")] += 1
        total_items += n

    logger.info(f"[ANALYSIS] Total shipment_items to insert: {total_items}")
    logger.info(f"\n  By kit:")
    for sku, count in sorted(by_kit.items(), key=lambda x: -x[1]):
        logger.info(f"    {sku}: {count} shipments")

    if args.dry_run:
        logger.info("\n[DRY RUN] No changes made. Use --execute to write.")
        return

    # Execute backfill
    logger.info(f"\n[EXECUTE] Backfilling {len(to_backfill)} shipments...")

    batch_size = 100
    inserted_total = 0
    errors = 0

    # Build all insert records
    all_inserts = []
    for s in to_backfill:
        for item_id in kit_items_by_kit[s["kit_id"]]:
            all_inserts.append({"shipment_id": s["id"], "item_id": item_id})

    logger.info(f"[EXECUTE] {len(all_inserts)} shipment_item records to insert")

    for i in range(0, len(all_inserts), batch_size):
        batch = all_inserts[i:i + batch_size]
        try:
            db.table("shipment_items").upsert(batch, on_conflict="shipment_id,item_id").execute()
            inserted_total += len(batch)
            if (i // batch_size + 1) % 10 == 0:
                logger.info(f"  Progress: {inserted_total}/{len(all_inserts)} inserted")
        except Exception as e:
            logger.error(f"  Batch {i // batch_size + 1} failed: {e}")
            errors += 1
            # Try individual inserts
            for rec in batch:
                try:
                    db.table("shipment_items").upsert(rec, on_conflict="shipment_id,item_id").execute()
                    inserted_total += 1
                except Exception as e2:
                    logger.error(f"    Individual insert failed: {e2}")
                    errors += 1

    logger.info(f"\n[DONE] Inserted {inserted_total} shipment_items, {errors} errors")

    # Verify
    logger.info("[VERIFY] Checking remaining gaps...")
    ship_items_after = paginate_all(db, "shipment_items", "shipment_id")
    has_items_after = set(si["shipment_id"] for si in ship_items_after)
    still_missing = sum(1 for s in all_shipments if s["id"] not in has_items_after and s.get("kit_id"))
    logger.info(f"[VERIFY] Shipments with kit_id still missing items: {still_missing}")


if __name__ == "__main__":
    main()
