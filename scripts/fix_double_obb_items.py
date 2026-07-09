#!/usr/bin/env python3
"""
fix_double_obb_items.py
=======================
One-off cleanup: some items in the `items` table were seeded with a DOUBLE
'OBB-OBB - ...' prefix (an OBB-brand line that got the system 'OBB-' prefix
added on top of an existing 'OBB - ' brand token). This renames each to a
single 'OBB-' prefix, e.g.

    OBB-OBB - HELLO BABY JOURNAL   ->   OBB-HELLO BABY JOURNAL

Only `items.sku` is changed. `kit_items` links by `item_id` (not sku), so the
rename does not break any kit composition. Names already carry a single 'Obb'
and are left untouched.

Safety:
  - Collision guard: if the target single-OBB sku already exists on ANOTHER
    item, we DON'T rename (would need a merge) — it's reported for review.
  - Idempotent: rows already single-OBB are ignored.
  - --dry-run shows every planned rename; --live applies them.

USAGE
  python scripts/fix_double_obb_items.py --dry-run
  python scripts/fix_double_obb_items.py --live
"""
import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""
SCRIPT_DIR = Path(__file__).parent

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "fix_double_obb_items.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("obb_fix_double")


def single_obb_sku(double_sku: str) -> str:
    """
    'OBB-OBB - HELLO BABY JOURNAL' -> 'OBB-HELLO BABY JOURNAL'.
    Strip the leading system 'OBB-', then strip the leading 'OBB' brand token
    (and its ' - '/'-' separator), then re-apply a single 'OBB-'.
    """
    rest = double_sku[4:] if double_sku.upper().startswith("OBB-") else double_sku
    rest = re.sub(r"^OBB\b[\s\-]*", "", rest, flags=re.IGNORECASE)
    return "OBB-" + rest


def main():
    ap = argparse.ArgumentParser(description="Rename double-OBB item SKUs to single-OBB.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="Preview renames, write NOTHING.")
    g.add_argument("--live", action="store_true", help="Apply the renames.")
    args = ap.parse_args()

    logger.info("=" * 70)
    logger.info("  FIX DOUBLE-OBB ITEM SKUS  —  %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("=" * 70)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set in .env")
        sys.exit(1)

    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # All item SKUs (for collision detection) + the double-OBB offenders.
    all_skus = {i["sku"].strip().upper()
                for i in (db.table("items").select("sku").execute().data or []) if i.get("sku")}
    doubles = db.table("items").select("id, sku, name").ilike("sku", "OBB-OBB%").execute().data or []

    logger.info("Found %d double-OBB items.", len(doubles))

    renames = []     # (id, old, new)
    collisions = []  # (old, new)
    for it in doubles:
        old = it["sku"]
        new = single_obb_sku(old)
        if new.upper() == old.upper():
            continue  # nothing to change
        if new.upper() in all_skus:
            collisions.append((old, new))
            continue
        renames.append((it["id"], old, new))

    logger.info("")
    logger.info("PLANNED RENAMES (%d):", len(renames))
    for _id, old, new in renames:
        logger.info("  %-52s  ->  %s", old, new)

    if collisions:
        logger.info("")
        logger.info("!! COLLISIONS — single-OBB target already exists (needs manual merge, SKIPPED):")
        for old, new in collisions:
            logger.info("  %-52s  ->  %s  (EXISTS)", old, new)

    if args.dry_run:
        logger.info("")
        logger.info("DRY RUN COMPLETE — nothing written. Re-run with --live to apply.")
        return

    done = 0
    for _id, old, new in renames:
        try:
            db.table("items").update({"sku": new}).eq("id", _id).execute()
            done += 1
            logger.info("  [RENAMED] %s -> %s", old, new)
        except Exception as e:
            logger.error("  [FAIL] %s -> %s: %s", old, new, e)

    logger.info("")
    logger.info("=" * 70)
    logger.info("  DONE — %d items renamed to single-OBB, %d collisions skipped", done, len(collisions))
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
