#!/usr/bin/env python3
"""
fix_kit_veracore_sku.py
=======================
Populate `kits.veracore_sku` for the 7 active kits whose ONLY mismatch with
VeraCore is the trailing "KITS" (our DB) vs "Kit" (VeraCore) — verified against
REEXPORTED-ProductSummary-639191819179040822.xls (all 7 targets exist).

We set veracore_sku (NOT sku) on purpose:
  - order push uses `veracore_sku or sku` (app.py) and inventory sync matches
    veracore_sku first, so sync starts working immediately;
  - kits.sku is denormalised as text in shipments.kit_sku / decisions.kit_sku and
    the engine's "already got this kit" guard reads that text — leaving sku alone
    keeps that guard intact.

Anchored on kit sku (unique). Idempotent: skips a kit whose veracore_sku already
matches the target. Collision-safe: never touches sku.

The 2 ambiguous kits (OBB-BT-41, OBB-CK-21) are intentionally NOT here — they need
Sheena's confirmation (no exact VeraCore match).

USAGE
  python scripts/fix_kit_veracore_sku.py --dry-run
  python scripts/fix_kit_veracore_sku.py --live
"""
import argparse
import logging
import os
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
        logging.FileHandler(SCRIPT_DIR / "fix_kit_veracore_sku.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("obb_fix_kit_vcsku")

# kit sku (our DB)  ->  exact VeraCore Product ID (verified present in the export)
TARGETS = {
    "OBB-BB-31 KITS": "OBB-BB-31 Kit",
    "OBB-BC-21 KITS": "OBB-BC-21 Kit",
    "OBB-BM-32 KITS": "OBB-BM-32 Kit",
    "OBB-BM-33 KITS": "OBB-BM-33 Kit",
    "OBB-BM-34 KITS": "OBB-BM-34 Kit",
    "OBB-BQ-22 KITS": "OBB-BQ-22 Kit",
    "OBB-BQ-41 KITS": "OBB-BQ-41 Kit",
}


def main():
    ap = argparse.ArgumentParser(description="Set veracore_sku for the 7 confirmed trailing-S kits.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="Preview, write NOTHING.")
    g.add_argument("--live", action="store_true", help="Apply the updates.")
    args = ap.parse_args()

    logger.info("=" * 68)
    logger.info("  FIX KIT veracore_sku (7 confirmed kits)  —  %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("=" * 68)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set in .env")
        sys.exit(1)
    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    planned, skipped, missing = [], [], []
    for kit_sku, target in TARGETS.items():
        rows = db.table("kits").select("id, sku, veracore_sku, quantity_available").eq("sku", kit_sku).execute().data or []
        if not rows:
            missing.append(kit_sku)
            continue
        kit = rows[0]
        if (kit.get("veracore_sku") or "").strip().upper() == target.upper():
            skipped.append((kit_sku, target))
            continue
        planned.append((kit["id"], kit_sku, kit.get("veracore_sku"), target, kit.get("quantity_available")))

    logger.info("")
    logger.info("PLANNED (%d):", len(planned))
    logger.info("  %-18s %-16s -> %-16s  qty", "kit sku", "veracore_sku(old)", "veracore_sku(new)")
    for _id, sku, old, new, qty in planned:
        logger.info("  %-18s %-16s -> %-16s  %s", sku, old or "(blank)", new, qty)
    if skipped:
        logger.info("")
        logger.info("ALREADY SET (skipped): %s", ", ".join(s for s, _ in skipped))
    if missing:
        logger.info("")
        logger.warning("NOT FOUND in DB (skipped): %s", ", ".join(missing))

    if args.dry_run:
        logger.info("")
        logger.info("DRY RUN — nothing written. Re-run with --live to apply.")
        return

    done = 0
    for _id, sku, old, new, qty in planned:
        try:
            db.table("kits").update({"veracore_sku": new}).eq("id", _id).execute()
            done += 1
            logger.info("  [SET] %s  veracore_sku=%s", sku, new)
        except Exception as e:
            logger.error("  [FAIL] %s -> %s: %s", sku, new, e, exc_info=True)

    logger.info("")
    logger.info("=" * 68)
    logger.info("  DONE — %d kits updated, %d already-set, %d missing", done, len(skipped), len(missing))
    logger.info("=" * 68)


if __name__ == "__main__":
    main()
