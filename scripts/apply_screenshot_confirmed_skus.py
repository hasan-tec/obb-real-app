"""
apply_screenshot_confirmed_skus.py — targeted, low-risk veracore_sku fill-in.

Writes items.veracore_sku for 3 items whose real VeraCore Product ID Hasan
confirmed directly from VeraCore's own admin UI (Product Inquiry search
screenshots, 2026-07-30) — the highest-confidence source available, since
it's ground truth even for products the GetInventory API feed omits.

  BELLYBRACE (LARGE)                                    -> OBB-BellyBrace(L)(Pink)
  China - Baby 1st Year Milestone Postcards (Chevron)    -> OBB-China+Baby1stYearMilestonePostcards(Chevron)
  China - Baby 1st Year Milestone Postcards (No Chevron) -> OBB-China+Baby1stYearMilestonePostcards

The two China ones are literally identical to the item's existing .sku value
(confirmed as the real Product ID by the screenshot) -- just copying sku ->
veracore_sku for those two. BELLYBRACE (LARGE) uses the confirmed Pink/Large
variant id since all live BellyBrace variants are Pink-only.

This does NOT merge/delete anything -- pure single-field UPDATE by item id.

Usage:
  python scripts/apply_screenshot_confirmed_skus.py --dry-run
  python scripts/apply_screenshot_confirmed_skus.py --live
"""
import os
import sys
import argparse
import logging

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_supabase  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("obb")

CONFIRMED = {
    "BELLYBRACE (LARGE)": "OBB-BellyBrace(L)(Pink)",
    "China - Baby 1st Year Milestone Postcards (Chevron)": "OBB-China+Baby1stYearMilestonePostcards(Chevron)",
    "China - Baby 1st Year Milestone Postcards (No Chevron)": "OBB-China+Baby1stYearMilestonePostcards",
}


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--live", action="store_true")
    args = ap.parse_args()

    db = get_supabase()
    items = db.table("items").select("id, sku, name, veracore_sku").execute().data or []
    by_name = {}
    for it in items:
        by_name.setdefault((it.get("name") or "").strip().lower(), []).append(it)

    for name, new_vsku in CONFIRMED.items():
        matches = by_name.get(name.strip().lower(), [])
        if len(matches) != 1:
            logger.error("SKIP name=%r — expected exactly 1 DB match, found %d", name, len(matches))
            continue
        it = matches[0]
        old_vsku = it.get("veracore_sku") or ""
        logger.info("name=%s id=%s sku=%s veracore_sku: %r -> %r",
                    name, it["id"], it["sku"], old_vsku, new_vsku)
        if args.live:
            db.table("items").update({"veracore_sku": new_vsku}).eq("id", it["id"]).execute()
            logger.info("  WROTE.")

    logger.info("Done. mode=%s", "LIVE" if args.live else "DRY-RUN")


if __name__ == "__main__":
    main()
