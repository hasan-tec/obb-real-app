"""
Repair the two wrongly-flipped flags on OBB-BQ-11 KITS (is_welcome_kit, is_universal).

BQ-11 is a T1 *renewal* kit, not a welcome kit. A manual kit edit on 2026-08-21
(11:45-11:53 UTC, a batch that touched 23 kits) flipped its is_welcome_kit to true.

Evidence it is a renewal kit:
  - name is "APR 2026 BQ-11"; every genuine welcome kit is 'OBB-WK-*' named "YYYY - WK-xN"
  - CURATION_REBUILD_PLAN.md 12.3b(b) enumerates the four T1 renewal kits as
    AB11(28), BO11(67), BP11(68), BQ11(69)
  - Sheena's Loom (5:25) lists them as separate things: "we only use WKE1, yes,
    correct, and then BQ11" — WKE1 is the welcome kit, BQ11 the renewal kit

What the flag breaks while it is set (CURATION_REBUILD_PLAN.md 12.5 acceptance test):
  load_kits_for_blocking() routes is_welcome_kit rows away from t1_renewal_sorted, so
  the T1 recency ladder shifts by one and BQ-11 is never blocked at all (the welcome
  tiebreak picks WK-E1 at age_rank 10006 over BQ-11 at 69):

      curating   should block   actually blocks
      T1         BQ-11          BP-11
      T2         BQ-11          BP-11   <- inverted vs Sheena's own September sheet
      T3         BP-11          BO-11
      T4         BO-11          AB-11

  BQ-11 has 50 units in stock and is the current T1 renewal kit, so its items sit in
  CAN USE and can be shipped to someone who already received them. Its 50 units also
  inflate the "Welcome Kit Stock" tile.

SECOND FLAG, same kit, same edit session (found 2026-08-23): is_universal was also
flipped, to false. BQ-11 is the ONLY kit in the whole T1 set with is_universal=false --
BP-11, BO-11, AB-11, every WK-*, and every CP-* kit are all universal. Proof it is
universal in practice: Sheena's AUG CP sheet shows a Pirate Ship batch shipping a BQ-11
box to Mariah Saxton, who is size XL. While the flag is false, evaluate_existing_kit_
coverage tells every non-S T1 customer "No kits match size M/L/XL for T1", so the report
asks the warehouse to build a fresh T1 run while 50 usable BQ-11 units sit in stock.

Touches exactly two boolean columns on one row. Never creates a kit, never touches items,
kit_items, shipments, or quantity_available.

Dry run (default, writes nothing):
    python scripts/fix_bq11_welcome_kit_flag.py
Apply:
    python scripts/fix_bq11_welcome_kit_flag.py --apply
"""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.fix_bq11")

TARGET_SKU = "OBB-BQ-11 KITS"


def _load_env() -> None:
    env_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
    )
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def main() -> int:
    apply = "--apply" in sys.argv
    _load_env()

    from supabase import create_client

    db = create_client(
        os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )

    rows = (
        db.table("kits")
        .select("id, sku, name, trimester, age_rank, is_welcome_kit, is_universal, quantity_available")
        .eq("sku", TARGET_SKU)
        .execute()
        .data
        or []
    )

    if len(rows) != 1:
        logger.error(
            "[FIX BQ11] expected exactly 1 kit with sku=%s, found %d — aborting",
            TARGET_SKU, len(rows),
        )
        return 1

    kit = rows[0]
    logger.info(
        "[FIX BQ11] current state — sku=%s name=%s trimester=%s age_rank=%s "
        "is_welcome_kit=%s is_universal=%s quantity_available=%s",
        kit["sku"], kit["name"], kit["trimester"], kit["age_rank"],
        kit["is_welcome_kit"], kit["is_universal"], kit["quantity_available"],
    )

    if kit["trimester"] != 1:
        logger.error(
            "[FIX BQ11] guard failed — expected trimester=1, got %s. This is not the kit "
            "this script was written for; aborting without writing.",
            kit["trimester"],
        )
        return 1

    fixes = {}
    if kit["is_welcome_kit"]:
        fixes["is_welcome_kit"] = False
    if not kit["is_universal"]:
        fixes["is_universal"] = True

    if not fixes:
        logger.info("[FIX BQ11] already correct — nothing to do")
        return 0

    if not apply:
        logger.info(
            "[FIX BQ11] DRY RUN — would apply %s on %s (id=%s). Re-run with --apply to write.",
            fixes, kit["sku"], kit["id"],
        )
        return 0

    db.table("kits").update(fixes).eq("id", kit["id"]).execute()

    after = (
        db.table("kits")
        .select("sku, is_welcome_kit, is_universal, quantity_available, trimester, age_rank")
        .eq("id", kit["id"])
        .execute()
        .data[0]
    )
    logger.info("[FIX BQ11] applied — read-back: %s", after)

    if after["is_welcome_kit"] or not after["is_universal"]:
        logger.error("[FIX BQ11] read-back still wrong: %s — write failed", after)
        return 1

    logger.info(
        "[FIX BQ11] done. Regenerate the curation report so the block lists pick this up."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
