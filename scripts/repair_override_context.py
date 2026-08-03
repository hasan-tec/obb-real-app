"""
repair_override_context.py — repair manual-override decisions that lost their
order context (gift ship-to, purchaser billing, order link, order_type).

BACKGROUND
  POST /customers/{id}/override-kit inserted a replacement decision with
  order_id=None and no ship_to / billing / order_type. Two live consequences:
    1. submit_to_veracore fell back to the ACCOUNT HOLDER's name, so gift boxes
       were addressed to the purchaser instead of the recipient (returned mail).
    2. The decisions page treats NULL order_type as "renewal if any shipment
       exists", flipping New -> Renewal in the UI.
  The handler is fixed (app.py _prior_order_context). This repairs the rows
  already written.

WHAT IT DOES / DOES NOT DO
  Writes ONLY: order_id, platform, order_type, ship_first_name, ship_last_name,
                billing_first_name/last_name/address1/city/state/zip/country.
  Never touches veracore_*, status, kit_* — so it cannot trigger, re-send or
  alter any shipment. Already-shipped rows are corrected for record accuracy
  only (submit_to_veracore is idempotent once veracore_order_id is set).

IMPORTANT
  Context is taken from decisions created STRICTLY BEFORE the override, so an
  old override is repaired with the order it actually replaced -- not with a
  newer, unrelated order the customer placed later.

Usage:
  python scripts/repair_override_context.py --dry-run
  python scripts/repair_override_context.py --live
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

FIELDS = ["order_id", "platform", "order_type", "ship_first_name", "ship_last_name",
          "billing_first_name", "billing_last_name", "billing_address1",
          "billing_city", "billing_state", "billing_zip", "billing_country"]


def context_before(rows: list, before_created_at: str) -> dict:
    """Coalesce order context from decisions created strictly before a timestamp.

    Mirrors app._prior_order_context but time-bounded: newest-first, most recent
    non-NULL wins; name and address groups taken atomically from one row each.
    """
    prior = sorted(
        [r for r in rows if str(r.get("created_at") or "") < before_created_at],
        key=lambda r: str(r.get("created_at") or ""),
        reverse=True,
    )
    ctx: dict = {}
    for field in ("order_id", "platform", "order_type"):
        ctx[field] = next((r[field] for r in prior if r.get(field)), None)

    ship_row = next((r for r in prior if r.get("ship_first_name")), None)
    if ship_row:
        ctx["ship_first_name"] = ship_row.get("ship_first_name")
        ctx["ship_last_name"] = ship_row.get("ship_last_name")

    bill_row = next((r for r in prior if r.get("billing_address1") or r.get("billing_first_name")), None)
    if bill_row:
        for field in ("billing_first_name", "billing_last_name", "billing_address1",
                      "billing_city", "billing_state", "billing_zip", "billing_country"):
            ctx[field] = bill_row.get(field)
    return ctx


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--live", action="store_true")
    args = ap.parse_args()

    db = get_supabase()
    overrides = (db.table("decisions")
                 .select("id, customer_id, kit_sku, status, veracore_order_id, created_at, "
                         + ", ".join(FIELDS))
                 .eq("decision_type", "manual-override")
                 .order("created_at").execute().data or [])
    logger.info("Found %d manual-override decisions", len(overrides))

    by_cust: dict = {}
    for o in overrides:
        by_cust.setdefault(o["customer_id"], None)
    for cid in by_cust:
        by_cust[cid] = (db.table("decisions")
                        .select("created_at, " + ", ".join(FIELDS))
                        .eq("customer_id", cid).execute().data or [])

    repaired = shipped_note = skipped = 0
    for o in overrides:
        ctx = context_before(by_cust[o["customer_id"]], str(o["created_at"]))
        patch = {f: ctx.get(f) for f in FIELDS if not o.get(f) and ctx.get(f)}
        if not patch:
            skipped += 1
            continue
        already_shipped = bool(o.get("veracore_order_id"))
        if already_shipped:
            shipped_note += 1
        logger.info("%s decision=%s kit=%s status=%s%s -> %s",
                    "WOULD PATCH" if args.dry_run else "PATCHING",
                    o["id"][:8], o.get("kit_sku"), o.get("status"),
                    "  [ALREADY SHIPPED - record accuracy only]" if already_shipped else "",
                    {k: v for k, v in patch.items()})
        if args.live:
            db.table("decisions").update(patch).eq("id", o["id"]).execute()
        repaired += 1

    logger.info("Done. mode=%s | rows patched=%d (of which already-shipped=%d) | nothing-to-do=%d",
                "LIVE" if args.live else "DRY-RUN", repaired, shipped_note, skipped)


if __name__ == "__main__":
    main()
