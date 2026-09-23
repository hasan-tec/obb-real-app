"""
Threads 20/21 — D4: one-off production repair for Nicole Goldstein's account
(nicgoldstein@goldsteinus.net), the concrete example behind root causes P4/P7 (decision's
ship-to never got the ORDER'S recipient, P8: buyer address overwrote it) and B8 (duplicate
shipment history from the pre-fix ship_decision).

  1. Decision 68ffd1be-569d-4c29-b955-7847955567bf still carries the purchaser's name /
     Nicole's own address instead of the box's real recipient. Set its ship-to snapshot to:
       Kailin Goldstein
       1100 South Loop 336 West, Apt 3213
       Conroe, Texas 77304, US
     ship_to_source='order' — the same shape ship_to_cols_from_shopify() would have written
     had B2 been live when this order came in.
  2. Shipment 862f794d-5f02-44bf-8edf-0bf3f819acb2 is a duplicate manual row for the SAME box
     as shipment 51214d2b… (the exact bug B8 now guards against) — delete it, its
     shipment_items first (FK is on shipment_id).

Prints BEFORE and AFTER for both. Safe to run twice (Golden rule 2): if the decision already
has this ship-to, or the duplicate shipment is already gone, that step is reported and skipped.

Dry run (default, writes nothing):
    python scripts/repair_threads_20_21.py
Apply:
    python scripts/repair_threads_20_21.py --apply
"""
import argparse
import logging
import os
import sys

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")  # importing app must not start background jobs
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402  (loads .env, gives get_supabase)

try:
    sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to cp1252
except Exception:
    pass
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.repair_threads_20_21")

NICOLE_EMAIL = "nicgoldstein@goldsteinus.net"
DECISION_ID = "68ffd1be-569d-4c29-b955-7847955567bf"
DUP_SHIPMENT_ID = "862f794d-5f02-44bf-8edf-0bf3f819acb2"

SHIP_TO_COLS = {
    "ship_first_name": "Kailin",
    "ship_last_name": "Goldstein",
    "ship_address1": "1100 South Loop 336 West",
    "ship_address2": "Apt 3213",
    "ship_city": "Conroe",
    "ship_state": "Texas",
    "ship_zip": "77304",
    "ship_country": "US",
    "ship_to_source": "order",
}


def _print_decision(label: str, d: dict) -> None:
    if not d:
        print(f"  {label}: NOT FOUND")
        return
    name = f"{d.get('ship_first_name') or ''} {d.get('ship_last_name') or ''}".strip() or "(none)"
    addr = ", ".join(x for x in [
        d.get("ship_address1"), d.get("ship_address2"), d.get("ship_city"),
        d.get("ship_state"), d.get("ship_zip"), d.get("ship_country"),
    ] if x) or "(none)"
    print(f"  {label}: ship_to={name!r}  address={addr}  ship_to_source={d.get('ship_to_source')!r}")


def _print_shipment(label: str, s: dict, item_count=None) -> None:
    if not s:
        print(f"  {label}: NOT FOUND (already deleted?)")
        return
    extra = f", {item_count} shipment_item(s)" if item_count is not None else ""
    print(f"  {label}: id={s['id']} customer_id={s.get('customer_id')} kit_sku={s.get('kit_sku')} "
          f"ship_date={s.get('ship_date')} notes={(s.get('notes') or '')[:60]!r}{extra}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    args = ap.parse_args()
    mode = "APPLY" if args.apply else "DRY RUN"
    db = app.get_supabase()

    print(f"\n══════ D4 {mode}: repair Nicole Goldstein's decision + duplicate shipment ══════")

    # ── 1. Decision ship-to ────────────────────────────────────────────────────────────
    dec_res = (db.table("decisions")
               .select("*, customers(email)").eq("id", DECISION_ID).execute())
    decision = dec_res.data[0] if dec_res.data else None
    print("\n[1] Decision ship-to")
    _print_decision("BEFORE", decision)

    dec_ok = True
    if decision is None:
        logger.error("[D4] decision %s not found — nothing to fix", DECISION_ID)
        dec_ok = False
    else:
        cust_email = (decision.get("customers") or {}).get("email")
        if (cust_email or "").strip().lower() != NICOLE_EMAIL:
            logger.warning("[D4] decision %s customer email is %r, expected %r — check before applying",
                            DECISION_ID, cust_email, NICOLE_EMAIL)
        already = all((decision.get(k) or None) == v for k, v in SHIP_TO_COLS.items())
        if already:
            print("  Already applied — ship-to matches Kailin Goldstein / Conroe already. Skipping.")
        elif args.apply:
            if (cust_email or "").strip().lower() != NICOLE_EMAIL:
                logger.error("[D4] refusing to write — customer email does not match %s", NICOLE_EMAIL)
                dec_ok = False
            else:
                db.table("decisions").update(SHIP_TO_COLS).eq("id", DECISION_ID).execute()
                decision = {**decision, **SHIP_TO_COLS}
                logger.info("[D4] decision %s updated", DECISION_ID)
        else:
            decision = {**decision, **SHIP_TO_COLS}  # preview only
    _print_decision("AFTER " if args.apply else "AFTER (preview)", decision)

    # ── 2. Duplicate shipment ──────────────────────────────────────────────────────────
    ship_res = db.table("shipments").select("*").eq("id", DUP_SHIPMENT_ID).execute()
    shipment = ship_res.data[0] if ship_res.data else None
    items_res = db.table("shipment_items").select("shipment_id, item_id").eq("shipment_id", DUP_SHIPMENT_ID).execute()
    items = items_res.data or []
    print("\n[2] Duplicate shipment")
    _print_shipment("BEFORE", shipment, len(items))

    if shipment is None:
        print("  Already deleted — nothing to do.")
    elif args.apply:
        if items:
            db.table("shipment_items").delete().eq("shipment_id", DUP_SHIPMENT_ID).execute()
            logger.info("[D4] deleted %d shipment_item(s) for shipment %s", len(items), DUP_SHIPMENT_ID)
        db.table("shipments").delete().eq("id", DUP_SHIPMENT_ID).execute()
        logger.info("[D4] deleted duplicate shipment %s", DUP_SHIPMENT_ID)
        _print_shipment("AFTER ", None)
    else:
        print(f"  AFTER (preview): would delete shipment {DUP_SHIPMENT_ID} and its {len(items)} shipment_item(s)")

    # ── 3. Profile recipient name (the boxes on this profile ship to Kailin) ─────────
    # backfill_recipient_refs.py recorded "Nicole Goldstein" from the decision BEFORE step 1.
    prof = db.table("customers").select("id, email, recipient_name").eq("email", NICOLE_EMAIL).execute().data or []
    print("\n[3] Profile recipient name")
    if len(prof) != 1:
        print(f"  Expected exactly 1 profile for {NICOLE_EMAIL}, found {len(prof)} — skipped")
    else:
        print(f"  BEFORE: recipient_name={prof[0].get('recipient_name')!r}")
        if prof[0].get("recipient_name") == "Kailin Goldstein":
            print("  Already applied — nothing to do.")
        elif args.apply:
            db.table("customers").update({"recipient_name": "Kailin Goldstein"}).eq("id", prof[0]["id"]).execute()
            logger.info("[D4] profile %s recipient_name → Kailin Goldstein", prof[0]["id"])
            print("  AFTER : recipient_name='Kailin Goldstein'")
        else:
            print("  AFTER (preview): recipient_name='Kailin Goldstein'")

    if not args.apply:
        print("\n(dry run — nothing written; re-run with --apply)")
        return 0
    if not dec_ok:
        print("\nApply finished with errors on step 1 — see log above.")
        return 1
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
