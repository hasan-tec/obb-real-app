"""
Threads 20/21 — D3: backfill the per-box ship-to snapshot (migration 022 / B2 —
decisions.ship_address1 etc.) on OPEN decisions that predate it, so the Decisions page,
Pirate Ship / VeraCore exports and the ✓ VC / ✓ OB popups read a real box address for old
pending/approved rows instead of relying on the customer row's CURRENT (possibly since-changed)
address to still be right (golden rule 3: old rows keep working via fallback — this just makes
the snapshot itself correct once one exists).

For every decision with status in ('pending', 'approved') and ship_address1 IS NULL:
  - Shopify : newest orders/create | orders/updated webhook payload for that order_id
              (order_payload(), same window-search approach as split_recipient_profiles.py)
              → shipping_address → app.ship_to_cols_from_shopify(...)
  - Cratejoy with cratejoy_shipment_id set: GET /v1/shipments/{id}/ → ship_address
              → app.ship_to_cols_from_cratejoy(...)
  - else: left NULL (readers already fall back to the customer row — nothing to do)

Only writes the ship_* columns, and only when a real address1 was found. Never touches
decisions this script can't find a source address for, and never touches ship_to_source='manual'
rows (impossible here since ship_address1 is NULL — manual always sets an address).

Dry run (default, writes nothing — prints every change):
    python scripts/backfill_decision_ship_to.py
Apply:
    python scripts/backfill_decision_ship_to.py --apply
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")  # importing app must not start background jobs
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

import app  # noqa: E402  (loads .env, gives get_supabase + the tested helpers)

try:
    sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to cp1252
except Exception:
    pass
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.backfill_decision_ship_to")


def paged(query_fn, page: int = 1000) -> list:
    """Read every row of a PostgREST query built by query_fn() (offset paging)."""
    out, off = [], 0
    while True:
        batch = query_fn().range(off, off + page - 1).execute().data or []
        out.extend(batch)
        if len(batch) < page:
            return out
        off += page


def order_payload(db, order_id: str, near_iso: str):
    """Newest orders/create|orders/updated payload for a Shopify order, searched in a window
    around the decision's creation (JSON filters on webhook_logs time out). Same approach as
    scripts/split_recipient_profiles.py order_payload()."""
    if not order_id or not near_iso:
        return None
    t = datetime.fromisoformat(str(near_iso).replace("Z", "+00:00"))
    for pad in (timedelta(minutes=15), timedelta(days=3), timedelta(days=60)):
        rows = (db.table("webhook_logs").select("id, created_at, pid:payload->>id")
                .eq("source", "shopify").in_("event_type", ["orders/create", "orders/updated"])
                .gte("created_at", (t - pad).isoformat()).lte("created_at", (t + pad).isoformat())
                .order("created_at", desc=True).limit(1000).execute().data or [])
        hit = next((r for r in rows if str(r.get("pid")) == str(order_id)), None)
        if hit:
            return db.table("webhook_logs").select("payload").eq("id", hit["id"]).limit(1).execute().data[0]["payload"]
    return None


def cratejoy_get_shipment(client: httpx.Client, ship_id: str):
    try:
        r = client.get(f"https://api.cratejoy.com/v1/shipments/{ship_id}/", headers=app._cj_basic_headers())
        if r.status_code != 200:
            logger.warning("[D3] Cratejoy shipment %s → HTTP %s", ship_id, r.status_code)
            return None
        return r.json()
    except Exception as e:
        logger.warning("[D3] Cratejoy shipment %s lookup failed: %s", ship_id, e)
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    args = ap.parse_args()

    db = app.get_supabase()
    logger.info("[D3] %s — open decisions missing a ship-to snapshot", "APPLY" if args.apply else "DRY RUN")

    # Migration 022 tolerance — same idea as backfill_recipient_refs.py: a dry run before the
    # migration has run just previews "nothing to do yet" instead of crashing; --apply refuses.
    try:
        db.table("decisions").select("ship_address1").limit(1).execute()
    except Exception as e:
        if args.apply:
            logger.error("[D3] migration 022 is not applied (%s) — run it in Supabase first", e)
            return 1
        logger.warning("[D3] migration 022 not applied yet (%s) — nothing to preview until it runs. "
                        "Apply migrations/022_recipient_profiles_ship_to_tracking.sql, then re-run.", e)
        print("\n══════ D3 summary ══════")
        print("migration 022 not applied yet — nothing to preview.")
        return 0

    decisions = paged(lambda: db.table("decisions")
                      .select("id, customer_id, platform, order_id, cratejoy_shipment_id, status, created_at")
                      .in_("status", ["pending", "approved"]).is_("ship_address1", "null"))
    logger.info("[D3] %d open decision(s) missing a ship-to snapshot", len(decisions))

    changes: list = []
    no_source: list = []
    with httpx.Client(timeout=30.0) as cj:
        for d in decisions:
            cols = None
            if d.get("platform") == "shopify" and d.get("order_id"):
                payload = order_payload(db, d["order_id"], d.get("created_at"))
                if payload:
                    got = app.ship_to_cols_from_shopify(payload.get("shipping_address") or {}, "", "")
                    if got.get("ship_address1"):
                        cols = got
            elif d.get("platform") == "cratejoy" and d.get("cratejoy_shipment_id"):
                shipment = cratejoy_get_shipment(cj, d["cratejoy_shipment_id"])
                if shipment:
                    got = app.ship_to_cols_from_cratejoy(shipment.get("ship_address") or {})
                    if got.get("ship_address1"):
                        cols = got
            if cols:
                changes.append((d["id"], d.get("customer_id"), cols))
            else:
                no_source.append((d["id"], d.get("platform"), d.get("order_id"), d.get("cratejoy_shipment_id")))

    for did, cust_id, cols in changes:
        logger.info("[D3] %s decision=%s customer=%s → %s",
                     "SET" if args.apply else "WOULD SET", did[:8], (cust_id or "")[:8], cols)
        if args.apply:
            db.table("decisions").update(cols).eq("id", did).is_("ship_address1", "null").execute()

    print("\n══════ D3 summary ══════")
    print(f"open decisions scanned      : {len(decisions)}")
    print(f"ship-to backfilled          : {len(changes)}{'' if args.apply else ' (dry run — nothing written)'}")
    print(f"no source address found     : {len(no_source)} (left NULL — readers fall back to the customer row)")
    for did, platform, order_id, ship_id in no_source[:25]:
        print(f"    decision {did[:8]} platform={platform} order_id={order_id} cratejoy_shipment_id={ship_id}")
    if len(no_source) > 25:
        print(f"    … and {len(no_source) - 25} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
