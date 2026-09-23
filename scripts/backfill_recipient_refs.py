"""
Threads 20/21 — D1: give every active legacy customer profile its recipient identity
(customers.recipient_ref + customers.recipient_name, migration 022) BEFORE the recipient-profile
code goes live, so incoming orders/boxes route to the right profile from day one.

  recipient_name  ← newest non-rejected decision's ship-to name
  recipient_ref   ← Cratejoy: 'cj:<subscription id>' of the newest decision tied to a real shipment
                     (read from Cratejoy: GET /v1/shipments/{id}/ → fulfillments[0].subscription_id)
                  ← Shopify:  'rc:<Recharge subscription id>' from the newest order webhook
                     (note_attributes rc_subscription_ids) for the newest decision's order_id

Skipped (printed): profiles that already have both; accounts whose recent decisions show more
than one recipient at the same time — those are split by scripts/split_recipient_profiles.py.
Only writes the two new columns, and only where they are NULL. Never touches decisions.

The app also self-heals legacy profiles at match time (app.ensure_profile_identity), so this
script is belt-and-braces — but it is the ONLY way Shopify profiles learn their Recharge id
before their next renewal, which is what keeps one person from being split in two when the
label name changes (spouse / nickname).

Dry run (default, writes nothing — prints every change):
    python scripts/backfill_recipient_refs.py
Apply:
    python scripts/backfill_recipient_refs.py --apply
Options:
    --days N     only profiles with a decision in the last N days (default 180)
"""
import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")  # importing app must not start background jobs
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

import app  # noqa: E402  (loads .env, gives get_supabase + the tested helpers)

try:
    sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to cp1252
except Exception:
    pass
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.backfill_recipient_refs")


def paged(query_fn, page: int = 1000) -> list:
    """Read every row of a PostgREST query built by query_fn() (offset paging)."""
    out, off = [], 0
    while True:
        batch = query_fn().range(off, off + page - 1).execute().data or []
        out.extend(batch)
        if len(batch) < page:
            return out
        off += page


def shopify_rc_by_order(db, since_iso: str) -> dict:
    """{shopify order id: 'rc:<id>'} from order webhooks since since_iso (newest wins).
    Keyset-paged on created_at — JSON filters on webhook_logs time out, so match in Python."""
    out: dict = {}
    cursor = since_iso
    pages = 0
    while True:
        rows = (db.table("webhook_logs")
                .select("created_at, pid:payload->>id, na:payload->note_attributes")
                .eq("source", "shopify").in_("event_type", ["orders/create", "orders/updated"])
                .gt("created_at", cursor).order("created_at").limit(1000).execute().data or [])
        pages += 1
        for r in rows:
            attrs = {str(a.get("name")): a.get("value") for a in (r.get("na") or []) if isinstance(a, dict)}
            ref = app.recipient_ref_for_shopify({"rc_subscription_ids": attrs.get("rc_subscription_ids")})
            if ref and r.get("pid"):
                out[str(r["pid"])] = ref
        logger.info("[D1] webhook scan page %d: %d rows (orders with Recharge ids so far: %d)", pages, len(rows), len(out))
        if len(rows) < 1000:
            return out
        cursor = rows[-1]["created_at"]


def cratejoy_sub_of_shipment(client: httpx.Client, ship_id: str):
    try:
        r = client.get(f"https://api.cratejoy.com/v1/shipments/{ship_id}/", headers=app._cj_basic_headers())
        if r.status_code != 200:
            logger.warning("[D1] Cratejoy shipment %s → HTTP %s", ship_id, r.status_code)
            return None
        return app._cj_sub_id_of(r.json()) or None
    except Exception as e:
        logger.warning("[D1] Cratejoy shipment %s lookup failed: %s", ship_id, e)
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--days", type=int, default=180)
    args = ap.parse_args()

    db = app.get_supabase()
    since = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
    logger.info("[D1] %s — profiles with a decision since %s", "APPLY" if args.apply else "DRY RUN", since[:10])

    decisions = paged(lambda: db.table("decisions")
                      .select("id, customer_id, status, platform, order_id, cratejoy_shipment_id, "
                              "ship_first_name, ship_last_name, created_at")
                      .gte("created_at", since).neq("status", "rejected").order("created_at", desc=True))
    by_cust = defaultdict(list)
    for d in decisions:
        by_cust[d["customer_id"]].append(d)  # newest first
    customers = {}
    ids = list(by_cust)
    cols = "id, email, first_name, last_name, recipient_ref, recipient_name"
    try:
        db.table("customers").select(cols).limit(1).execute()
    except Exception as e:
        if args.apply:
            logger.error("[D1] migration 022 is not applied (%s) — run it in Supabase first", e)
            return 1
        logger.warning("[D1] migration 022 not applied yet — previewing with empty recipient columns (dry run only)")
        cols = "id, email, first_name, last_name"
    for i in range(0, len(ids), 200):
        for c in db.table("customers").select(cols).in_("id", ids[i:i + 200]).execute().data or []:
            customers[c["id"]] = c
    logger.info("[D1] %d profiles with recent decisions", len(customers))

    # Accounts with two recipients at the same time → split script, not here
    # "At the same time" = two different recipient names on non-rejected decisions in the SAME
    # month. A name that changes over time (gift started, spouse, nickname) is one person.
    multi = set()
    for cid, ds in by_cust.items():
        per_month = defaultdict(set)
        for d in ds:
            if d.get("ship_first_name"):
                per_month[(d.get("created_at") or "")[:7]].add(
                    app.norm_name(f"{d.get('ship_first_name') or ''} {d.get('ship_last_name') or ''}"))
        if any(len(n) > 1 for n in per_month.values()):
            multi.add(cid)

    rc_map = shopify_rc_by_order(db, since) if any(
        c for c in customers.values() if not c.get("recipient_ref")) else {}

    changes, skipped_multi, no_ref = [], [], []
    taken = defaultdict(set)  # email -> refs already used
    for c in customers.values():
        if c.get("recipient_ref"):
            taken[(c.get("email") or "").lower()].add(c["recipient_ref"])
    with httpx.Client(timeout=30.0) as cj:
        for cid, c in sorted(customers.items(), key=lambda kv: (kv[1].get("email") or "")):
            if c.get("recipient_ref") and (c.get("recipient_name") or "").strip():
                continue
            if cid in multi:
                skipped_multi.append(c.get("email"))
                continue
            ds = by_cust[cid]
            upd = {}
            if not (c.get("recipient_name") or "").strip():
                named = next((d for d in ds if d.get("ship_first_name")), None)
                if named:
                    upd["recipient_name"] = " ".join(
                        f"{named.get('ship_first_name') or ''} {named.get('ship_last_name') or ''}".split())
            if not c.get("recipient_ref"):
                ref = None
                cj_dec = next((d for d in ds if d.get("platform") == "cratejoy" and d.get("cratejoy_shipment_id")), None)
                if cj_dec:
                    sub = cratejoy_sub_of_shipment(cj, cj_dec["cratejoy_shipment_id"])
                    ref = app.recipient_ref_for_cratejoy(sub)
                else:
                    sh_dec = next((d for d in ds if d.get("platform") == "shopify" and d.get("order_id")
                                   and str(d["order_id"]) in rc_map), None)
                    ref = rc_map.get(str(sh_dec["order_id"])) if sh_dec else None
                em = (c.get("email") or "").lower()
                if ref and ref not in taken[em]:
                    upd["recipient_ref"] = ref
                    taken[em].add(ref)
                elif not ref:
                    no_ref.append(c.get("email"))
            if upd:
                changes.append((cid, c.get("email"), upd))

    for cid, email, upd in changes:
        logger.info("[D1] %s %s → %s", "SET" if args.apply else "WOULD SET", email, upd)
        if args.apply:
            db.table("customers").update(upd).eq("id", cid).execute()

    print("\n══════ D1 summary ══════")
    print(f"profiles scanned           : {len(customers)}")
    print(f"profiles updated           : {len(changes)}{'' if args.apply else ' (dry run — nothing written)'}")
    print(f"  with recipient_ref       : {sum(1 for _, _, u in changes if 'recipient_ref' in u)}")
    print(f"  with recipient_name      : {sum(1 for _, _, u in changes if 'recipient_name' in u)}")
    print(f"no subscription id found   : {len(no_ref)} (fine — they match on recipient name)")
    print(f"skipped, multi-recipient   : {len(skipped_multi)} → run split_recipient_profiles.py for:")
    for e in sorted(set(skipped_multi)):
        print(f"    {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
