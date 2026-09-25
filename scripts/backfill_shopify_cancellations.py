"""
Shopify cancellation backfill (CANCELLATION_SYNC_PLAN.md §2.5).

Finds every Shopify order that was cancelled and runs it through the SAME helper the live webhook
and the daily reconcile use (app._apply_shopify_cancellation):
  - stamps decisions.order_cancelled_at on every decision of the order (migration 023)
  - closes still-PENDING boxes with an 'Auto-rejected:' reason (conditional on still being pending)
  - flags APPROVED boxes in Activity, never closes them
  - leaves SHIPPED boxes alone
Never changes customers.subscription_status.

Sources, merged by order id:
  1. webhook_logs — every stored orders/updated + orders/create payload with cancelled_at set.
     Scanned in weekly chunks: one jsonb scan across the whole table times out.
  2. Shopify's own status:cancelled search (last ~60 days; the app has no read_all_orders scope).

Run migration 023 first. Idempotent — safe to re-run.

Dry run (default, writes nothing — prints every order and what would happen):
    python scripts/backfill_shopify_cancellations.py
Apply:
    python scripts/backfill_shopify_cancellations.py --apply
Options:
    --since YYYY-MM-DD   earliest webhook_logs date to scan (default 2026-03-01)
"""
import argparse
import asyncio
import logging
import os
import sys
from collections import Counter
from datetime import date, datetime, timedelta

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")  # importing app must not start background jobs
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402  (loads .env, gives get_supabase + the shared cancellation helper)

try:
    sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to cp1252
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.backfill_shopify_cancellations")
for noisy in ("httpx", "httpcore"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


def cancelled_from_webhook_logs(db, since: date) -> dict[str, dict]:
    """Every order whose stored Shopify payload carries cancelled_at, scanned week by week."""
    found: dict[str, dict] = {}
    start = since
    end = date.today() + timedelta(days=1)
    while start < end:
        stop = min(start + timedelta(days=7), end)
        off = 0
        while True:
            try:
                rows = (db.table("webhook_logs")
                        .select("payload->id, payload->name, payload->cancelled_at, payload->cancel_reason")
                        .eq("source", "shopify").in_("event_type", ["orders/updated", "orders/create"])
                        .gte("created_at", start.isoformat()).lt("created_at", stop.isoformat())
                        .not_.is_("payload->>cancelled_at", "null")
                        .range(off, off + 499).execute().data or [])
            except Exception as e:
                logger.error("[BACKFILL] webhook_logs chunk %s..%s failed: %s", start, stop, e)
                break
            for r in rows:
                oid = str(r.get("id") or "")
                if oid and oid not in found:
                    found[oid] = {"name": r.get("name"), "cancelled_at": r.get("cancelled_at"),
                                  "cancel_reason": r.get("cancel_reason")}
            if len(rows) < 500:
                break
            off += 500
        start = stop
    logger.info("[BACKFILL] webhook_logs since %s: %d cancelled orders", since, len(found))
    return found


def cancelled_from_shopify() -> dict[str, dict]:
    if not app.shopify_fulfillment_enabled():
        logger.warning("[BACKFILL] Shopify outbound creds not set — skipping the Shopify search source")
        return {}
    try:
        found = app.get_shopify_client().get_cancelled_orders()
    except Exception as e:
        logger.error("[BACKFILL] Shopify cancelled search failed: %s", e)
        return {}
    for info in found.values():  # GraphQL returns CUSTOMER/OTHER; webhooks send customer/other
        info["cancel_reason"] = (info.get("cancel_reason") or "").lower() or None
    logger.info("[BACKFILL] Shopify status:cancelled search: %d orders", len(found))
    return found


async def run(apply: bool, since: date) -> int:
    db = app.get_supabase()
    orders = cancelled_from_webhook_logs(db, since)
    for oid, info in cancelled_from_shopify().items():
        orders.setdefault(oid, info)
    print(f"\nCancelled Shopify orders found: {len(orders)}\n")

    totals: Counter = Counter()
    for oid, info in sorted(orders.items(), key=lambda kv: str(kv[1].get("cancelled_at") or "")):
        decisions = (db.table("decisions").select("id, status, customer_id, created_at, kit_sku")
                     .eq("order_id", oid).execute().data or [])
        if not decisions:
            continue
        by_status = Counter(d["status"] for d in decisions)
        emails = set()
        for d in decisions:
            c = (db.table("customers").select("email").eq("id", d["customer_id"]).limit(1).execute().data or [])
            if c:
                emails.add(c[0]["email"])
        print(f"{info.get('name') or oid:<12} cancelled {str(info.get('cancelled_at'))[:10]}  "
              f"{dict(by_status)}  {', '.join(sorted(emails))}")
        counts = await app._apply_shopify_cancellation(
            db, oid, info.get("name"), info.get("cancelled_at"), info.get("cancel_reason"),
            "backfill", dry_run=not apply,
        )
        totals.update(counts)
        totals["orders_with_decisions"] += 1

    verb = "Closed" if apply else "Would close"
    print(f"\n{verb} {totals['closed']} pending box(es); approved flagged {totals['approved_flagged']}; "
          f"left alone {totals['left_alone']}; across {totals['orders_with_decisions']} order(s).")
    if apply:
        print(f"Stamped order_cancelled_at on {totals['stamped']} decision(s).")
    else:
        print("DRY RUN — nothing written. Re-run with --apply to write.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--since", default="2026-03-01", help="earliest webhook_logs date to scan")
    args = ap.parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()
    return asyncio.run(run(args.apply, since))


if __name__ == "__main__":
    sys.exit(main())
