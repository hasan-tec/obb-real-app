"""
Fill decisions.shipped_at (migration 024) for boxes shipped before it existed, so the "Shipped on"
date filter also works for past days.

Source of the date, in order:
  1. the shipment the engine wrote for that decision (notes mention "decision <first 8 of id>"),
  2. a shipment for the same customer + order id,
  3. otherwise the decision's own updated_at (approximate; reported separately).
The date is stored as 12:00 UTC on that day, so it lands on the same calendar day for viewers
anywhere from UTC-11 to UTC+11. Only rows with status 'shipped' and shipped_at empty are touched,
so a re-run is a no-op.

Dry run (default):  python scripts/backfill_shipped_at.py
Apply:              python scripts/backfill_shipped_at.py --apply      (Hasan only)
"""
import logging
import os
import re
import sys
from collections import Counter, defaultdict

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
logging.basicConfig(level=logging.WARNING)

DECISION_REF = re.compile(r"decision ([0-9a-f]{8})", re.I)


def _all(db, table: str, cols: str, **eq) -> list:
    out, off = [], 0
    while True:
        q = db.table(table).select(cols)
        for k, v in eq.items():
            q = q.eq(k, v)
        batch = q.range(off, off + 999).execute().data or []
        out += batch
        if len(batch) < 1000:
            return out
        off += 1000


def pick_ship_day(d: dict, by_ref: dict, by_order: dict) -> tuple[str | None, str]:
    """(YYYY-MM-DD, source) for one shipped decision."""
    hit = by_ref.get((d["customer_id"], d["id"][:8].lower()))
    if hit:
        return hit, "shipment_for_decision"
    hit = by_order.get((d["customer_id"], str(d.get("order_id") or "")))
    if hit and d.get("order_id"):
        return hit, "shipment_same_order"
    if d.get("updated_at"):
        return str(d["updated_at"])[:10], "updated_at_fallback"
    return None, "no_date"


def main() -> int:
    apply = "--apply" in sys.argv
    db = app.get_supabase()
    decs = [d for d in _all(db, "decisions", "id, customer_id, order_id, updated_at, shipped_at", status="shipped")
            if not d.get("shipped_at")]
    ships = _all(db, "shipments", "customer_id, order_id, ship_date, notes")
    by_ref, by_order = {}, {}
    for s in ships:
        if not s.get("ship_date"):
            continue
        for ref in DECISION_REF.findall(s.get("notes") or ""):
            by_ref.setdefault((s["customer_id"], ref.lower()), s["ship_date"])
        if s.get("order_id"):
            prev = by_order.get((s["customer_id"], str(s["order_id"])))
            by_order[(s["customer_id"], str(s["order_id"]))] = max(prev or "", s["ship_date"])

    plan, sources = defaultdict(list), Counter()
    for d in decs:
        day, src = pick_ship_day(d, by_ref, by_order)
        sources[src] += 1
        if day:
            plan[f"{day}T12:00:00+00:00"].append(d["id"])
    print(f"{'APPLY' if apply else 'DRY RUN'}: {len(decs)} shipped decisions without shipped_at")
    print("  date source:", dict(sources))
    print("  distinct days:", len(plan), "| latest:", sorted(plan)[-3:] if plan else [])
    if not apply:
        print("  (dry run — nothing written; re-run with --apply)")
        return 0
    done = 0
    for stamp, ids in plan.items():
        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            db.table("decisions").update({"shipped_at": stamp}).in_("id", chunk).is_("shipped_at", "null").execute()
            done += len(chunk)
    print(f"  written: {done}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
