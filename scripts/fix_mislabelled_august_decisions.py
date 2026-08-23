"""
Fix decisions wrongly marked 'shipped' by finalize_august_cycle.py Phase B.

Phase B marked each affected customer's newest surviving pending decision as 'shipped'
without checking the kit matched what actually shipped. 303 of the 304 it touched have a
kit_sku that matches no August shipment for that customer — the engine suggested one kit,
Sheena manually shipped a different one, and the decision row now falsely claims the
suggested kit went out.

See FORWARD_CURATION_PLAN.md Part 1 for full background and the safety analysis.

Selection: decisions with status='shipped', updated_at in the 2026-08-22T21 burst (the
exact window finalize_august_cycle.py ran in), whose kit_sku does not match any of that
customer's shipments with ship_date in 2026-08. Those get set to 'rejected' — the engine's
suggestion was superseded, not fulfilled. kit_sku is left untouched (falsifying it to the
shipped kit would misrepresent what the engine actually suggested).

Never touches shipments, shipment_items, kits, or items.

Dry run (default, writes nothing):
    python scripts/fix_mislabelled_august_decisions.py
Apply:
    python scripts/fix_mislabelled_august_decisions.py --apply
"""
import os
import sys
from collections import Counter, defaultdict

import httpx

APPLY = "--apply" in sys.argv
BURST_PREFIX = "2026-08-22T21"   # the exact window finalize_august_cycle.py ran in

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
_env = os.path.join(_root, ".env")
if os.path.exists(_env):
    for line in open(_env, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

from supabase import create_client  # noqa: E402

db = create_client(
    os.environ["SUPABASE_URL"],
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ["SUPABASE_ANON_KEY"],
)


def _paginate(query, size=1000):
    out, off, base = [], 0, str(query.params)
    while True:
        query.params = httpx.QueryParams(base)
        batch = query.range(off, off + size - 1).execute()
        out.extend(batch.data or [])
        if len(batch.data or []) < size:
            break
        off += size
    return out


def main():
    print("MODE:", "APPLY (WILL WRITE)" if APPLY else "DRY RUN (no writes)")

    decs = _paginate(db.table("decisions").select(
        "id, customer_id, status, kit_sku, created_at, updated_at"))
    touched = [d for d in decs
               if d["status"] == "shipped" and (d.get("updated_at") or "")[:13] == BURST_PREFIX]
    print(f"decisions marked 'shipped' in the {BURST_PREFIX} burst: {len(touched)}")

    ships = _paginate(db.table("shipments").select("customer_id, kit_sku, ship_date"))
    aug_kits_by_cust = defaultdict(set)
    for s in ships:
        if (s.get("ship_date") or "")[:7] == "2026-08":
            aug_kits_by_cust[s["customer_id"]].add(s.get("kit_sku"))

    mismatched = [d for d in touched if d.get("kit_sku") not in aug_kits_by_cust.get(d["customer_id"], set())]
    matched = [d for d in touched if d not in mismatched]

    print(f"  kit matches an August shipment (leave alone) : {len(matched)}")
    print(f"  kit matches NO August shipment (fix)          : {len(mismatched)}")
    print("  decision month of the ones to fix:", dict(sorted(Counter(d['created_at'][:7] for d in mismatched).items())))

    # Sanity guard from the plan: none of these should fall in Sheena's own 15:58-16:06 window —
    # that window is a different, legitimate update (her bulk ship), not this bug.
    her_window = [d for d in mismatched if "2026-08-22T15:5" in (d.get("updated_at") or "")
                  or "2026-08-22T16:0" in (d.get("updated_at") or "")]
    if her_window:
        print(f"\n  !! {len(her_window)} candidates fall in Sheena's 15:58-16:06 window — "
              "excluding them, they are not this bug")
        mismatched = [d for d in mismatched if d not in her_window]

    print(f"\nFINAL: {len(mismatched)} decisions to set from 'shipped' to 'rejected'")

    if not APPLY:
        print("\nDRY RUN — nothing written. Re-run with --apply to execute.")
        return

    ids = [d["id"] for d in mismatched]
    for i in range(0, len(ids), 100):
        db.table("decisions").update({"status": "rejected"}).in_("id", ids[i:i + 100]).execute()
        print(f"  {min(i + 100, len(ids))}/{len(ids)} updated")

    print(f"\nDONE — {len(ids)} decisions set to rejected")


if __name__ == "__main__":
    main()
