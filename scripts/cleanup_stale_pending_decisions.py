"""
Reject stale duplicate PENDING decisions, keeping the newest one per customer.

WHY: every monthly cycle creates a new pending decision per customer, but old ones were
never cleared. Measured 2026-08-17: 1,936 pending across 709 customers, 543 of whom had
more than one (some 8-9), going back to May. The bulk-recurate handler skips any customer
that has another pending decision, so the backlog made recurate a no-op for most of the
list ("0 succeeded, 104 skipped").

This only ever touches DECISIONS. It never creates or edits kits, items or customers.

Dry run (default, writes nothing):
    python scripts/cleanup_stale_pending_decisions.py
Apply:
    python scripts/cleanup_stale_pending_decisions.py --apply
"""
import os
import sys
from collections import Counter, defaultdict

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

APPLY = "--apply" in sys.argv

_env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
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
    print("MODE:", "APPLY (will write)" if APPLY else "DRY RUN (no writes)")

    rows = _paginate(
        db.table("decisions")
        .select("id, customer_id, status, created_at, kit_sku, trimester, decision_type")
        .eq("status", "pending")
    )
    print("pending decisions: %d" % len(rows))

    by_cust = defaultdict(list)
    for r in rows:
        by_cust[r["customer_id"]].append(r)

    stale = []
    for cid, group in by_cust.items():
        if len(group) < 2:
            continue
        # Keep the newest; everything older is superseded by it.
        group.sort(key=lambda r: r["created_at"], reverse=True)
        stale.extend(group[1:])

    print("customers with >1 pending: %d" % sum(1 for g in by_cust.values() if len(g) > 1))
    print("stale pending to reject:   %d" % len(stale))
    print("would remain pending:      %d" % (len(rows) - len(stale)))
    print("by month created:", dict(sorted(Counter(r["created_at"][:7] for r in stale).items())))

    if not stale:
        print("nothing to do")
        return

    print("\nsample (first 10):")
    for r in stale[:10]:
        print("  cust=%s %s kit=%s T%s type=%s"
              % (r["customer_id"][:8], r["created_at"][:19], r["kit_sku"],
                 r["trimester"], r["decision_type"]))

    if not APPLY:
        print("\nDRY RUN — nothing written. Re-run with --apply to reject these.")
        return

    ids = [r["id"] for r in stale]
    done = 0
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        db.table("decisions").update({"status": "rejected"}).in_("id", chunk).execute()
        done += len(chunk)
        print("  rejected %d/%d" % (done, len(ids)))

    after = _paginate(db.table("decisions").select("id, customer_id").eq("status", "pending"))
    per = Counter()
    for r in after:
        per[r["customer_id"]] += 1
    print("\nAFTER: %d pending across %d customers; still >1: %d"
          % (len(after), len(per), sum(1 for v in per.values() if v > 1)))


if __name__ == "__main__":
    main()
