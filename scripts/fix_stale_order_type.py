"""
Re-stamp decisions.order_type where it disagrees with the customer's real shipment history.

WHY IT DRIFTS: _compute_order_type() (app.py) decides new-vs-renewal AT INGEST, from
"does this customer have a shipment with ship_date set and before today". That is the
right call at ingest — it stops the value flipping New->Renewal the moment staff approve
a decision. But it freezes: if a customer's history is imported LATER (as 444 July
shipments were, back-filled during August 2026), the decision keeps the stamp it got
when we still thought they were new.

Measured 2026-08-23 on the August pool:
  - 3 Cratejoy customers stamped 'new' whose July 15 shipment was imported after their
    Aug 1 decision. All three are T4. They belong in the renewal count.
  - 13 Shopify decisions from 2026-08-11 with order_type NULL (a separate ingest gap).
    These are duplicate decisions for customers already in the pool, so they do not move
    any count — re-stamping them is hygiene, not a fix.

Correct rule, applied here: a decision is 'renewal' if the customer has any shipment with
ship_date strictly BEFORE that decision's own created_at date, else 'new'. This is
_compute_order_type's own definition, evaluated against history as it stands now rather
than as it stood at ingest.

Only rows where stamped != computed are written. Never touches shipments, customers,
kits, or any other decision column.

Dry run (default, writes nothing):
    python scripts/fix_stale_order_type.py
    python scripts/fix_stale_order_type.py --month 2026-08
Apply:
    python scripts/fix_stale_order_type.py --month 2026-08 --apply
    python scripts/fix_stale_order_type.py --apply          # all months
"""
import logging
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.fix_order_type")


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


def _month_bounds(month: str) -> tuple[str, str]:
    year, mon = int(month[:4]), int(month[5:7])
    nxt = f"{year + 1:04d}-01-01" if mon == 12 else f"{year:04d}-{mon + 1:02d}-01"
    return f"{month}-01", nxt


def main() -> int:
    apply = "--apply" in sys.argv
    month = None
    if "--month" in sys.argv:
        month = sys.argv[sys.argv.index("--month") + 1]

    _load_env()
    from supabase import create_client
    from curation_report import _paginate_all

    db = create_client(
        os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )

    ships = _paginate_all(db.table("shipments").select("customer_id, ship_date"))
    prior: dict[str, list[str]] = defaultdict(list)
    for s in ships:
        day = (s.get("ship_date") or "")[:10]
        if day:
            prior[s["customer_id"]].append(day)
    logger.info(
        "[FIX ORDER_TYPE] loaded %d shipments covering %d customers",
        len(ships), len(prior),
    )

    query = db.table("decisions").select("id, customer_id, order_type, platform, created_at")
    if month:
        start, end = _month_bounds(month)
        query = query.gte("created_at", start).lt("created_at", end)
        logger.info("[FIX ORDER_TYPE] scope: decisions created in %s", month)
    else:
        logger.info("[FIX ORDER_TYPE] scope: ALL decisions")
    decisions = _paginate_all(query)
    logger.info("[FIX ORDER_TYPE] decisions in scope: %d", len(decisions))

    wrong = []
    for d in decisions:
        decision_day = (d.get("created_at") or "")[:10]
        if not decision_day:
            continue
        should = "renewal" if any(
            day < decision_day for day in prior.get(d["customer_id"], [])
        ) else "new"
        if d.get("order_type") != should:
            wrong.append((d, should))

    if not wrong:
        logger.info("[FIX ORDER_TYPE] nothing to repair — every stamp matches history")
        return 0

    transitions = Counter(
        f"{d.get('order_type')} -> {should}" for d, should in wrong
    )
    logger.info(
        "[FIX ORDER_TYPE] %d of %d decisions disagree with history",
        len(wrong), len(decisions),
    )
    for label, count in sorted(transitions.items()):
        logger.info("[FIX ORDER_TYPE]   %-22s %d", label, count)
    logger.info(
        "[FIX ORDER_TYPE]   by platform: %s",
        dict(Counter(d.get("platform") for d, _ in wrong)),
    )
    logger.info(
        "[FIX ORDER_TYPE]   by month: %s",
        dict(sorted(Counter((d["created_at"] or "")[:7] for d, _ in wrong).items())),
    )

    # The only transition that moves a report number is new <-> renewal; NULL -> * is
    # backfill. Call it out explicitly so the impact is never a surprise.
    moves = [(d, s) for d, s in wrong if d.get("order_type") in ("new", "renewal")]
    logger.info(
        "[FIX ORDER_TYPE]   of those, %d change an existing new/renewal answer "
        "(the rest are NULL backfill)", len(moves),
    )

    if not apply:
        logger.info("[FIX ORDER_TYPE] DRY RUN — re-run with --apply to write.")
        return 0

    updated = 0
    for d, should in wrong:
        db.table("decisions").update({"order_type": should}).eq("id", d["id"]).execute()
        updated += 1
    logger.info("[FIX ORDER_TYPE] wrote %d rows", updated)

    # Read back and re-verify against the same rule.
    recheck = _paginate_all(
        db.table("decisions")
        .select("id, customer_id, order_type, created_at")
        .in_("id", [d["id"] for d, _ in wrong][:200])
    )
    still_wrong = 0
    for d in recheck:
        decision_day = (d.get("created_at") or "")[:10]
        should = "renewal" if any(
            day < decision_day for day in prior.get(d["customer_id"], [])
        ) else "new"
        if d.get("order_type") != should:
            still_wrong += 1
    if still_wrong:
        logger.error("[FIX ORDER_TYPE] %d rows still wrong after write", still_wrong)
        return 1

    logger.info(
        "[FIX ORDER_TYPE] verified — all re-read rows now match history. "
        "Regenerate the curation report to pick this up."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
