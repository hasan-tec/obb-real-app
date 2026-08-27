"""
OBB Curation Engine — Phase 2: Monthly Curation Report
Generates the monthly curation report answering Ting's 7 key questions:
1. How many renewal customers am I shipping next month by trimester?
2. How many can stay on existing kits right now?
3. Which exact customers need a newly curated monthly kit?
4. Which items are too risky to reuse this month?
5. Which items are still reasonable candidates, oldest inventory first?
6. How many units should I ask the warehouse to build per trimester?
7. If I commit these curated items this month, what will that do to next month's safe pool?
"""

import logging
import re
from datetime import date, timedelta
from collections import defaultdict, Counter
from typing import Optional
import httpx

logger = logging.getLogger("obb.curation_report")


# ─── Config Defaults ───

DEFAULT_SHIP_DAY = 14             # 14th of the month
DEFAULT_WAREHOUSE_MIN = 100       # minimum kit build quantity
DEFAULT_LOOKBACK_MONTHS = 4       # months to look back for DO NOT USE
DEFAULT_RECENCY_MONTHS = 3        # only include customers whose last shipment was within X months
RISK_HIGH_THRESHOLD = 60.0        # legacy percentage path only (projection_engine.py) — see generate_item_risk_report
RISK_MEDIUM_THRESHOLD = 25.0      # legacy percentage path only (projection_engine.py) — see generate_item_risk_report

# ─── Kit-recipe blocking (CURATION_REBUILD_PLAN.md §12) ───
# Kit build-month is derived from age_rank, not shipment dates — see §12.3b(a).
# age_rank r corresponds to calendar month AGE_RANK_EPOCH_MONTH + (r - AGE_RANK_EPOCH_RANK).
AGE_RANK_EPOCH_MONTH = "2024-03"
AGE_RANK_EPOCH_RANK = 65
AGE_RANK_MIN_TRUSTED = 65          # ranks below this are legacy one-offs, not a reliable monthly sequence


# ─── Trimester Calculation (same as Phase 1) ───

def calc_trimester(due_date: date, ship_date: date) -> int:
    """Trimester from due/ship date. Delegates to the single source of truth in app.py
    so the formula can never drift between the engine and the monthly report.
    Lazy import avoids a circular import at module load (app imports this module)."""
    from app import calculate_trimester
    return calculate_trimester(due_date, ship_date)


# ─── Lookback Window Calculation ───

def calc_lookback_window(ship_date: date, lookback_months: int = DEFAULT_LOOKBACK_MONTHS) -> tuple[date, date]:
    """
    Calculate the lookback window for DO NOT USE generation.
    Returns (start_date, end_date) — the window of shipment dates to consider.
    """
    # Go back lookback_months from the 1st of the ship month
    start_month = ship_date.month - lookback_months
    start_year = ship_date.year
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    lookback_start = date(start_year, start_month, 1)
    # End is the last day of the month before the ship month
    lookback_end = date(ship_date.year, ship_date.month, 1) - timedelta(days=1)
    return lookback_start, lookback_end


# ─── Kit-Recipe Blocking (CURATION_REBUILD_PLAN.md §12) ───
#
# Replaces the percentage-of-group DO NOT USE calculation for the live monthly report.
# Sheena blocks at the kit-recipe level: for the trimester being curated and every
# trimester below it, block that layer's welcome kit plus its recent renewal kits.
# The window shifts back 3 months per layer below the one being curated (§12.2/§12.4b).
#
# T1 renewals get the SAME window as every other layer — Sheena's September T3 note lists
# MAY/APR/MAR in its T1 column, and her SEPT CQ31 sheet blocks BOTH BQ-11 and BP-11 because
# that window straddles BQ-11's April 2026 build. They are just not built monthly, so their
# month cannot come from age_rank (for T1 kits age_rank is only an ordering: 69/68/67/28 for
# kits built Apr-2026/Jul-2025/Apr-2025/Aug-2024). It comes from kits.build_month instead,
# and a month resolves to whichever T1 kit was current then.

def _month_idx(month_str: str) -> int:
    """Absolute month index (year*12 + month-1) for arithmetic that's rollover-free."""
    year, month = int(month_str[:4]), int(month_str[5:7])
    return year * 12 + (month - 1)


def _month_add(month_str: str, delta: int) -> str:
    """month_str shifted by delta months, e.g. _month_add('2026-01', -1) == '2025-12'."""
    idx = _month_idx(month_str) + delta
    year, month0 = divmod(idx, 12)
    return f"{year:04d}-{month0 + 1:02d}"


_T1_MONTH_RE = re.compile(
    r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]*\.?\s+(\d{4})\b", re.I
)
_MONTH_NUM = {m: i + 1 for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
)}


def t1_build_month(kit: dict) -> Optional[str]:
    """'YYYY-MM' the kit was built, or None if it cannot be determined.

    Prefers the `build_month` column (migration 021). Falls back to the month written into
    the kit name ('APR 2026 BQ-11', 'JULY 2025 - BP-11') so blocking is still correct on a
    deploy that lands before the migration is run in Supabase. The column is the durable
    answer: a name is free text, and a manual kit edit already broke blocking once
    (is_welcome_kit on BQ-11, 2026-08-21).
    """
    explicit = str(kit.get("build_month") or "").strip()
    if len(explicit) >= 7 and explicit[4] == "-" and explicit[:4].isdigit():
        return explicit[:7]
    match = _T1_MONTH_RE.search(kit.get("name") or "")
    if not match:
        return None
    return f"{int(match.group(2)):04d}-{_MONTH_NUM[match.group(1)[:3].upper()]:02d}"


def _ship_day_months_back(ship_date: date, months: int) -> date:
    """The same day-of-month as `ship_date`, `months` months earlier.

    Clamps to the last day of the target month when that month is shorter
    (ship day 31 -> February). months=0 returns ship_date itself.
    """
    if months == 0:
        return ship_date
    target = _month_add(f"{ship_date.year:04d}-{ship_date.month:02d}", -months)
    year, month = int(target[:4]), int(target[5:7])
    try:
        return date(year, month, ship_date.day)
    except ValueError:
        nxt = _month_add(target, 1)
        return date(int(nxt[:4]), int(nxt[5:7]), 1) - timedelta(days=1)


def month_to_age_rank(month_str: str) -> int:
    """The age_rank a kit built in month_str would carry. See §12.3b(a)."""
    return AGE_RANK_EPOCH_RANK + (_month_idx(month_str) - _month_idx(AGE_RANK_EPOCH_MONTH))


def _window_offsets(depth: int) -> list[int]:
    """Month offsets (relative to the cycle month) blocked for a layer at this depth.
    depth 0 = the trimester being curated itself; depth 1, 2, 3 = layers below it."""
    if depth == 0:
        return [0, -1, -2, -3]
    if depth == 1:
        return [-1, -2, -3]
    start = -(3 * depth - 2)
    return [start, start - 1, start - 2]


def load_kits_for_blocking(db) -> dict:
    """
    Load every kit (deliberately unfiltered by quantity_available / is_welcome_kit —
    a kit built in a blocked month is blocked regardless of leftover stock) and index it
    for blocked_kits(). Call once per run, not once per trimester.

    Returns {
        "by_layer_age_rank": {(trimester, age_rank): [kit, ...]},   # renewal kits only
        "welcome_by_trimester": {trimester: kit},                   # one active welcome kit per trimester
        "t1_renewal_sorted": [kit, ...],                            # T1 renewals, newest age_rank first
        "t1_timeline": [(build_month, kit), ...],                   # T1 renewals, OLDEST build month first
    }
    """
    all_kits = _paginate_all(db.table("kits").select("*"))

    by_layer_age_rank = defaultdict(list)
    welcome_candidates = defaultdict(list)
    t1_renewal = []

    for k in all_kits:
        trimester = k.get("trimester")
        if trimester is None:
            continue
        if k.get("is_welcome_kit"):
            welcome_candidates[trimester].append(k)
            continue
        age_rank = k.get("age_rank")
        if age_rank is not None:
            by_layer_age_rank[(trimester, age_rank)].append(k)
        if trimester == 1:
            t1_renewal.append(k)

    # D1 fix (plan §12.4d): tiebreak must MAXIMIZE age_rank, then quantity_available, then sku —
    # a min() over a negated key silently preferred a zero-stock duplicate SKU (e.g. picking
    # 'OBB-WK-G2 KIT' with 0 shipments over 'OBB-WK-G2 KITS' with 747, at the same age_rank).
    welcome_by_trimester = {}
    for trimester, candidates in welcome_candidates.items():
        welcome_by_trimester[trimester] = max(
            candidates,
            key=lambda k: (k.get("age_rank") or 0, k.get("quantity_available") or 0, k.get("sku") or ""),
        )

    t1_renewal_sorted = sorted(t1_renewal, key=lambda k: k.get("age_rank") or 0, reverse=True)

    # Oldest first, so "which T1 kit was current in month M" is the last entry <= M.
    t1_timeline: list[tuple[str, dict]] = []
    t1_unresolved: list[str] = []
    for kit in t1_renewal:
        build_month = t1_build_month(kit)
        if build_month:
            t1_timeline.append((build_month, kit))
        else:
            t1_unresolved.append(kit.get("sku") or "?")
    t1_timeline.sort(key=lambda pair: pair[0])
    if t1_unresolved:
        logger.error(
            "[CURATION] No build month on T1 renewal kit(s) %s — set kits.build_month "
            "(migration 021), or keep the month in the name as 'APR 2026 BQ-11'. Those kits "
            "cannot be month-blocked and are skipped.", t1_unresolved,
        )
    logger.info(
        "[CURATION] T1 renewal timeline (oldest first): %s",
        [f"{bm} {k['sku']}" for bm, k in t1_timeline] or "EMPTY — falling back to recency rank",
    )

    logger.info(
        "[CURATION] Kit-blocking index loaded: %d renewal (trimester,age_rank) buckets, "
        "%d welcome kits, %d T1 renewal kits",
        len(by_layer_age_rank), len(welcome_by_trimester), len(t1_renewal_sorted),
    )

    return {
        "by_layer_age_rank": dict(by_layer_age_rank),
        "welcome_by_trimester": welcome_by_trimester,
        "t1_renewal_sorted": t1_renewal_sorted,
        "t1_timeline": t1_timeline,
    }


def blocked_kits(trimester: int, cycle_month: str, kit_index: dict) -> set[str]:
    """
    The set of kit ids DO-NOT-USE for `trimester`, curating in `cycle_month` (e.g. "2026-09").
    Rule: CURATION_REBUILD_PLAN.md §12.2/§12.4b — for each layer L from `trimester` down to 1,
    block that layer's active welcome kit plus its renewal kits in the layer's month window
    (T1 renewals use recency rank instead of a month window — see module docstring above).
    """
    kit_ids: set[str] = set()
    t1_list = kit_index["t1_renewal_sorted"]

    for layer in range(trimester, 0, -1):
        depth = trimester - layer

        if layer == 1:
            # Same month window as every other layer; a month resolves to the T1 kit that
            # was current then (newest build_month <= that month). Curating T3 for 2026-09
            # gives MAY/APR/MAR 2026 -> BQ-11 (built Apr) and BP-11 (current through Mar),
            # which is exactly the pair on Sheena's SEPT CQ31 sheet.
            timeline = kit_index.get("t1_timeline") or []
            if timeline:
                for offset in _window_offsets(depth):
                    month = _month_add(cycle_month, offset)
                    current = None
                    for build_month, kit in timeline:
                        if build_month > month:
                            break
                        current = kit
                    if current is not None:
                        kit_ids.add(current["id"])
            elif t1_list:
                # No build month on any T1 kit (load_kits_for_blocking logs which). Fall back
                # to the old recency ladder rather than silently blocking nothing.
                kit_ids.add(t1_list[min(max(depth - 1, 0), len(t1_list) - 1)]["id"])
        else:
            for offset in _window_offsets(depth):
                month = _month_add(cycle_month, offset)
                rank = month_to_age_rank(month)
                if rank >= AGE_RANK_MIN_TRUSTED:
                    for kit in kit_index["by_layer_age_rank"].get((layer, rank), []):
                        kit_ids.add(kit["id"])

        welcome_kit = kit_index["welcome_by_trimester"].get(layer)
        if welcome_kit:
            kit_ids.add(welcome_kit["id"])

    return kit_ids


def compute_blocked_items(
    trimester: int,
    cycle_month: str,
    kit_index: dict,
    kit_items_map: dict[str, set],
    alt_map: dict[str, set],
) -> tuple[set[str], set[str]]:
    """Blocked kit ids and their item ids, expanded through registered alternatives
    (§12.4e — alternatives model physical substitutability, orthogonal to how the
    blocked set is derived, and dropping them risks under-blocking a real duplicate)."""
    kit_ids = blocked_kits(trimester, cycle_month, kit_index)
    item_ids: set[str] = set()
    for kid in kit_ids:
        item_ids.update(kit_items_map.get(kid, set()))
    expanded = set(item_ids)
    for iid in list(item_ids):
        expanded.update(alt_map.get(iid, set()))
    return expanded, kit_ids


# ─── Bulk Data Loaders ───

def _paginate_all(table_query, page_size: int = 1000) -> list:
    """Paginate through all rows of a Supabase query."""
    results = []
    offset = 0
    # Save base params before .range() mutates them (postgrest-py stacks offset/limit)
    base_params = str(table_query.params)
    while True:
        # Reset params to base before each page to avoid stacking offset/limit
        table_query.params = httpx.QueryParams(base_params)
        batch = table_query.range(offset, offset + page_size - 1).execute()
        results.extend(batch.data or [])
        if len(batch.data or []) < page_size:
            break
        offset += page_size
    return results


def load_renewal_pool(db, ship_date: date, include_paused: bool = False, recency_months: Optional[int] = None) -> list[dict]:
    """
    Get all renewal customers for the monthly report.
    - Has at least 1 shipment (renewal, not new)
    - Status: active, cancelled-prepaid, optionally paused
    - Exclude cancelled-expired
    - Must have due_date for trimester calculation
    - recency_months: if set, only include customers whose last shipment was within this many months of ship_date
    """
    logger.info(f"[CURATION] Loading renewal pool. ship_date={ship_date}, include_paused={include_paused}, recency_months={recency_months}")

    # Load all active/cancelled-prepaid customers with due dates
    valid_statuses = ["active", "cancelled-prepaid"]
    if include_paused:
        valid_statuses.append("paused")

    all_customers = []
    for status in valid_statuses:
        customers = _paginate_all(
            db.table("customers")
            .select("id, email, first_name, last_name, due_date, clothing_size, subscription_status, platform")
            .eq("subscription_status", status)
            .not_.is_("due_date", "null")
        )
        all_customers.extend(customers)

    logger.info(f"[CURATION] Total eligible customers with due_date: {len(all_customers)}")

    # Filter to renewal customers only (have at least 1 shipment)
    # Bulk check: get all customer_ids that have shipments + their latest ship_date
    shipments = _paginate_all(
        db.table("shipments").select("customer_id, ship_date")
    )

    # Build: customer_id -> latest ship_date
    latest_ship: dict[str, str] = {}
    all_ship_customer_ids: set[str] = set()
    for s in shipments:
        cid = s["customer_id"]
        sd = s.get("ship_date") or ""
        all_ship_customer_ids.add(cid)
        if sd and (cid not in latest_ship or sd > latest_ship[cid]):
            latest_ship[cid] = sd

    renewal_pool = [c for c in all_customers if c["id"] in all_ship_customer_ids]
    new_customers = [c for c in all_customers if c["id"] not in all_ship_customer_ids]

    logger.info(f"[CURATION] Raw renewal pool (before recency): {len(renewal_pool)} customers")
    logger.info(f"[CURATION] New customers (welcome kit track): {len(new_customers)}")

    # Apply recency filter if requested
    if recency_months is not None and recency_months > 0:
        cutoff_date = ship_date - timedelta(days=recency_months * 30)
        before_filter = len(renewal_pool)
        renewal_pool = [
            c for c in renewal_pool
            if latest_ship.get(c["id"], "") >= str(cutoff_date)
        ]
        excluded = before_filter - len(renewal_pool)
        logger.info(
            f"[CURATION] Recency filter: last ship >= {cutoff_date} ({recency_months} months) "
            f"→ {len(renewal_pool)} kept, {excluded} excluded as stale"
        )

    logger.info(f"[CURATION] Final renewal pool: {len(renewal_pool)} customers")

    return renewal_pool, new_customers


def decisions_exist_in_month(db, month: str) -> bool:
    """
    True if ANY decision (any status) was created in `month`. Used by run_monthly_report to
    decide whether a report month can use its own orders or must fall back to the prior
    month's (FORWARD_CURATION_PLAN.md Part 2). Deliberately status-unfiltered — a month whose
    decisions are all already rejected/shipped still counts as "happened" and must resolve to
    itself, not silently fall back a month. Only a month with ZERO decisions at all (September,
    before any order has landed) triggers the fallback.
    """
    month_start = f"{month}-01"
    month_end_excl = _month_add(month, 1) + "-01"
    hit = (
        db.table("decisions")
        .select("id")
        .gte("created_at", month_start)
        .lt("created_at", month_end_excl)
        .limit(1)
        .execute()
    )
    return bool(hit.data)


def load_renewal_pool_from_decisions(
    db, ship_date: date, report_month: str,
    pool_month: Optional[str] = None,
    include_processed: bool = False,
    new_window_start: Optional[date] = None,
    new_window_end: Optional[date] = None,
) -> tuple[list[dict], list[dict]]:
    """
    Build the monthly pool from the `decisions` table instead of shipment recency.

    WHY: the recency filter answers "who received a shipment in the last N months" —
    historical data. Curation needs "who requires a shipment this cycle". A decision row
    is created the moment a Shopify order or Cratejoy renewal lands, so `decisions` is the
    engine's native equivalent of Sheena's manual order sheet.

    Rule (deliberately minimal — see CURATION_REBUILD_PLAN.md 3.1):
      - decision created within `pool_month` (defaults to `report_month` — see
        FORWARD_CURATION_PLAN.md Part 2 for when the caller passes a different month)
      - status NOT IN (rejected, shipped), UNLESS include_processed=True, in which case every
        decision in the pool month counts — curating a future month needs the whole prior
        cycle, processed or not, since those customers are expected to renew regardless
      - customer subscription_status IN (active, cancelled-prepaid) — same filter
        load_renewal_pool() already applies by default, and the direct equivalent of
        Sheena deleting a cancellation from her sheet: cancelled-expired is precisely
        that state. Fixed 2026-08-13 (plan audit finding F2) — measured 5 cancelled-expired
        customers were reaching the pool before this filter existed.
      - customer has a due_date
      - de-duplicated by customer_id

    Trimester is always recomputed live from due_date vs ship_date by project_trimesters()
    — never the frozen decisions.trimester snapshot, which drifts as the ship date moves.
    This is what makes a forward pool useful: the same pool_month customers land in
    different trimesters depending on report_month's ship_date.

    Returns (renewal_pool, new_customers) to match load_renewal_pool()'s contract.
    """
    pool_month = pool_month or report_month
    month_start = f"{pool_month}-01"
    month_end_excl = _month_add(pool_month, 1) + "-01"

    logger.info(
        "[CURATION] Loading pool from decisions — report_month=%s pool_month=%s "
        "window=[%s, %s) ship_date=%s include_processed=%s",
        report_month, pool_month, month_start, month_end_excl, ship_date, include_processed,
    )

    decisions = _paginate_all(
        db.table("decisions")
        .select("customer_id, status, created_at, platform, order_type")
        .gte("created_at", month_start)
        .lt("created_at", month_end_excl)
    )
    logger.info("[CURATION] Decisions created in %s: %d", pool_month, len(decisions))

    if include_processed:
        actionable = decisions
        logger.info("[CURATION] include_processed=True — keeping all %d decisions regardless of status", len(actionable))
    else:
        actionable = [d for d in decisions if d.get("status") not in ("rejected", "shipped")]
        logger.info(
            "[CURATION] After status filter (excl rejected/shipped): %d kept, %d dropped",
            len(actionable), len(decisions) - len(actionable),
        )

    customers = _paginate_all(
        db.table("customers")
        .select("id, email, first_name, last_name, due_date, clothing_size, subscription_status, platform")
        .not_.is_("due_date", "null")
        .in_("subscription_status", ["active", "cancelled-prepaid"])
    )
    customers_by_id = {c["id"]: c for c in customers}

    # Dedupe keeps the first decision seen per customer. Order is whatever Postgres
    # returns (no ORDER BY), so if one customer had two decisions on different platforms
    # the retained decision_platform would be arbitrary. Measured 2026-08-12: 5 customers
    # have >1 actionable decision this month and 0 of them differ on platform, so this is
    # currently harmless — revisit if that stops being true.
    pool, seen = [], set()
    dropped_unresolved = 0
    for d in actionable:
        cid = d["customer_id"]
        if cid in seen:
            continue
        cust = customers_by_id.get(cid)
        if not cust:
            # Either the customer has a NULL due_date or the row is missing entirely.
            # Don't assert which — the query above cannot distinguish them.
            dropped_unresolved += 1
            continue
        seen.add(cid)
        # Decision platform is the order's platform — what Sheena reconciles against,
        # since she processes Shopify and Cratejoy as separate lists.
        # order_type is the order's own new/renewal flag — carried through so the
        # renewal/new split below can use it instead of guessing from shipment history.
        pool.append({
            **cust,
            "decision_platform": d.get("platform"),
            "order_type": d.get("order_type"),
        })

    logger.info(
        "[CURATION] Pool after dedupe: %d customers (%d decisions dropped — no customer row with a due_date)",
        len(pool), dropped_unresolved,
    )

    # Split renewal vs new on the ORDER's own type, not on shipment history.
    #
    # The old rule was "any shipment ever => renewal". That silently reclassified every
    # first-time customer whose box had already shipped this cycle, because shipping it
    # gave them a shipment row. Measured 2026-08-23 on the August pool: 80 of the 523
    # "renewals" were first orders (order_type='new'). Two consequences, both live:
    #   - the renewal count could not be compared against Sheena's renewals-only sheet
    #     (523 vs her 449; stripping the 80 gives 443, which matches her to within 6)
    #   - the welcome-kit watchlist read 3 new customers instead of ~80, so it was
    #     planning welcome-kit stock against a number ~25x too small
    # `decisions.order_type` is set by the Shopify/Cratejoy intake and is populated on
    # 2410 of 2485 rows; the shipment heuristic stays as the fallback for the remainder.
    shipments = _paginate_all(db.table("shipments").select("customer_id"))
    has_shipment = {s["customer_id"] for s in shipments}

    def _is_renewal(c: dict) -> bool:
        order_type = c.get("order_type")
        if order_type == "new":
            return False
        if order_type == "renewal":
            return True
        return c["id"] in has_shipment  # order_type missing — fall back to shipment history

    renewal_pool = [c for c in pool if _is_renewal(c)]
    new_customers = [c for c in pool if not _is_renewal(c)]

    unresolved = sum(1 for c in pool if c.get("order_type") not in ("new", "renewal"))
    logger.info(
        "[CURATION] Renewal/new split by order_type: %d renewal, %d new "
        "(%d had no order_type and fell back to shipment history)",
        len(renewal_pool), len(new_customers), unresolved,
    )

    # ── New customers use a ROLLING window; renewals stay on the calendar month ──
    #
    # The two cohorts genuinely behave differently. Verified against Sheena's own sheets
    # on 2026-08-23:
    #   renewals — the subscription charge is a monthly batch. 371 of the 377 rows on her
    #     August renewal sheet are dated 2026-08-01, so a calendar month IS the cohort.
    #     Every rolling window tested was materially worse (Jul15->Aug15 gave 403 against
    #     her 449; the calendar month gives 446). Do not "fix" the renewal side.
    #   new customers — sign-ups arrive daily and the welcome box goes out whenever it is
    #     next picked, so that queue is a rolling backlog straddling the month boundary.
    #     Her "new boxes sent as of the 15th" was 147; counting first orders placed in
    #     calendar August alone gives ~81, which is why the two never reconciled.
    #
    # Window is (previous ship date, this ship date] — everyone who placed a first order
    # since the last boxing run. Chosen as the business rule rather than a day-count tuned
    # to reproduce 147 exactly.
    if new_window_start is not None:
        window_start = new_window_start.isoformat()
        window_end = (new_window_end or ship_date).isoformat()
        rolling = _paginate_all(
            db.table("decisions")
            .select("customer_id, created_at, platform, order_type")
            .gt("created_at", window_start)
            .lte("created_at", window_end + "T23:59:59")
        )
        seen_new, rolling_new = set(), []
        for d in rolling:
            if d.get("order_type") != "new":
                continue
            cid = d["customer_id"]
            if cid in seen_new:
                continue
            cust = customers_by_id.get(cid)
            if not cust:
                continue
            seen_new.add(cid)
            rolling_new.append({
                **cust,
                "decision_platform": d.get("platform"),
                "order_type": d.get("order_type"),
            })
        logger.info(
            "[CURATION] Welcome-kit track on rolling window (%s, %s]: %d new customers "
            "(the calendar-month figure would have been %d)",
            window_start, window_end, len(rolling_new), len(new_customers),
        )
        new_customers = rolling_new

    platform_counts = defaultdict(int)
    for c in renewal_pool:
        platform_counts[c.get("decision_platform") or "unknown"] += 1

    logger.info(
        "[CURATION] Decisions pool: %d renewal, %d new — renewal by platform: %s",
        len(renewal_pool), len(new_customers), dict(platform_counts),
    )

    return renewal_pool, new_customers


def project_trimesters(customers: list[dict], ship_date: date) -> dict[int, list[dict]]:
    """
    Group customers by projected trimester for the ship_date.
    Returns {trimester: [customer_dicts]}
    """
    groups = defaultdict(list)
    for c in customers:
        due = date.fromisoformat(c["due_date"])
        tri = calc_trimester(due, ship_date)
        c["projected_trimester"] = tri
        groups[tri].append(c)

    for tri in sorted(groups.keys()):
        logger.info(f"[CURATION] T{tri}: {len(groups[tri])} renewal customers")

    return dict(groups)


def load_all_shipment_items_bulk(db, customer_ids: list[str], lookback_start: date, lookback_end: date) -> dict[str, set]:
    """
    Bulk-load all item receipts for a set of customers within the lookback window.
    Returns {customer_id: set(item_ids)}
    
    Performance: Uses batch pagination instead of per-customer queries.
    """
    logger.info(f"[CURATION] Loading shipment items for {len(customer_ids)} customers, window: {lookback_start} to {lookback_end}")

    # Step 1: Load ALL shipments in the lookback window (paginated)
    all_ships = _paginate_all(
        db.table("shipments")
        .select("id, customer_id")
        .gte("ship_date", str(lookback_start))
        .lte("ship_date", str(lookback_end))
    )

    # Filter to our customer set
    cust_set = set(customer_ids)
    relevant_ships = [s for s in all_ships if s["customer_id"] in cust_set]
    logger.info(f"[CURATION] Found {len(relevant_ships)} shipments in window (from {len(all_ships)} total)")

    if not relevant_ships:
        return {cid: set() for cid in customer_ids}

    # Build shipment_id → customer_id map
    ship_to_cust = {s["id"]: s["customer_id"] for s in relevant_ships}
    ship_ids = list(ship_to_cust.keys())

    # Step 2: Load ALL shipment_items for these shipments (paginated, batched by shipment IDs)
    customer_items = defaultdict(set)

    # Process in batches of 500 shipment IDs to avoid query size limits
    batch_size = 500
    for i in range(0, len(ship_ids), batch_size):
        batch_ids = ship_ids[i:i + batch_size]
        sit_rows = _paginate_all(
            db.table("shipment_items")
            .select("shipment_id, item_id")
            .in_("shipment_id", batch_ids)
        )
        for sit in sit_rows:
            cust_id = ship_to_cust.get(sit["shipment_id"])
            if cust_id:
                customer_items[cust_id].add(sit["item_id"])

    logger.info(f"[CURATION] Loaded items for {len(customer_items)} customers with shipment data in window")

    # Ensure all customer_ids have an entry (even if empty)
    for cid in customer_ids:
        if cid not in customer_items:
            customer_items[cid] = set()

    return dict(customer_items)


def load_full_customer_history_bulk(db, customer_ids: list[str]) -> dict[str, set]:
    """
    Load FULL shipment item history for customers (no date filter).
    Used for existing-kit coverage analysis (Phase 1 uses full history).
    Returns {customer_id: set(item_ids)}
    """
    logger.info(f"[CURATION] Loading FULL history for {len(customer_ids)} customers")

    # Load all shipments for these customers
    all_ships = _paginate_all(
        db.table("shipments").select("id, customer_id")
    )

    cust_set = set(customer_ids)
    relevant_ships = [s for s in all_ships if s["customer_id"] in cust_set]
    ship_to_cust = {s["id"]: s["customer_id"] for s in relevant_ships}
    ship_ids = list(ship_to_cust.keys())

    customer_items = defaultdict(set)
    batch_size = 500
    for i in range(0, len(ship_ids), batch_size):
        batch_ids = ship_ids[i:i + batch_size]
        sit_rows = _paginate_all(
            db.table("shipment_items")
            .select("shipment_id, item_id")
            .in_("shipment_id", batch_ids)
        )
        for sit in sit_rows:
            cust_id = ship_to_cust.get(sit["shipment_id"])
            if cust_id:
                customer_items[cust_id].add(sit["item_id"])

    for cid in customer_ids:
        if cid not in customer_items:
            customer_items[cid] = set()

    logger.info(f"[CURATION] Full history loaded: {len(customer_items)} customers, {sum(len(v) for v in customer_items.values())} total item records")
    return dict(customer_items)


def load_received_kit_skus_bulk(db, customer_ids: list[str]) -> dict[str, set]:
    """
    Load all kit SKUs each customer has already received.
    Returns {customer_id: set(kit_skus)}
    """
    all_ships = _paginate_all(
        db.table("shipments").select("customer_id, kit_sku")
    )

    cust_set = set(customer_ids)
    result = defaultdict(set)
    for s in all_ships:
        if s["customer_id"] in cust_set and s.get("kit_sku"):
            result[s["customer_id"]].add(s["kit_sku"])

    for cid in customer_ids:
        if cid not in result:
            result[cid] = set()

    return dict(result)


def load_item_alternatives(db) -> dict[str, set]:
    """
    Load all item alternatives (bidirectional).
    Returns {item_id: set(alternative_item_ids)}
    """
    alts = db.table("item_alternatives").select("item_id, alternative_item_id").execute()
    alt_map = defaultdict(set)
    for a in (alts.data or []):
        alt_map[a["item_id"]].add(a["alternative_item_id"])
        alt_map[a["alternative_item_id"]].add(a["item_id"])
    logger.info(f"[CURATION] Loaded {len(alts.data or [])} alternative pairs ({len(alt_map)} items with alternatives)")
    return dict(alt_map)


# ─── Existing Kit Coverage Analysis ───

def evaluate_existing_kit_coverage(
    customer: dict,
    trimester: int,
    full_history_items: set,
    received_kit_skus: set,
    available_kits: list[dict],
    kit_items_map: dict[str, set],
    alt_map: dict[str, set],
) -> dict:
    """
    For one customer: find which existing kits (with stock) are safe to assign.
    Same logic as Phase 1 assign_kit() but returns primary + alternatives.
    
    Returns: {
        "needs_new_curation": bool,
        "recommended_kit_id": str or None,
        "recommended_kit_sku": str or None,
        "alternative_kit_skus": list[str],
        "reason": str,
        "blocking_item_count": int,
    }
    """
    clothing_size = customer.get("clothing_size")

    # Build blocked items (received items + their alternatives)
    blocked_items = set(full_history_items)
    for item_id in list(full_history_items):
        alts = alt_map.get(item_id, set())
        blocked_items.update(alts)

    # Filter kits by size
    if clothing_size:
        size_to_variant = {"S": 1, "M": 2, "L": 3, "XL": 4}
        customer_variant = size_to_variant.get(clothing_size, 1)
        size_filtered = [k for k in available_kits if k.get("is_universal") or k["size_variant"] == customer_variant]
    else:
        # NULL size → universal kits ONLY (S-only kits with variant=1 should not go to unknown-size customers)
        size_filtered = [k for k in available_kits if k.get("is_universal")]

    if not size_filtered:
        return {
            "needs_new_curation": True,
            "recommended_kit_id": None,
            "recommended_kit_sku": None,
            "alternative_kit_skus": [],
            "reason": f"No kits match size {clothing_size or 'universal'} for T{trimester}",
            "blocking_item_count": len(blocked_items),
        }

    # Check each kit for duplicate items
    safe_kits = []
    for kit in size_filtered:
        if kit["sku"] in received_kit_skus:
            continue
        kit_item_ids = kit_items_map.get(kit["id"], set())
        if not kit_item_ids:
            # Kit has no items mapped — treat as safe (items not configured)
            safe_kits.append(kit)
            continue
        overlap = kit_item_ids & blocked_items
        if not overlap:
            safe_kits.append(kit)

    # Sort by age_rank (FIFO — oldest first)
    safe_kits.sort(key=lambda k: k.get("age_rank", 0))

    if not safe_kits:
        return {
            "needs_new_curation": True,
            "recommended_kit_id": None,
            "recommended_kit_sku": None,
            "alternative_kit_skus": [],
            "reason": f"All T{trimester} kits have duplicate items with customer history ({len(blocked_items)} blocked items, {len(size_filtered)} kits checked)",
            "blocking_item_count": len(blocked_items),
        }

    primary = safe_kits[0]
    alternatives = [k["sku"] for k in safe_kits[1:4]]  # Top 3 alternatives

    return {
        "needs_new_curation": False,
        "recommended_kit_id": primary["id"],
        "recommended_kit_sku": primary["sku"],
        "alternative_kit_skus": alternatives,
        "reason": f"Safe: {primary['sku']} (age_rank={primary.get('age_rank', 0)}, {len(safe_kits)} safe kits available)",
        "blocking_item_count": len(blocked_items),
        # Every safe kit for this customer, oldest first — the caller needs the full list
        # (not just the top 3 shown in the UI) to reallocate when a kit runs out of stock.
        "safe_kit_ids": [k["id"] for k in safe_kits],
    }


# ─── DO NOT USE / CAN USE Generation ───

def generate_item_risk_report(
    trimester: int,
    trimester_customers: list[dict],
    customer_items_in_window: dict[str, set],
    all_items: list[dict],
    alt_map: dict[str, set],
    cycle_month: Optional[str] = None,
    kit_index: Optional[dict] = None,
    kit_items_map: Optional[dict] = None,
) -> tuple[list[dict], list[dict]]:
    """
    Generate DO NOT USE and CAN USE item lists for a trimester.

    Two modes:
    - cycle_month given (the live monthly report): kit-recipe blocking per
      CURATION_REBUILD_PLAN.md §12 — binary, independent of customer-group size.
      Requires kit_index (load_kits_for_blocking) and kit_items_map.
    - cycle_month=None (legacy, kept for projection_engine.py's Forward Planner —
      it has no kit build-month for future months, see plan §12.4c): percentage-of-group
      blocking, unchanged from the original implementation.

    Returns: (do_not_use_list, can_use_list)
    Each item dict has: item_id, name, sku, blocked_count, group_size, blocked_pct, risk_level
    """
    group_size = len(trimester_customers)

    if cycle_month is not None:
        return _generate_item_risk_report_kit_recipe(
            trimester, group_size, cycle_month, all_items, alt_map, kit_index, kit_items_map,
        )

    if group_size == 0:
        return [], []

    customer_ids = [c["id"] for c in trimester_customers]

    # Count how many customers in this trimester received each item (in the lookback window)
    item_blocked_counts = defaultdict(int)
    for cid in customer_ids:
        items_received = customer_items_in_window.get(cid, set())
        # Also count alternatives
        expanded = set(items_received)
        for iid in list(items_received):
            expanded.update(alt_map.get(iid, set()))
        for iid in expanded:
            item_blocked_counts[iid] += 1

    # Build item lookup
    item_lookup = {i["id"]: i for i in all_items}

    # Classify each item
    do_not_use = []
    can_use = []

    today = date.today()

    for item in all_items:
        iid = item["id"]
        blocked = item_blocked_counts.get(iid, 0)
        pct = (blocked / group_size * 100) if group_size > 0 else 0

        # Check if item is expired
        expiry_raw = item.get("expiry_date")
        is_expired = False
        if expiry_raw:
            try:
                expiry_dt = date.fromisoformat(str(expiry_raw)[:10])
                if expiry_dt < today:
                    is_expired = True
            except (ValueError, TypeError):
                pass

        if is_expired:
            risk = "HIGH"
        elif pct >= RISK_HIGH_THRESHOLD:
            risk = "HIGH"
        elif pct >= RISK_MEDIUM_THRESHOLD:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        entry = {
            "item_id": iid,
            "name": item.get("name", "Unknown"),
            "sku": item.get("sku", ""),
            "blocked_count": blocked,
            "group_size": group_size,
            "blocked_pct": round(pct, 1),
            "risk_level": risk,
            "unit_cost": item.get("unit_cost"),
            "category": item.get("category"),
            "is_expired": is_expired,
            "quantity_available": item.get("quantity_available") or 0,
            "inventory_synced_at": item.get("inventory_synced_at"),
        }

        if risk in ("HIGH", "MEDIUM"):
            do_not_use.append(entry)
        else:
            can_use.append(entry)

    # Sort DO NOT USE by blocked_pct desc, name as a stable secondary key
    do_not_use.sort(key=lambda x: (-x["blocked_pct"], x["name"]))
    # Sort CAN USE by age — items don't have age_rank directly, but we use blocked_pct asc then name
    can_use.sort(key=lambda x: (x["blocked_pct"], x["name"]))

    logger.info(f"[CURATION] T{trimester}: DO NOT USE = {len(do_not_use)} items, CAN USE = {len(can_use)} items")

    return do_not_use, can_use


def _generate_item_risk_report_kit_recipe(
    trimester: int,
    group_size: int,
    cycle_month: str,
    all_items: list[dict],
    alt_map: dict[str, set],
    kit_index: Optional[dict],
    kit_items_map: Optional[dict],
) -> tuple[list[dict], list[dict]]:
    """Kit-recipe blocking path — see generate_item_risk_report's docstring.

    Binary and independent of customer-group size (plan §12.4d D2 — the empty-trimester
    early return in the legacy path is wrong here: a trimester with 0 customers this month
    still has a real block list, e.g. T1's pool is already down to single digits)."""
    if kit_index is None or kit_items_map is None:
        raise ValueError("cycle_month blocking requires kit_index and kit_items_map")

    blocked_item_ids, blocked_kit_ids = compute_blocked_items(
        trimester, cycle_month, kit_index, kit_items_map, alt_map,
    )

    do_not_use = []
    can_use = []
    today = date.today()

    for item in all_items:
        iid = item["id"]

        expiry_raw = item.get("expiry_date")
        is_expired = False
        if expiry_raw:
            try:
                expiry_dt = date.fromisoformat(str(expiry_raw)[:10])
                if expiry_dt < today:
                    is_expired = True
            except (ValueError, TypeError):
                pass

        blocked = is_expired or (iid in blocked_item_ids)
        risk = "HIGH" if blocked else "NONE"

        entry = {
            "item_id": iid,
            "name": item.get("name", "Unknown"),
            "sku": item.get("sku", ""),
            "blocked_count": group_size if blocked else 0,
            "group_size": group_size,
            "blocked_pct": 100.0 if blocked else 0.0,
            "risk_level": risk,
            "unit_cost": item.get("unit_cost"),
            "category": item.get("category"),
            "is_expired": is_expired,
            "quantity_available": item.get("quantity_available") or 0,
            "inventory_synced_at": item.get("inventory_synced_at"),
        }

        (do_not_use if blocked else can_use).append(entry)

    do_not_use.sort(key=lambda x: (-x["blocked_pct"], x["name"]))
    can_use.sort(key=lambda x: (x["blocked_pct"], x["name"]))

    logger.info(
        "[CURATION] T%d kit-recipe (%s): %d kits blocked -> DO NOT USE = %d items, CAN USE = %d items",
        trimester, cycle_month, len(blocked_kit_ids), len(do_not_use), len(can_use),
    )

    return do_not_use, can_use


# ─── Build Quantity Calculator ───

def calculate_build_quantities(
    projected_count: int,
    covered_count: int,
    warehouse_minimum: int = DEFAULT_WAREHOUSE_MIN,
) -> dict:
    """Calculate recommended kit build quantities."""
    need_new = projected_count - covered_count
    recommended_build = max(need_new, warehouse_minimum) if need_new > 0 else 0
    leftover = recommended_build - need_new if recommended_build > 0 else 0

    return {
        "projected_customers": projected_count,
        "covered_by_existing": covered_count,
        "need_new_curation": need_new,
        "recommended_build_qty": recommended_build,
        "expected_leftover": leftover,
    }


# ─── Main Report Runner ───

def run_monthly_report(
    db,
    report_month: str,                      # e.g. "2026-04"
    ship_date: Optional[date] = None,
    warehouse_minimum: int = DEFAULT_WAREHOUSE_MIN,
    include_paused: bool = False,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
    recency_months: Optional[int] = DEFAULT_RECENCY_MONTHS,
    pool_source: str = "decisions",
    include_processed: bool = True,
) -> dict:
    """
    Run the full monthly curation report.
    
    Returns a comprehensive report dict with:
    - executive_overview: per-trimester counts
    - inventory_status: existing kit availability
    - trimester_reports: {trimester: {do_not_use, can_use, customers, build_qty}}
    - welcome_watchlist: welcome kit stock status
    """
    logger.info(f"[CURATION] ═══════════════════════════════════════════════════")
    logger.info(f"[CURATION] Starting Monthly Curation Report: {report_month}")
    logger.info(f"[CURATION] ═══════════════════════════════════════════════════")

    # Parse report month
    year, month = int(report_month.split("-")[0]), int(report_month.split("-")[1])
    if ship_date is None:
        ship_date = date(year, month, DEFAULT_SHIP_DAY)
    logger.info(f"[CURATION] Ship date: {ship_date}")
    logger.info(f"[CURATION] Warehouse minimum: {warehouse_minimum}")
    logger.info(f"[CURATION] Lookback: {lookback_months} months, Include paused: {include_paused}")
    logger.info(f"[CURATION] Recency filter: {recency_months} months (None = all history)")
    logger.info(f"[CURATION] Pool source: {pool_source}")

    lookback_start, lookback_end = calc_lookback_window(ship_date, lookback_months)
    logger.info(f"[CURATION] Lookback window: {lookback_start} to {lookback_end}")

    # ── Step 1: Load renewal pool ──
    if pool_source == "decisions":
        # FORWARD_CURATION_PLAN.md Part 2: report_month drives ship_date/trimesters/blocking;
        # pool_month drives WHO is in the list, and is only different from report_month when
        # report_month itself has no orders yet (a future month, e.g. curating September while
        # still in August). Checked with an unfiltered existence query, not the actionable
        # count, so a month whose orders are all already processed still resolves to itself.
        if decisions_exist_in_month(db, report_month):
            pool_month = report_month
        else:
            pool_month = _month_add(report_month, -1)
            logger.info(
                "[CURATION] No decisions exist yet for %s — falling back to pool_month=%s",
                report_month, pool_month,
            )
        # Welcome-kit track counts first orders since the PREVIOUS boxing run, not the
        # calendar month — see the rolling-window note in load_renewal_pool_from_decisions.
        #
        # On a forward projection that window has not closed yet: curating September on
        # 27 Aug leaves (15 Aug, 15 Sep] only twelve days wide, which read as 42 new
        # customers against August's real 134 — a partial count presented as a forecast.
        # The renewal side already handles this by falling back to pool_month, so shift
        # the welcome window back by the same one cycle and both halves of the report are
        # measured on the same closed period. Current-month reports are unaffected:
        # pool_month == report_month leaves back=1, i.e. (ship-1mo, ship] as before.
        back = 2 if pool_month != report_month else 1
        window_start = _ship_day_months_back(ship_date, back)
        window_end = _ship_day_months_back(ship_date, back - 1)
        if back != 1:
            logger.info(
                "[CURATION] Forward projection (%s has no orders yet) — welcome-kit window "
                "shifted back one cycle to (%s, %s] so it matches the pool_month=%s renewals",
                report_month, window_start, window_end, pool_month,
            )
        renewal_pool, new_customers = load_renewal_pool_from_decisions(
            db, ship_date, report_month, pool_month=pool_month,
            include_processed=include_processed,
            new_window_start=window_start, new_window_end=window_end,
        )
    else:
        pool_month = report_month
        renewal_pool, new_customers = load_renewal_pool(db, ship_date, include_paused, recency_months)

    # ── Step 2: Project trimesters ──
    trimester_groups = project_trimesters(renewal_pool, ship_date)

    # ── Step 3: Load all kits with stock ──
    all_kits = db.table("kits").select("*").eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()
    kits_by_trimester = defaultdict(list)
    for k in (all_kits.data or []):
        kits_by_trimester[k["trimester"]].append(k)

    logger.info(f"[CURATION] Kits with stock > 0: {len(all_kits.data or [])} across {len(kits_by_trimester)} trimesters")

    # ── Step 4: Load kit_items map (bulk) ──
    all_kit_items = _paginate_all(
        db.table("kit_items").select("kit_id, item_id")
    )
    kit_items_map = defaultdict(set)
    for ki in all_kit_items:
        kit_items_map[ki["kit_id"]].add(ki["item_id"])
    logger.info(f"[CURATION] Kit items loaded: {len(all_kit_items)} mappings across {len(kit_items_map)} kits")

    # ── Step 5: Load item alternatives ──
    alt_map = load_item_alternatives(db)

    # ── Step 5b: Load kit-recipe blocking index (§12) — unfiltered by stock/welcome, once per run ──
    kit_index = load_kits_for_blocking(db)

    # ── Step 6: Load all items ──
    all_items = db.table("items").select("*").order("name").execute()
    all_items_list = all_items.data or []
    logger.info(f"[CURATION] Total items in DB: {len(all_items_list)}")

    # ── Step 7: Bulk-load customer histories ──
    all_renewal_ids = [c["id"] for c in renewal_pool]

    # Full history for existing-kit coverage (Phase 1 rule)
    full_history = load_full_customer_history_bulk(db, all_renewal_ids)

    # Lookback window history for DO NOT USE (Phase 2 rule)
    window_history = load_all_shipment_items_bulk(db, all_renewal_ids, lookback_start, lookback_end)

    # Received kit SKUs for duplicate kit check
    received_kits = load_received_kit_skus_bulk(db, all_renewal_ids)

    # ── Step 8: Per-trimester analysis ──
    trimester_reports = {}

    for tri in [1, 2, 3, 4]:
        tri_customers = trimester_groups.get(tri, [])
        tri_kits = kits_by_trimester.get(tri, [])

        logger.info(f"[CURATION] ── T{tri}: {len(tri_customers)} customers, {len(tri_kits)} kits with stock ──")

        # 8a: Evaluate existing kit coverage for each customer
        customer_results = []
        covered_count = 0

        for cust in tri_customers:
            cust_full_items = full_history.get(cust["id"], set())
            cust_received_skus = received_kits.get(cust["id"], set())

            result = evaluate_existing_kit_coverage(
                customer=cust,
                trimester=tri,
                full_history_items=cust_full_items,
                received_kit_skus=cust_received_skus,
                available_kits=tri_kits,
                kit_items_map=kit_items_map,
                alt_map=alt_map,
            )

            customer_results.append({
                "customer_id": cust["id"],
                "email": cust.get("email"),
                "first_name": cust.get("first_name"),
                "last_name": cust.get("last_name"),
                "clothing_size": cust.get("clothing_size"),
                "platform": cust.get("platform"),
                "projected_trimester": tri,
                **result,
            })

        # 8a2: Allocate kits against real stock.
        # evaluate_existing_kit_coverage() answers "which kits COULD this customer receive"
        # per-customer, with no knowledge of the other customers, so a kit with 2 units in
        # stock was being recommended to every customer whose history pointed at it.
        # Measured 2026-08-15: BT-32 (stock 4) recommended to 45 customers, BQ-41 (stock 26)
        # to 34, BP-41 (stock 2) to 9. An aggregate covered<=total_stock cap does NOT catch
        # this — the trimester total is comfortably under budget while individual kits are
        # exhausted and a big recent kit sits idle.
        #
        # Fix: one pass holding remaining stock, assigning each customer their oldest safe
        # kit that still has units and falling through to their next safe kit when it runs
        # out. Keeps the existing oldest-first (age_rank) rule — it just makes it aware of
        # what physically exists.
        #
        # Customers are processed fewest-safe-kits-first so a scarce kit is not consumed by
        # someone who had other options while a customer with only that one option falls
        # through to "needs new curation". Tie-break on customer_id purely for determinism.
        # NOTE: this ordering is an engine default chosen to maximise coverage, not a stated
        # business rule — if Sheena has a real priority order, it belongs here.
        kit_by_id = {k["id"]: k for k in tri_kits}
        remaining_stock = {k["id"]: (k.get("quantity_available") or 0) for k in tri_kits}

        allocatable = [r for r in customer_results if not r["needs_new_curation"]]
        allocatable.sort(key=lambda r: (len(r.get("safe_kit_ids") or []), r["customer_id"]))

        covered_count = 0
        reallocated = 0
        exhausted = 0
        for res in allocatable:
            original_kit_id = res.get("recommended_kit_id")
            for kit_id in (res.get("safe_kit_ids") or []):
                if remaining_stock.get(kit_id, 0) > 0:
                    remaining_stock[kit_id] -= 1
                    kit = kit_by_id.get(kit_id, {})
                    res["recommended_kit_id"] = kit_id
                    res["recommended_kit_sku"] = kit.get("sku")
                    if kit_id != original_kit_id:
                        reallocated += 1
                        res["reason"] = (
                            f"Safe: {kit.get('sku')} (reassigned — earlier safe kits out of stock)"
                        )
                    # Recompute alternatives against what's actually left, excluding the kit
                    # just assigned — otherwise a reassigned customer shows the same SKU as both
                    # their recommendation and their alternative, which reads as a bug on screen.
                    res["alternative_kit_skus"] = [
                        kit_by_id[alt_id]["sku"]
                        for alt_id in (res.get("safe_kit_ids") or [])
                        if alt_id != kit_id and remaining_stock.get(alt_id, 0) > 0
                    ][:3]
                    covered_count += 1
                    break
            else:
                # Every kit this customer could safely receive is out of stock.
                exhausted += 1
                res["needs_new_curation"] = True
                res["recommended_kit_id"] = None
                res["recommended_kit_sku"] = None
                res["alternative_kit_skus"] = []
                res["reason"] = (
                    f"All {len(res.get('safe_kit_ids') or [])} safe T{tri} kits are out of stock"
                )

        logger.info(
            "[CURATION] T%d stock-aware allocation: %d covered, %d reassigned to a later safe kit, "
            "%d had every safe kit exhausted (total stock %d)",
            tri, covered_count, reallocated, exhausted,
            sum(k.get("quantity_available") or 0 for k in tri_kits),
        )

        # 8b: Generate DO NOT USE / CAN USE — kit-recipe blocking (§12), binary
        do_not_use, can_use = generate_item_risk_report(
            trimester=tri,
            trimester_customers=tri_customers,
            customer_items_in_window=window_history,
            all_items=all_items_list,
            alt_map=alt_map,
            cycle_month=report_month,
            kit_index=kit_index,
            kit_items_map=kit_items_map,
        )

        # 8c: Build quantity
        build_qty = calculate_build_quantities(
            projected_count=len(tri_customers),
            covered_count=covered_count,
            warehouse_minimum=warehouse_minimum,
        )

        logger.info(f"[CURATION] T{tri} results: {covered_count} covered, {len(tri_customers) - covered_count} need new curation, build qty = {build_qty['recommended_build_qty']}")

        # 8d: Inventory status — existing kits in FIFO order
        inventory_status = []
        for kit in tri_kits:
            inventory_status.append({
                "sku": kit["sku"],
                "quantity_available": kit.get("quantity_available", 0),
                "age_rank": kit.get("age_rank", 0),
                "is_universal": kit.get("is_universal", False),
                "size_variant": kit.get("size_variant"),
            })

        trimester_reports[tri] = {
            "customers": customer_results,
            "covered_count": covered_count,
            "needs_new_count": len(tri_customers) - covered_count,
            "do_not_use": do_not_use,
            "can_use": can_use,
            "build_qty": build_qty,
            "inventory_status": inventory_status,
        }

    # ── Step 9: Welcome kit watchlist ──
    welcome_kits = db.table("kits").select("sku, trimester, quantity_available, age_rank").eq("is_welcome_kit", True).gt("quantity_available", 0).order("age_rank").execute()
    welcome_watchlist = {
        "total_stock": sum(k.get("quantity_available", 0) for k in (welcome_kits.data or [])),
        "kits": welcome_kits.data or [],
        "new_customer_count": len(new_customers),
    }
    logger.info(f"[CURATION] Welcome watchlist: {welcome_watchlist['total_stock']} units, {len(new_customers)} new customers waiting")

    # ── Step 10: Build executive overview ──
    executive = {
        "report_month": report_month,
        "ship_date": str(ship_date),
        "generated_at": str(date.today()),
        "warehouse_minimum": warehouse_minimum,
        "lookback_months": lookback_months,
        "lookback_window": f"{lookback_start} to {lookback_end}",
        "total_renewal_customers": len(renewal_pool),
        "total_new_customers": len(new_customers),
        "recency_months": recency_months,
        "pool_source": pool_source,
        # FORWARD_CURATION_PLAN.md Part 2 — surfaced explicitly so nobody has to infer where
        # the pool came from. When pool_month != report_month this is a forward projection:
        # last cycle's customers, trimesters recomputed at THIS month's ship date.
        "pool_month": pool_month,
        "pool_is_forward_projection": pool_month != report_month,
        "include_processed": include_processed,
        # Sheena curates Shopify and Cratejoy as separate lists, so a combined total
        # cannot be compared against her manual counts. Break it down per platform.
        "renewal_by_platform": dict(sorted(
            Counter(c.get("decision_platform") or c.get("platform") or "unknown"
                    for c in renewal_pool).items()
        )),
        "trimesters": {},
    }

    for tri in [1, 2, 3, 4]:
        tr = trimester_reports.get(tri, {})
        tri_custs = trimester_groups.get(tri, [])
        executive["trimesters"][tri] = {
            "projected_customers": len(tri_custs),
            "covered_by_existing": tr.get("covered_count", 0),
            "needs_new_curation": tr.get("needs_new_count", 0),
            "recommended_build_qty": tr.get("build_qty", {}).get("recommended_build_qty", 0),
            "expected_leftover": tr.get("build_qty", {}).get("expected_leftover", 0),
            "do_not_use_count": len(tr.get("do_not_use", [])),
            "can_use_count": len(tr.get("can_use", [])),
            # Same Shopify/Cratejoy split the headline already carries, but per trimester.
            # Sheena's T1-T4 counts are Shopify-only, so a combined per-trimester number
            # cannot be reconciled against her sheet — this is the line that can.
            "by_platform": dict(sorted(Counter(
                c.get("decision_platform") or c.get("platform") or "unknown"
                for c in tri_custs
            ).items())),
        }

    logger.info(f"[CURATION] ═══════════════════════════════════════════════════")
    logger.info(f"[CURATION] Report complete: {report_month}")
    for tri in [1, 2, 3, 4]:
        ex = executive["trimesters"][tri]
        logger.info(f"[CURATION]   T{tri}: {ex['projected_customers']} customers | {ex['covered_by_existing']} covered | {ex['needs_new_curation']} need new | build {ex['recommended_build_qty']}")
    logger.info(f"[CURATION] ═══════════════════════════════════════════════════")

    return {
        "executive": executive,
        "trimester_reports": trimester_reports,
        "welcome_watchlist": welcome_watchlist,
    }
