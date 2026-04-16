"""
OBB Curation Engine — Phase 2: Forward Planner (Projection Engine)
Projects the monthly curation report into the future (3, 4, or 6 months).
Answers Ting's question: "If I commit these curated items this month,
what will that do to next month's safe pool?"

Two modes:
  Mode A (Forecast) — No committed items; projects status quo forward.
  Mode B (Committed) — Items locked in for a month; propagated into future months.
"""

import logging
from datetime import date, timedelta
from collections import defaultdict
from typing import Optional

from curation_report import (
    calc_trimester,
    calc_lookback_window,
    _paginate_all,
    load_renewal_pool,
    project_trimesters,
    load_all_shipment_items_bulk,
    load_full_customer_history_bulk,
    load_received_kit_skus_bulk,
    load_item_alternatives,
    evaluate_existing_kit_coverage,
    generate_item_risk_report,
    calculate_build_quantities,
    DEFAULT_SHIP_DAY,
    DEFAULT_WAREHOUSE_MIN,
    DEFAULT_LOOKBACK_MONTHS,
    DEFAULT_RECENCY_MONTHS,
    RISK_HIGH_THRESHOLD,
    RISK_MEDIUM_THRESHOLD,
)

logger = logging.getLogger("obb.forward_planner")

# ─── Warning Thresholds ───
SAFE_ITEMS_MIN_THRESHOLD = 8     # warn if safe item pool < 8 (kit needs ~7-8 items)
WELCOME_KIT_SAFETY_FACTOR = 2    # warn if welcome stock < new_customers * 2


def _next_month(year: int, month: int) -> tuple[int, int]:
    """Advance to next month, handling year rollover."""
    if month == 12:
        return year + 1, 1
    return year, month + 1


def load_committed_items(db, months: list[str]) -> dict[str, dict[int, set]]:
    """
    Load committed items from DB for given months.
    Returns: {report_month: {trimester: set(item_ids)}}
    """
    if not months:
        return {}

    result = defaultdict(lambda: defaultdict(set))

    for month_str in months:
        rows = db.table("curation_committed_items") \
            .select("trimester, item_id") \
            .eq("report_month", month_str) \
            .execute()

        for row in (rows.data or []):
            result[month_str][row["trimester"]].add(row["item_id"])

    loaded_count = sum(
        sum(len(items) for items in tri_map.values())
        for tri_map in result.values()
    )
    logger.info(f"[PLANNER] Loaded {loaded_count} committed items across {len(result)} months")
    return dict(result)


def _estimate_kit_coverage_for_month(
    tri_customers: list[dict],
    tri_kits: list[dict],
    full_history: dict[str, set],
    received_kits: dict[str, set],
    kit_items_map: dict[str, set],
    alt_map: dict[str, set],
    extra_blocked: set = None,
) -> tuple[int, int]:
    """
    Estimate how many customers are covered by existing kits for a single trimester.
    Returns (covered_count, needs_new_count)

    extra_blocked: additional item IDs to treat as blocked for ALL customers
                   (from committed items in prior months).
    """
    if not tri_customers or not tri_kits:
        return 0, len(tri_customers)

    covered = 0
    for cust in tri_customers:
        cust_full_items = full_history.get(cust["id"], set())
        cust_received_skus = received_kits.get(cust["id"], set())

        # Merge extra blocked items if any
        if extra_blocked:
            cust_full_items = cust_full_items | extra_blocked

        result = evaluate_existing_kit_coverage(
            customer=cust,
            trimester=cust.get("projected_trimester", 4),
            full_history_items=cust_full_items,
            received_kit_skus=cust_received_skus,
            available_kits=tri_kits,
            kit_items_map=kit_items_map,
            alt_map=alt_map,
        )
        if not result["needs_new_curation"]:
            covered += 1

    return covered, len(tri_customers) - covered


def _estimate_item_risk_for_month(
    trimester: int,
    tri_customers: list[dict],
    window_history: dict[str, set],
    all_items_list: list[dict],
    alt_map: dict[str, set],
    extra_blocked: set = None,
) -> tuple[int, int]:
    """
    Estimate DO NOT USE / CAN USE counts for a trimester/month.
    Returns (do_not_use_count, can_use_count)

    extra_blocked: items committed in prior months that should be added
                   to every customer's blocked set.
    """
    if not tri_customers:
        return 0, len(all_items_list)

    # If we have extra blocked items, inject them into window_history copies
    if extra_blocked:
        augmented = {}
        for cust in tri_customers:
            cid = cust["id"]
            original = window_history.get(cid, set())
            augmented[cid] = original | extra_blocked
        effective_history = augmented
    else:
        effective_history = window_history

    do_not_use, can_use = generate_item_risk_report(
        trimester=trimester,
        trimester_customers=tri_customers,
        customer_items_in_window=effective_history,
        all_items=all_items_list,
        alt_map=alt_map,
    )
    return len(do_not_use), len(can_use)


def _generate_warnings(months_data: dict, welcome_watchlist: dict) -> list[dict]:
    """Generate warnings across all projected months."""
    warnings = []

    for month_str, mdata in months_data.items():
        for tri in [1, 2, 3, 4]:
            tri_data = mdata.get("trimesters", {}).get(tri, {})
            if not tri_data:
                continue

            # Warning: Safe item pool too low
            can_use = tri_data.get("can_use_count", 0)
            if can_use < SAFE_ITEMS_MIN_THRESHOLD and tri_data.get("projected_customers", 0) > 0:
                warnings.append({
                    "month": month_str,
                    "type": "low_safe_items",
                    "trimester": tri,
                    "severity": "critical" if can_use < 4 else "warning",
                    "detail": f"Only {can_use} safe items remain for T{tri} (need ~7-8 per kit)",
                })

            # Warning: Kit stock depleted
            kit_stock = tri_data.get("kit_stock_remaining", 0)
            projected = tri_data.get("projected_customers", 0)
            if kit_stock == 0 and projected > 0:
                warnings.append({
                    "month": month_str,
                    "type": "kit_stock_depleted",
                    "trimester": tri,
                    "severity": "warning",
                    "detail": f"T{tri} pre-built kit stock is exhausted — all {projected} customers need new curation",
                })

            # Warning: High needs-new ratio
            needs_new = tri_data.get("needs_new_curation", 0)
            if projected > 0 and needs_new / projected > 0.8:
                warnings.append({
                    "month": month_str,
                    "type": "high_needs_new",
                    "trimester": tri,
                    "severity": "warning",
                    "detail": f"T{tri}: {needs_new}/{projected} ({needs_new/projected*100:.0f}%) need new curation",
                })

    # Warning: Welcome kit stock
    if welcome_watchlist:
        total_stock = welcome_watchlist.get("total_stock", 0)
        new_per_month = welcome_watchlist.get("new_customer_count", 0)
        if new_per_month > 0 and total_stock < new_per_month * WELCOME_KIT_SAFETY_FACTOR:
            warnings.append({
                "month": "all",
                "type": "welcome_low_stock",
                "trimester": 0,
                "severity": "warning",
                "detail": f"Welcome kit stock ({total_stock}) may not cover {new_per_month} new customers/month (need ~{new_per_month * WELCOME_KIT_SAFETY_FACTOR})",
            })

    logger.info(f"[PLANNER] Generated {len(warnings)} warnings")
    return warnings


def project_forward(
    db,
    base_month: str,
    ship_day: int = DEFAULT_SHIP_DAY,
    horizon_months: int = 3,
    warehouse_minimum: int = DEFAULT_WAREHOUSE_MIN,
    include_paused: bool = False,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
    recency_months: Optional[int] = DEFAULT_RECENCY_MONTHS,
) -> dict:
    """
    Main forward planner function.
    Projects the curation landscape over `horizon_months` starting from `base_month`.

    Returns dict with per-month projections, warnings, and committed items.
    """
    logger.info(f"[PLANNER] ═══════════════════════════════════════════════════")
    logger.info(f"[PLANNER] Starting Forward Projection: {base_month}, horizon={horizon_months}")
    logger.info(f"[PLANNER] Ship day={ship_day}, wh_min={warehouse_minimum}, lookback={lookback_months}, recency={recency_months}")
    logger.info(f"[PLANNER] ═══════════════════════════════════════════════════")

    base_year, base_mo = int(base_month.split("-")[0]), int(base_month.split("-")[1])

    # Build list of months to project
    month_list = []
    y, m = base_year, base_mo
    for _ in range(horizon_months):
        month_list.append(f"{y}-{m:02d}")
        y, m = _next_month(y, m)

    logger.info(f"[PLANNER] Projecting months: {month_list}")

    # ── Load committed items for all months ──
    committed = load_committed_items(db, month_list)

    # ── Load the customer pool ONCE (same pool, re-projected per month) ──
    base_ship_date = date(base_year, base_mo, ship_day)
    renewal_pool, new_customers = load_renewal_pool(db, base_ship_date, include_paused, recency_months)
    all_renewal_ids = [c["id"] for c in renewal_pool]

    logger.info(f"[PLANNER] Renewal pool: {len(renewal_pool)}, New: {len(new_customers)}")

    # ── Load shared data ONCE ──
    # Kits with stock (will be depleted month by month)
    all_kits_raw = db.table("kits").select("*").eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()
    all_kits = all_kits_raw.data or []
    logger.info(f"[PLANNER] Total kits with stock: {len(all_kits)}")

    # Kit items map
    all_kit_items = _paginate_all(db.table("kit_items").select("kit_id, item_id"))
    kit_items_map = defaultdict(set)
    for ki in all_kit_items:
        kit_items_map[ki["kit_id"]].add(ki["item_id"])

    # Item alternatives
    alt_map = load_item_alternatives(db)

    # All items
    all_items_raw = db.table("items").select("*").order("name").execute()
    all_items_list = all_items_raw.data or []

    # Full customer history (loaded once, reused)
    full_history = load_full_customer_history_bulk(db, all_renewal_ids)
    received_kits = load_received_kit_skus_bulk(db, all_renewal_ids)

    # Welcome kit watchlist
    welcome_kits = db.table("kits").select("sku, trimester, quantity_available, age_rank") \
        .eq("is_welcome_kit", True).gt("quantity_available", 0).order("age_rank").execute()
    welcome_watchlist = {
        "total_stock": sum(k.get("quantity_available", 0) for k in (welcome_kits.data or [])),
        "kits": welcome_kits.data or [],
        "new_customer_count": len(new_customers),
    }

    # ── Track kit stock depletion across months ──
    # Deep copy kit quantities so we can subtract
    kit_stock_remaining = {k["id"]: k["quantity_available"] for k in all_kits}

    # ── Track cumulative committed items per trimester (propagated forward) ──
    cumulative_committed = defaultdict(set)  # {trimester: set(item_ids)}

    # ── Project each month ──
    months_data = {}

    for month_idx, month_str in enumerate(month_list):
        m_year, m_month = int(month_str.split("-")[0]), int(month_str.split("-")[1])
        ship_date = date(m_year, m_month, ship_day)

        logger.info(f"[PLANNER] ── Month {month_idx + 1}/{horizon_months}: {month_str} (ship={ship_date}) ──")

        # Re-project trimesters with this month's ship_date
        trimester_groups = project_trimesters(renewal_pool, ship_date)

        # Lookback window for this month
        lookback_start, lookback_end = calc_lookback_window(ship_date, lookback_months)

        # Load window history for this month's lookback
        window_history = load_all_shipment_items_bulk(db, all_renewal_ids, lookback_start, lookback_end)

        # Add committed items from this month and prior months to cumulative blocked
        month_committed = committed.get(month_str, {})
        for tri, items in month_committed.items():
            cumulative_committed[tri].update(items)
            logger.info(f"[PLANNER]   Added {len(items)} committed items for T{tri} in {month_str}")

        # Build current kit list with remaining stock
        current_kits = []
        for k in all_kits:
            remaining = kit_stock_remaining.get(k["id"], 0)
            if remaining > 0:
                kit_copy = dict(k)
                kit_copy["quantity_available"] = remaining
                current_kits.append(kit_copy)

        kits_by_trimester = defaultdict(list)
        for k in current_kits:
            kits_by_trimester[k["trimester"]].append(k)

        # ── Per-trimester analysis ──
        month_trimesters = {}

        for tri in [1, 2, 3, 4]:
            tri_custs = trimester_groups.get(tri, [])
            tri_kits = kits_by_trimester.get(tri, [])

            # Extra blocked = cumulative committed items for this trimester
            extra_blocked = cumulative_committed.get(tri, set()) or None
            if extra_blocked and len(extra_blocked) == 0:
                extra_blocked = None

            # Estimate kit coverage
            covered, needs_new = _estimate_kit_coverage_for_month(
                tri_customers=tri_custs,
                tri_kits=tri_kits,
                full_history=full_history,
                received_kits=received_kits,
                kit_items_map=kit_items_map,
                alt_map=alt_map,
                extra_blocked=extra_blocked,
            )

            # Kit stock for this trimester (computed before build_qty so we can cap coverage)
            tri_kit_stock = sum(k["quantity_available"] for k in tri_kits)

            # Cap covered by physical kit availability — can't ship more kits than exist in stock.
            # _estimate_kit_coverage_for_month checks item-history compatibility per-customer but
            # doesn't constrain by quantity, so in later months where stock < eligible customers
            # it can overestimate coverage and underestimate the required build.
            if covered > tri_kit_stock:
                logger.info(
                    f"[PLANNER]   T{tri} coverage cap: estimated={covered} but stock={tri_kit_stock}"
                    f" → capping covered to {tri_kit_stock}, needs_new={len(tri_custs) - tri_kit_stock}"
                )
                covered = tri_kit_stock
                needs_new = len(tri_custs) - covered

            # Estimate item risk (DO NOT USE / CAN USE counts)
            dnu_count, cu_count = _estimate_item_risk_for_month(
                trimester=tri,
                tri_customers=tri_custs,
                window_history=window_history,
                all_items_list=all_items_list,
                alt_map=alt_map,
                extra_blocked=extra_blocked,
            )

            # Build quantity
            build_qty = calculate_build_quantities(
                projected_count=len(tri_custs),
                covered_count=covered,
                warehouse_minimum=warehouse_minimum,
            )

            # (tri_kit_stock already computed above for coverage cap)

            # Committed items for this trimester/month
            committed_for_tri = month_committed.get(tri, set())

            month_trimesters[tri] = {
                "projected_customers": len(tri_custs),
                "covered_by_existing": covered,
                "needs_new_curation": needs_new,
                "recommended_build_qty": build_qty["recommended_build_qty"],
                "expected_leftover": build_qty["expected_leftover"],
                "do_not_use_count": dnu_count,
                "can_use_count": cu_count,
                "kit_stock_remaining": tri_kit_stock,
                "kit_count": len(tri_kits),
                "committed_item_count": len(committed_for_tri),
                "cumulative_committed_count": len(cumulative_committed.get(tri, set())),
            }

            logger.info(
                f"[PLANNER]   T{tri}: {len(tri_custs)} custs, "
                f"{covered} covered, {needs_new} need new, "
                f"kit_stock={tri_kit_stock}, safe_items={cu_count}, "
                f"committed={len(committed_for_tri)}"
            )

            # Deplete kit stock for next month's projection
            # Estimate: covered customers consume kits from stock
            remaining_to_deplete = covered
            for kit in sorted(tri_kits, key=lambda k: k.get("age_rank", 0)):
                if remaining_to_deplete <= 0:
                    break
                available = kit_stock_remaining.get(kit["id"], 0)
                used = min(available, remaining_to_deplete)
                kit_stock_remaining[kit["id"]] = available - used
                remaining_to_deplete -= used

        months_data[month_str] = {
            "ship_date": str(ship_date),
            "lookback_window": f"{lookback_start} to {lookback_end}",
            "total_renewal": len(renewal_pool),
            "total_new": len(new_customers),
            "trimesters": month_trimesters,
            "has_committed_items": bool(month_committed),
        }

    # ── Generate warnings ──
    warnings = _generate_warnings(months_data, welcome_watchlist)

    # ── Committed items summary ──
    committed_summary = {}
    for month_str, tri_map in committed.items():
        committed_summary[month_str] = {
            tri: len(items) for tri, items in tri_map.items()
        }

    result = {
        "base_month": base_month,
        "horizon": horizon_months,
        "ship_day": ship_day,
        "warehouse_minimum": warehouse_minimum,
        "lookback_months": lookback_months,
        "recency_months": recency_months,
        "generated_at": str(date.today()),
        "months": months_data,
        "warnings": warnings,
        "committed_items": committed_summary,
        "welcome_watchlist": welcome_watchlist,
    }

    logger.info(f"[PLANNER] ═══════════════════════════════════════════════════")
    logger.info(f"[PLANNER] Projection complete: {len(month_list)} months, {len(warnings)} warnings")
    for month_str in month_list:
        md = months_data[month_str]
        tri_summary = ", ".join(
            f"T{t}={md['trimesters'].get(t, {}).get('projected_customers', 0)}"
            for t in [1, 2, 3, 4]
        )
        logger.info(f"[PLANNER]   {month_str}: {tri_summary}")
    logger.info(f"[PLANNER] ═══════════════════════════════════════════════════")

    return result
