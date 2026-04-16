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
from datetime import date, timedelta
from collections import defaultdict
from typing import Optional
import httpx

logger = logging.getLogger("obb.curation_report")


# ─── Config Defaults ───

DEFAULT_SHIP_DAY = 14             # 14th of the month
DEFAULT_WAREHOUSE_MIN = 100       # minimum kit build quantity
DEFAULT_LOOKBACK_MONTHS = 4       # months to look back for DO NOT USE
DEFAULT_RECENCY_MONTHS = 3        # only include customers whose last shipment was within X months
RISK_HIGH_THRESHOLD = 60.0        # >= 60% blocked = HIGH risk
RISK_MEDIUM_THRESHOLD = 25.0      # >= 25% blocked = MEDIUM risk


# ─── Trimester Calculation (same as Phase 1) ───

def calc_trimester(due_date: date, ship_date: date) -> int:
    """Calculate trimester from due date and ship date. Same formula as Phase 1."""
    t4_cutoff = ship_date + timedelta(days=19)
    t3_cutoff = t4_cutoff + timedelta(weeks=13)
    t2_cutoff = t3_cutoff + timedelta(weeks=14)
    if due_date <= t4_cutoff:
        return 4
    elif due_date <= t3_cutoff:
        return 3
    elif due_date <= t2_cutoff:
        return 2
    else:
        return 1


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
        # NULL size → universal + variant-1 (same fix as C2)
        size_filtered = [k for k in available_kits if k.get("is_universal") or k["size_variant"] == 1]

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
    }


# ─── DO NOT USE / CAN USE Generation ───

def generate_item_risk_report(
    trimester: int,
    trimester_customers: list[dict],
    customer_items_in_window: dict[str, set],
    all_items: list[dict],
    alt_map: dict[str, set],
) -> tuple[list[dict], list[dict]]:
    """
    Generate DO NOT USE and CAN USE item lists for a trimester.
    
    DO NOT USE: Items that ≥25% of the trimester group received in the lookback window.
    CAN USE: Everything else, sorted by age_rank (oldest first).
    
    Returns: (do_not_use_list, can_use_list)
    Each item dict has: item_id, name, sku, blocked_count, group_size, blocked_pct, risk_level
    """
    group_size = len(trimester_customers)
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
        }

        if risk in ("HIGH", "MEDIUM"):
            do_not_use.append(entry)
        else:
            can_use.append(entry)

    # Sort DO NOT USE by blocked_pct desc
    do_not_use.sort(key=lambda x: -x["blocked_pct"])
    # Sort CAN USE by age — items don't have age_rank directly, but we use blocked_pct asc then name
    can_use.sort(key=lambda x: (x["blocked_pct"], x["name"]))

    logger.info(f"[CURATION] T{trimester}: DO NOT USE = {len(do_not_use)} items, CAN USE = {len(can_use)} items")

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

    lookback_start, lookback_end = calc_lookback_window(ship_date, lookback_months)
    logger.info(f"[CURATION] Lookback window: {lookback_start} to {lookback_end}")

    # ── Step 1: Load renewal pool ──
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

            if not result["needs_new_curation"]:
                covered_count += 1

        # 8b: Generate DO NOT USE / CAN USE
        do_not_use, can_use = generate_item_risk_report(
            trimester=tri,
            trimester_customers=tri_customers,
            customer_items_in_window=window_history,
            all_items=all_items_list,
            alt_map=alt_map,
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
