#!/usr/bin/env python3
"""
OBB Data Reconciliation Audit Script
======================================
Reads all 24 order-history CSVs AND the live Supabase DB,
cross-references everything, and produces detailed reports.

This is READ-ONLY — no DB writes.

Reports produced:
  1. Customer parity (CSV vs DB)
  2. Shipment parity (CSV vs DB per customer per month)
  3. Shipment items coverage (which shipments have no items)
  4. Kit coverage (which kits have no kit_items)
  5. Kit SKU mapping (CSV kit SKUs vs DB kit SKUs)
  6. Summary with action items

USAGE:
  python audit_data.py
"""

import csv
import os
import re
import sys
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from collections import defaultdict
from dotenv import load_dotenv

# ── Environment ──────────────────────────────────────────────────────────────
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://tkcvvjxmzfjaesdhyfiy.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY in environment")

from supabase import create_client

SCRIPT_DIR = Path(__file__).parent
ORDER_HISTORY_DIR = SCRIPT_DIR.parent / "order history"
REPORT_DIR = SCRIPT_DIR / "audit_reports"

# ── Logging ──────────────────────────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "audit_data.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("obb_audit")


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED PARSERS (same logic as import_history.py)
# ═══════════════════════════════════════════════════════════════════════════════

MONTH_ABBR_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUG": 8, "SEPT": 9, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def batch_sort_key(p: Path) -> int:
    match = re.search(r"([A-Z]{2})\.csv$", p.name)
    if not match:
        return 9999
    code = match.group(1)
    return (ord(code[0]) - ord("A")) * 26 + (ord(code[1]) - ord("A"))


def get_csv_files_sorted(history_dir: Path) -> list:
    files = list(history_dir.glob("*.csv"))
    return sorted(files, key=batch_sort_key)


def extract_month_meta(filename: str) -> tuple:
    for abbr, num in MONTH_ABBR_MAP.items():
        if abbr in filename.upper():
            year_match = re.search(r"'(\d{2})", filename)
            year = 2000 + int(year_match.group(1)) if year_match else 2025
            batch_match = re.search(r"([A-Z]{2})\.csv$", filename)
            batch = batch_match.group(1) if batch_match else "??"
            label = f"{abbr} '{year - 2000:02d} {batch}"
            return label, year, num, batch
    return (filename, 2025, 1, "??")


def clean(val: str) -> Optional[str]:
    if not val:
        return None
    v = val.strip()
    if v.lower() in ("", "(blank)", "#error!", "none", "null", "n/a"):
        return None
    return v


def parse_kit_sku(raw: str) -> str:
    sku = re.sub(r"\s+Kits?$", "", raw.strip(), flags=re.IGNORECASE)
    return sku.strip()


def extract_trimester_from_sku(kit_sku: str) -> Optional[int]:
    parts = kit_sku.split("-")
    for part in reversed(parts):
        if part and part[0].isdigit():
            t = int(part[0])
            if 1 <= t <= 4:
                return t
    return None


def is_kit_header(row: list) -> bool:
    while len(row) < 3:
        row.append("")
    col_a = row[0].strip()
    col_b = row[1].strip()
    col_c = row[2].strip()
    if not col_a or col_b or col_c:
        return False
    return bool(re.search(r"\bKits?\b", col_a, re.IGNORECASE))


def is_customer_row(row: list) -> bool:
    while len(row) < 3:
        row.append("")
    email = row[2].strip()
    return bool(email and "@" in email and "." in email.split("@")[-1])


def parse_ship_date(created_at: Optional[str], fallback: date) -> date:
    if not created_at:
        return fallback
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return fallback


def normalize_kit_sku_for_matching(sku: str) -> str:
    """Normalize a kit SKU for matching: strip ' KITS'/' KIT', uppercase."""
    s = re.sub(r"\s+KITS?$", "", sku.strip(), flags=re.IGNORECASE).strip().upper()
    return s


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1: PARSE ALL CSVs
# ═══════════════════════════════════════════════════════════════════════════════

def parse_all_csvs() -> tuple:
    """
    Parse all 24 order-history CSVs.
    Returns:
        csv_customers: {email: {latest customer data}}
        csv_shipments: list of {email, kit_sku, ship_date, year_month, file_label, item_name, order_id}
        csv_kit_skus:  set of all unique kit SKUs seen in CSV headers
    """
    files = get_csv_files_sorted(ORDER_HISTORY_DIR)
    logger.info(f"[CSV] Found {len(files)} order-history CSV files")

    csv_customers = {}
    csv_shipments = []
    csv_kit_skus = set()
    duplicate_rows = 0
    seen_dedup = set()

    for csv_path in files:
        label, year, month_num, batch = extract_month_meta(csv_path.name)
        fallback_ship_date = date(year, month_num, 10)
        logger.info(f"  Parsing: {label} ({csv_path.name})")

        try:
            with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
                rows = list(csv.reader(f))
        except Exception as e:
            logger.error(f"  Could not read {csv_path.name}: {e}")
            continue

        current_kit_sku = None
        file_count = 0

        for row_idx, row in enumerate(rows):
            while len(row) < 23:
                row.append("")

            if row_idx < 2:
                continue

            if is_kit_header(row):
                current_kit_sku = parse_kit_sku(row[0])
                csv_kit_skus.add(current_kit_sku)
                continue

            if not is_customer_row(row):
                continue

            if not current_kit_sku:
                continue

            email = row[2].strip().lower()
            order_id = clean(row[1])
            created_at = clean(row[14])
            ship_date = parse_ship_date(created_at, fallback_ship_date)
            year_month = ship_date.strftime("%Y-%m")
            item_name = clean(row[0])  # col A has item name for customer rows
            trimester = extract_trimester_from_sku(current_kit_sku)

            # Dedup key: same as import_history.py
            dedup_key = (email, current_kit_sku, year_month)
            if dedup_key in seen_dedup:
                duplicate_rows += 1
                continue
            seen_dedup.add(dedup_key)

            csv_shipments.append({
                "email": email,
                "kit_sku": current_kit_sku,
                "ship_date": ship_date,
                "year_month": year_month,
                "file_label": label,
                "item_name": item_name,
                "order_id": order_id,
                "trimester": trimester,
            })

            # Merge customer (latest wins)
            if email not in csv_customers:
                csv_customers[email] = {}
            csv_customers[email]["email"] = email
            csv_customers[email]["file_label"] = label

            file_count += 1

        logger.info(f"    -> {file_count} unique customer-shipment rows")

    logger.info(f"[CSV] TOTAL: {len(csv_customers)} unique emails, "
                f"{len(csv_shipments)} unique shipments, "
                f"{len(csv_kit_skus)} unique kit SKUs, "
                f"{duplicate_rows} duplicate rows skipped")

    return csv_customers, csv_shipments, csv_kit_skus


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: LOAD DB STATE
# ═══════════════════════════════════════════════════════════════════════════════

def paginate_all(db, table_name: str, select_cols: str = "*") -> list:
    """Load all rows from a table via pagination."""
    all_rows = []
    offset = 0
    page_size = 1000
    while True:
        r = db.table(table_name).select(select_cols).range(offset, offset + page_size - 1).execute()
        all_rows.extend(r.data or [])
        if len(r.data or []) < page_size:
            break
        offset += page_size
    return all_rows


def load_db_state(db) -> dict:
    """Load full DB state into memory."""
    logger.info("[DB] Loading all data from Supabase...")

    logger.info("  Loading customers...")
    customers = paginate_all(db, "customers")
    logger.info(f"  -> {len(customers)} customers")

    logger.info("  Loading kits...")
    kits = paginate_all(db, "kits", "id,sku,name,trimester,size_variant,is_welcome_kit,quantity_available,age_rank")
    logger.info(f"  -> {len(kits)} kits")

    logger.info("  Loading items...")
    items = paginate_all(db, "items", "id,name,sku,category,has_sizing,is_therabox")
    logger.info(f"  -> {len(items)} items")

    logger.info("  Loading kit_items...")
    kit_items = paginate_all(db, "kit_items", "kit_id,item_id,quantity")
    logger.info(f"  -> {len(kit_items)} kit_items")

    logger.info("  Loading shipments...")
    shipments = paginate_all(db, "shipments", "id,customer_id,kit_id,kit_sku,ship_date,trimester_at_ship,order_id,platform")
    logger.info(f"  -> {len(shipments)} shipments")

    logger.info("  Loading shipment_items...")
    ship_items = paginate_all(db, "shipment_items", "shipment_id,item_id")
    logger.info(f"  -> {len(ship_items)} shipment_items")

    return {
        "customers": customers,
        "kits": kits,
        "items": items,
        "kit_items": kit_items,
        "shipments": shipments,
        "shipment_items": ship_items,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3: CROSS-REFERENCE AND AUDIT
# ═══════════════════════════════════════════════════════════════════════════════

def audit_customers(csv_customers: dict, db_state: dict) -> dict:
    """
    Audit 1: Customer Parity
    Check every CSV email is in DB.
    Check every DB customer is in CSV (or classify extra ones).
    """
    logger.info("\n" + "=" * 70)
    logger.info("  AUDIT 1: CUSTOMER PARITY")
    logger.info("=" * 70)

    db_customers_by_email = {}
    for c in db_state["customers"]:
        db_customers_by_email[c["email"].lower()] = c

    csv_emails = set(csv_customers.keys())
    db_emails = set(db_customers_by_email.keys())

    in_csv_not_db = csv_emails - db_emails
    in_db_not_csv = db_emails - csv_emails
    in_both = csv_emails & db_emails

    logger.info(f"  CSV unique emails:       {len(csv_emails)}")
    logger.info(f"  DB unique emails:        {len(db_emails)}")
    logger.info(f"  In both:                 {len(in_both)}")
    logger.info(f"  In CSV but NOT in DB:    {len(in_csv_not_db)}")
    logger.info(f"  In DB but NOT in CSV:    {len(in_db_not_csv)}")

    if in_csv_not_db:
        logger.warning(f"  MISSING IN DB ({len(in_csv_not_db)}):")
        for e in sorted(in_csv_not_db)[:20]:
            logger.warning(f"    - {e}")

    if in_db_not_csv:
        logger.info(f"  Extra DB customers (not in CSV history) — {len(in_db_not_csv)}:")
        # These are likely webhook-created customers after import
        for e in sorted(in_db_not_csv)[:10]:
            c = db_customers_by_email[e]
            logger.info(f"    - {e} (platform={c.get('platform')}, status={c.get('subscription_status')}, "
                       f"created={c.get('created_at', '')[:10]})")
        if len(in_db_not_csv) > 10:
            logger.info(f"    ... and {len(in_db_not_csv) - 10} more")

    return {
        "csv_emails": len(csv_emails),
        "db_emails": len(db_emails),
        "in_both": len(in_both),
        "missing_in_db": sorted(in_csv_not_db),
        "extra_in_db": sorted(in_db_not_csv),
        "db_customers_by_email": db_customers_by_email,
    }


def audit_kit_skus(csv_kit_skus: set, db_state: dict) -> dict:
    """
    Audit 2: Kit SKU Parity
    Check that every kit SKU from CSV headers has a matching kit in DB.
    """
    logger.info("\n" + "=" * 70)
    logger.info("  AUDIT 2: KIT SKU PARITY")
    logger.info("=" * 70)

    # Build DB kit lookup (normalize both ways)
    db_kit_by_norm = {}  # normalized sku -> db kit record
    db_kits_raw = {}     # raw db sku -> db kit record
    for k in db_state["kits"]:
        norm = normalize_kit_sku_for_matching(k["sku"])
        db_kit_by_norm[norm] = k
        db_kits_raw[k["sku"]] = k

    # Check CSV kit SKUs against DB
    csv_matched = {}
    csv_unmatched = []
    for csv_sku in sorted(csv_kit_skus):
        norm = normalize_kit_sku_for_matching(csv_sku)
        if norm in db_kit_by_norm:
            csv_matched[csv_sku] = db_kit_by_norm[norm]
        else:
            csv_unmatched.append(csv_sku)

    logger.info(f"  CSV unique kit SKUs:     {len(csv_kit_skus)}")
    logger.info(f"  Matched in DB:           {len(csv_matched)}")
    logger.info(f"  NOT found in DB:         {len(csv_unmatched)}")

    if csv_unmatched:
        logger.warning("  KITS IN CSV BUT NOT IN DB:")
        for sku in csv_unmatched:
            logger.warning(f"    - '{sku}'")

    # Check kit_items coverage
    kit_items_by_kit_id = defaultdict(list)
    for ki in db_state["kit_items"]:
        kit_items_by_kit_id[ki["kit_id"]].append(ki["item_id"])

    kits_no_items = []
    kits_with_items = []
    for k in db_state["kits"]:
        if k["id"] in kit_items_by_kit_id:
            kits_with_items.append(k)
        else:
            kits_no_items.append(k)

    logger.info(f"\n  DB kits with kit_items:   {len(kits_with_items)}")
    logger.info(f"  DB kits WITHOUT items:    {len(kits_no_items)}")
    if kits_no_items:
        logger.warning("  KITS WITHOUT ITEMS (no composition defined):")
        for k in kits_no_items:
            logger.warning(f"    - {k['sku']} (T{k['trimester']}, wk={k['is_welcome_kit']})")

    return {
        "csv_kit_skus": len(csv_kit_skus),
        "matched": len(csv_matched),
        "unmatched": csv_unmatched,
        "kits_no_items": [k["sku"] for k in kits_no_items],
        "kits_with_items": len(kits_with_items),
        "db_kit_by_norm": db_kit_by_norm,
        "kit_items_by_kit_id": dict(kit_items_by_kit_id),
    }


def audit_shipments(csv_shipments: list, db_state: dict, customer_audit: dict, kit_audit: dict) -> dict:
    """
    Audit 3: Shipment Parity
    For each CSV shipment, check if it exists in DB.
    For each DB shipment, check if it exists in CSV.
    """
    logger.info("\n" + "=" * 70)
    logger.info("  AUDIT 3: SHIPMENT PARITY")
    logger.info("=" * 70)

    db_customers_by_email = customer_audit["db_customers_by_email"]
    db_kit_by_norm = kit_audit["db_kit_by_norm"]

    # Build DB shipment lookup: (customer_id, normalized_kit_sku, year_month)
    db_ship_index = {}  # dedup_key -> shipment record
    db_ship_by_customer = defaultdict(list)
    for s in db_state["shipments"]:
        norm_sku = normalize_kit_sku_for_matching(s["kit_sku"]) if s.get("kit_sku") else ""
        ym = s["ship_date"][:7] if s.get("ship_date") else ""
        key = (s["customer_id"], norm_sku, ym)
        db_ship_index[key] = s
        db_ship_by_customer[s["customer_id"]].append(s)

    # Build shipment_items index: shipment_id -> count of items
    ship_items_count = defaultdict(int)
    for si in db_state["shipment_items"]:
        ship_items_count[si["shipment_id"]] += 1

    # Check CSV → DB
    csv_matched = 0
    csv_not_in_db = []
    csv_matched_no_items = []
    csv_matched_with_items = 0

    for cs in csv_shipments:
        email = cs["email"]
        cust = db_customers_by_email.get(email)
        if not cust:
            csv_not_in_db.append({**cs, "reason": "customer not found in DB"})
            continue

        customer_id = cust["id"]
        # Match using normalized kit SKU
        norm_csv_sku = normalize_kit_sku_for_matching(cs["kit_sku"])
        ym = cs["year_month"]
        key = (customer_id, norm_csv_sku, ym)

        if key in db_ship_index:
            csv_matched += 1
            db_ship = db_ship_index[key]
            item_count = ship_items_count.get(db_ship["id"], 0)
            if item_count == 0:
                csv_matched_no_items.append({
                    "email": email,
                    "kit_sku": cs["kit_sku"],
                    "year_month": ym,
                    "file_label": cs["file_label"],
                    "db_shipment_id": db_ship["id"],
                    "db_kit_id": db_ship.get("kit_id"),
                    "db_kit_sku": db_ship.get("kit_sku"),
                })
            else:
                csv_matched_with_items += 1
        else:
            csv_not_in_db.append({
                "email": email,
                "kit_sku": cs["kit_sku"],
                "year_month": ym,
                "file_label": cs["file_label"],
                "reason": "no matching shipment in DB",
            })

    # Check DB → CSV (find extra DB shipments not in CSV)
    csv_ship_keys = set()
    for cs in csv_shipments:
        email = cs["email"]
        cust = db_customers_by_email.get(email)
        if cust:
            norm_sku = normalize_kit_sku_for_matching(cs["kit_sku"])
            csv_ship_keys.add((cust["id"], norm_sku, cs["year_month"]))

    db_extra = []
    for key, db_ship in db_ship_index.items():
        if key not in csv_ship_keys:
            # Find email for this customer
            email = "unknown"
            for e, c in db_customers_by_email.items():
                if c["id"] == db_ship["customer_id"]:
                    email = e
                    break
            db_extra.append({
                "email": email,
                "kit_sku": db_ship.get("kit_sku"),
                "ship_date": db_ship.get("ship_date"),
                "db_shipment_id": db_ship["id"],
                "has_kit_id": bool(db_ship.get("kit_id")),
            })

    logger.info(f"  CSV unique shipments:           {len(csv_shipments)}")
    logger.info(f"  DB total shipments:             {len(db_state['shipments'])}")
    logger.info(f"  CSV→DB matched:                 {csv_matched}")
    logger.info(f"    with shipment_items:           {csv_matched_with_items}")
    logger.info(f"    WITHOUT shipment_items:        {len(csv_matched_no_items)}")
    logger.info(f"  CSV→DB NOT matched:             {len(csv_not_in_db)}")
    logger.info(f"  DB extra (not in CSV):          {len(db_extra)}")

    # Log some unmatched details
    if csv_not_in_db:
        logger.warning(f"\n  CSV shipments NOT in DB (first 30):")
        for s in csv_not_in_db[:30]:
            logger.warning(f"    - {s['email']} | {s['kit_sku']} | {s['year_month']} | {s.get('file_label','')} | {s['reason']}")

    if csv_matched_no_items:
        logger.warning(f"\n  Shipments matched but NO shipment_items ({len(csv_matched_no_items)}):")
        # Group by kit_sku
        by_kit = defaultdict(int)
        for s in csv_matched_no_items:
            by_kit[s["db_kit_sku"] or s["kit_sku"]] += 1
        for sku, count in sorted(by_kit.items(), key=lambda x: -x[1]):
            logger.warning(f"    - {sku}: {count} shipments missing items")

    if db_extra:
        logger.info(f"\n  DB shipments with no CSV match (first 20):")
        for s in db_extra[:20]:
            logger.info(f"    - {s['email']} | {s['kit_sku']} | {s['ship_date']} | kit_id={s['has_kit_id']}")

    return {
        "csv_total": len(csv_shipments),
        "db_total": len(db_state["shipments"]),
        "matched": csv_matched,
        "matched_with_items": csv_matched_with_items,
        "matched_no_items": len(csv_matched_no_items),
        "csv_not_in_db": csv_not_in_db,
        "db_extra": db_extra,
        "no_items_detail": csv_matched_no_items,
    }


def audit_shipment_items_backfill(db_state: dict, kit_audit: dict) -> dict:
    """
    Audit 4: Which shipments can be backfilled with shipment_items?
    If a shipment has kit_id and the kit has kit_items, we can backfill.
    """
    logger.info("\n" + "=" * 70)
    logger.info("  AUDIT 4: SHIPMENT ITEMS BACKFILL ANALYSIS")
    logger.info("=" * 70)

    kit_items_by_kit_id = kit_audit["kit_items_by_kit_id"]

    # Find all shipments with no shipment_items
    ship_items_set = set()
    for si in db_state["shipment_items"]:
        ship_items_set.add(si["shipment_id"])

    no_items = []
    for s in db_state["shipments"]:
        if s["id"] not in ship_items_set:
            no_items.append(s)

    # Classify: can we backfill?
    can_backfill = []
    cannot_backfill_no_kit_id = []
    cannot_backfill_no_kit_items = []

    for s in no_items:
        if not s.get("kit_id"):
            cannot_backfill_no_kit_id.append(s)
        elif s["kit_id"] not in kit_items_by_kit_id:
            cannot_backfill_no_kit_items.append(s)
        else:
            can_backfill.append(s)

    logger.info(f"  Total shipments without items:    {len(no_items)}")
    logger.info(f"  CAN backfill (kit_id + items):    {len(can_backfill)}")
    logger.info(f"  Cannot: no kit_id:                {len(cannot_backfill_no_kit_id)}")
    logger.info(f"  Cannot: kit has no items:         {len(cannot_backfill_no_kit_items)}")

    # Group cannot-backfill by kit_sku
    if cannot_backfill_no_kit_id:
        by_sku = defaultdict(int)
        for s in cannot_backfill_no_kit_id:
            by_sku[s.get("kit_sku", "NULL")] += 1
        logger.warning(f"\n  No kit_id shipments by kit_sku:")
        for sku, count in sorted(by_sku.items(), key=lambda x: -x[1]):
            logger.warning(f"    - {sku}: {count}")

    if cannot_backfill_no_kit_items:
        by_sku = defaultdict(int)
        for s in cannot_backfill_no_kit_items:
            by_sku[s.get("kit_sku", "NULL")] += 1
        logger.warning(f"\n  Kit exists but no kit_items, by kit_sku:")
        for sku, count in sorted(by_sku.items(), key=lambda x: -x[1]):
            logger.warning(f"    - {sku}: {count}")

    return {
        "total_no_items": len(no_items),
        "can_backfill": len(can_backfill),
        "cannot_no_kit_id": len(cannot_backfill_no_kit_id),
        "cannot_no_kit_items": len(cannot_backfill_no_kit_items),
        "backfill_shipments": can_backfill,
        "no_kit_id_shipments": cannot_backfill_no_kit_id,
        "no_kit_items_shipments": cannot_backfill_no_kit_items,
    }


def per_customer_history_audit(csv_shipments: list, db_state: dict, customer_audit: dict) -> dict:
    """
    Audit 5: Per-customer shipment history diff
    For each customer, compare their CSV shipment list vs DB shipment list.
    """
    logger.info("\n" + "=" * 70)
    logger.info("  AUDIT 5: PER-CUSTOMER HISTORY DIFF")
    logger.info("=" * 70)

    db_customers_by_email = customer_audit["db_customers_by_email"]

    # Build CSV history per customer
    csv_by_customer = defaultdict(list)
    for cs in csv_shipments:
        csv_by_customer[cs["email"]].append(cs)

    # Build DB history per customer email
    # First, build customer_id -> email reverse map
    cid_to_email = {}
    for e, c in db_customers_by_email.items():
        cid_to_email[c["id"]] = e

    db_by_customer = defaultdict(list)
    for s in db_state["shipments"]:
        email = cid_to_email.get(s["customer_id"], "unknown")
        db_by_customer[email].append(s)

    # Compare
    perfect_match = 0
    count_mismatch = 0
    sku_mismatch = 0
    mismatch_details = []

    for email in sorted(csv_by_customer.keys()):
        csv_ships = csv_by_customer[email]
        db_ships = db_by_customer.get(email, [])

        csv_count = len(csv_ships)
        db_count = len(db_ships)

        if csv_count != db_count:
            count_mismatch += 1
            mismatch_details.append({
                "email": email,
                "csv_count": csv_count,
                "db_count": db_count,
                "diff": db_count - csv_count,
                "type": "count",
            })
        else:
            # Check if kit SKUs match month by month
            csv_set = set()
            for cs in csv_ships:
                csv_set.add((normalize_kit_sku_for_matching(cs["kit_sku"]), cs["year_month"]))
            db_set = set()
            for ds in db_ships:
                if ds.get("kit_sku"):
                    db_set.add((normalize_kit_sku_for_matching(ds["kit_sku"]), ds["ship_date"][:7]))

            if csv_set == db_set:
                perfect_match += 1
            else:
                sku_mismatch += 1
                mismatch_details.append({
                    "email": email,
                    "csv_count": csv_count,
                    "db_count": db_count,
                    "csv_only": csv_set - db_set,
                    "db_only": db_set - csv_set,
                    "type": "sku",
                })

    logger.info(f"  Customers checked:        {len(csv_by_customer)}")
    logger.info(f"  Perfect match:            {perfect_match}")
    logger.info(f"  Count mismatch:           {count_mismatch}")
    logger.info(f"  SKU/month mismatch:       {sku_mismatch}")

    if mismatch_details:
        logger.warning(f"\n  Mismatches (first 30):")
        for m in mismatch_details[:30]:
            if m["type"] == "count":
                logger.warning(f"    [{m['type']}] {m['email']}: CSV={m['csv_count']}, DB={m['db_count']} (diff={m['diff']:+d})")
            else:
                logger.warning(f"    [{m['type']}] {m['email']}: CSV-only={m['csv_only']}, DB-only={m['db_only']}")

    return {
        "customers_checked": len(csv_by_customer),
        "perfect_match": perfect_match,
        "count_mismatch": count_mismatch,
        "sku_mismatch": sku_mismatch,
        "mismatch_details": mismatch_details,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

def print_summary(customer_r, kit_r, ship_r, backfill_r, history_r):
    logger.info("\n" + "=" * 70)
    logger.info("  FINAL AUDIT SUMMARY")
    logger.info("=" * 70)

    logger.info("\n  ── CUSTOMERS ──")
    logger.info(f"  CSV emails: {customer_r['csv_emails']} | DB emails: {customer_r['db_emails']}")
    logger.info(f"  Missing in DB: {len(customer_r['missing_in_db'])} | Extra in DB: {len(customer_r['extra_in_db'])}")

    logger.info("\n  ── KIT SKUS ──")
    logger.info(f"  CSV kit SKUs: {kit_r['csv_kit_skus']} | Matched: {kit_r['matched']}")
    logger.info(f"  Unmatched: {kit_r['unmatched']}")
    logger.info(f"  DB kits with items: {kit_r['kits_with_items']} | Without items: {len(kit_r['kits_no_items'])}")

    logger.info("\n  ── SHIPMENTS ──")
    logger.info(f"  CSV: {ship_r['csv_total']} | DB: {ship_r['db_total']}")
    logger.info(f"  Matched: {ship_r['matched']} | Not in DB: {len(ship_r['csv_not_in_db'])} | DB extra: {len(ship_r['db_extra'])}")
    logger.info(f"  Matched WITH items: {ship_r['matched_with_items']} | WITHOUT items: {ship_r['matched_no_items']}")

    logger.info("\n  ── BACKFILL POTENTIAL ──")
    logger.info(f"  Shipments without items: {backfill_r['total_no_items']}")
    logger.info(f"  Can backfill: {backfill_r['can_backfill']}")
    logger.info(f"  Cannot (no kit_id): {backfill_r['cannot_no_kit_id']}")
    logger.info(f"  Cannot (kit has no items): {backfill_r['cannot_no_kit_items']}")

    logger.info("\n  ── PER-CUSTOMER HISTORY ──")
    logger.info(f"  Checked: {history_r['customers_checked']}")
    logger.info(f"  Perfect: {history_r['perfect_match']} | Count mismatch: {history_r['count_mismatch']} | SKU mismatch: {history_r['sku_mismatch']}")

    # ACTION ITEMS
    logger.info("\n" + "=" * 70)
    logger.info("  ACTION ITEMS")
    logger.info("=" * 70)

    actions = []
    if customer_r['missing_in_db']:
        actions.append(f"1. CRITICAL: {len(customer_r['missing_in_db'])} customers in CSV but not in DB — need to insert")
    if kit_r['unmatched']:
        actions.append(f"2. CRITICAL: {len(kit_r['unmatched'])} kit SKUs in CSV not found in DB — need to create kits: {kit_r['unmatched']}")
    if kit_r['kits_no_items']:
        actions.append(f"3. HIGH: {len(kit_r['kits_no_items'])} kits have no kit_items — need composition data")
    if ship_r['csv_not_in_db']:
        actions.append(f"4. HIGH: {len(ship_r['csv_not_in_db'])} CSV shipments not found in DB — need to import")
    if backfill_r['can_backfill'] > 0:
        actions.append(f"5. HIGH: {backfill_r['can_backfill']} shipments can have items backfilled from kit_items")
    if backfill_r['cannot_no_kit_id'] > 0:
        actions.append(f"6. MEDIUM: {backfill_r['cannot_no_kit_id']} shipments have no kit_id — need kit_sku→kit_id resolution")
    if backfill_r['cannot_no_kit_items'] > 0:
        actions.append(f"7. MEDIUM: {backfill_r['cannot_no_kit_items']} shipments' kits have no items defined — need kit composition first")
    if history_r['count_mismatch'] > 0:
        actions.append(f"8. LOW: {history_r['count_mismatch']} customers have different shipment counts CSV vs DB")

    for a in actions:
        logger.info(f"  {a}")

    if not actions:
        logger.info("  No action items — data looks clean!")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("=" * 70)
    logger.info("  OBB DATA RECONCILIATION AUDIT")
    logger.info("  READ-ONLY — No DB changes")
    logger.info("=" * 70)

    # Create report dir
    REPORT_DIR.mkdir(exist_ok=True)

    # Phase 1: Parse CSVs
    csv_customers, csv_shipments, csv_kit_skus = parse_all_csvs()

    # Phase 2: Load DB
    db = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    db_state = load_db_state(db)

    # Phase 3: Audits
    customer_result = audit_customers(csv_customers, db_state)
    kit_result = audit_kit_skus(csv_kit_skus, db_state)
    ship_result = audit_shipments(csv_shipments, db_state, customer_result, kit_result)
    backfill_result = audit_shipment_items_backfill(db_state, kit_result)
    history_result = per_customer_history_audit(csv_shipments, db_state, customer_result)

    # Final summary
    print_summary(customer_result, kit_result, ship_result, backfill_result, history_result)

    # Save detailed reports as JSON for later processing
    report = {
        "customers": {
            "missing_in_db": customer_result["missing_in_db"],
            "extra_in_db_count": len(customer_result["extra_in_db"]),
        },
        "kits": {
            "unmatched_skus": kit_result["unmatched"],
            "kits_no_items": kit_result["kits_no_items"],
        },
        "shipments": {
            "csv_not_in_db_count": len(ship_result["csv_not_in_db"]),
            "csv_not_in_db_first_50": ship_result["csv_not_in_db"][:50],
            "db_extra_count": len(ship_result["db_extra"]),
            "matched_no_items_count": ship_result["matched_no_items"],
        },
        "backfill": {
            "can_backfill": backfill_result["can_backfill"],
            "cannot_no_kit_id": backfill_result["cannot_no_kit_id"],
            "cannot_no_kit_items": backfill_result["cannot_no_kit_items"],
        },
        "history": {
            "perfect_match": history_result["perfect_match"],
            "count_mismatch": history_result["count_mismatch"],
            "sku_mismatch": history_result["sku_mismatch"],
            "mismatch_details_first_50": [
                {k: (list(v) if isinstance(v, set) else v) for k, v in m.items()}
                for m in history_result["mismatch_details"][:50]
            ],
        },
    }

    report_path = REPORT_DIR / "audit_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"\n  Detailed report saved to: {report_path}")
    logger.info(f"  Full log saved to: {SCRIPT_DIR / 'audit_data.log'}")


if __name__ == "__main__":
    main()
