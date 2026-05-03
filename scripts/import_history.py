#!/usr/bin/env python3
"""
OBB Customer History Import Script
====================================

Imports 24 months of customer + shipment history from the 'order history/' CSV
files into Supabase, oldest month first.

USAGE
-----
  python import_history.py --dry-run
      Parse all files, log everything, write NOTHING to DB.

  python import_history.py --dry-run --file "MAR '26 CK"
      Dry run on a single month (partial filename match, case-insensitive).

  python import_history.py --all
      Full import: all 24 months, MAR '24 BM -> MAR '26 CK.

  python import_history.py --file "MAR '26 CK"
      Import a single month only.

  python import_history.py --all --verbose
      Full import with per-row logging (very detailed).

  python import_history.py --dry-run --all --verbose
      Full dry run with per-row logging.

ARCHITECTURE -- BATCH / COLLECT-THEN-WRITE
------------------------------------------
  To keep runtime under a few minutes (not hours), the script uses a
  three-phase approach instead of one DB call per customer:

  PHASE 0 -- Pre-load from Supabase (4 queries total)
    - Load all existing customers into memory {email -> record}
    - Load kit cache (kits + kit_items)
    - Load existing shipments as a dedup key set

  PHASE 1 -- CSV collection (zero DB calls)
    - Parse all CSV files purely in memory
    - Merge customer data per email (oldest->newest, latest non-None wins)
    - Collect all shipment records into a list

  PHASE 2 -- Batch customer writes
    - Insert NEW customers (not in existing) in batches of 50
    - Update EXISTING customers' null fields (individually, usually few)

  PHASE 3 -- Batch shipment writes
    - Resolve email -> customer_id from phase 2 results
    - Filter out duplicates using the pre-loaded dedup set
    - Batch insert shipments in groups of 100
    - Batch insert shipment_items per batch

CSV FORMAT (24-column, 0-indexed)
----------------------------------
  Row 0: Empty (all commas) -- skip
  Row 1: Column headers -- skip
  Row 2+: Kit header OR customer row OR item row

  Kit header row: col A = "OBB-CK-41 Kits", cols B+C empty
  Customer row:   col C contains @ (valid email)
  Item-only row:  col A filled, col B+C empty, row follows last customer in group

  Col  0: KIT ASSIGNMENT + KIT SKU  (item name when on customer row)
  Col  1: ORDER NUMBER               e.g. "#OBB-12086"
  Col  2: ORDER_EMAIL                PRIMARY customer identifier
  Col  3: LINE ITEM SKU              e.g. " OBB-SUBPLAN-9 ||  "
  Col  4: SHIPPING NAME              "Taylor Phillips"
  Col  5: SHIPPING ADDRESS 1
  Col  6: SHIPPING ADDRESS 2
  Col  7: SHIPPING COMPANY           (skipped)
  Col  8: SHIPPING CITY
  Col  9: SHIPPING CITY (duplicate header, actually STATE/PROVINCE)
  Col 10: SHIPPING ZIP
  Col 11: SHIPPING COUNTRY
  Col 12: SHIPPING PHONE
  Col 13: SHIPMENT ID                (Shopify internal, skipped)
  Col 14: CREATED AT                 ISO timestamp "2026-03-01T17:09:23Z"
  Col 15: ALL ORDER COUNT            (used for validation logging only)
  Col 16: Q1 -- previous OBB customer (not stored)
  Col 17: Q2 -- wants_daddy_item      "yes-daddy-to-be" | "yes-mommy-to-be" | "no"
  Col 18: Q3 -- baby_gender           "baby-girl" | "baby-boy" | "unknown"
  Col 19: Q4 -- clothing_size         "sm" | "med" | "l" | "xl" | "xxl"
  Col 20: Q5 -- due_date              "2025-06-10"
  Col 21: SALES CHANNEL NAME         (skipped)
  Col 22: NOTE                       (skipped -- usually empty)

UPDATE STRATEGY FOR EXISTING CUSTOMERS
---------------------------------------
  Files are processed oldest -> newest (MAR '24 -> MAR '26), so the most recent
  CSV data wins for any customer that appears in multiple months.

  For NEW customers (not already in DB):
    - Insert with all fields from the latest CSV appearance.
    - subscription_status set to "active".

  For EXISTING customers (already in DB, e.g. from a live webhook):
    - Only fields that are currently NULL/empty in DB are updated.
    - subscription_status is NEVER touched.
    - platform is never downgraded from "both".

SHIPMENT IDEMPOTENCY
---------------------
  Dedup key: (customer_id, kit_sku, year_month)
  Pre-loaded from DB before any inserts. Safe to re-run.

KIT ITEMS
----------
  We do not parse item names from the CSV (fragile string matching).
  If the kit SKU exists in the kits table, we copy all of that kit's
  items (from kit_items) into shipment_items for each new shipment.
  This gives the decision engine full item-level history for all kits in DB.
"""

import csv
import os
import re
import sys
import logging
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client

# --- Environment --------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
    or ""
)

# --- Paths --------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
ORDER_HISTORY_DIR = SCRIPT_DIR.parent.parent / "order history"

# --- Logging ------------------------------------------------------------------
# Force UTF-8 on Windows console so arrow/box chars don't crash logging
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            SCRIPT_DIR / "import_history.log", mode="w", encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("obb_import")


# -----------------------------------------------------------------------------
# NORMALIZERS & PARSERS
# -----------------------------------------------------------------------------

def clean(val: str) -> Optional[str]:
    """Return None for blank / error placeholder values, otherwise strip whitespace."""
    if not val:
        return None
    v = val.strip()
    if v.lower() in ("", "(blank)", "#error!", "none", "null", "n/a"):
        return None
    return v


def normalize_size(raw: Optional[str]) -> Optional[str]:
    """Normalize clothing size to S / M / L / XL (or None if unrecognised)."""
    if not raw:
        return None
    size_map = {
        "s": "S", "sm": "S", "small": "S",
        "m": "M", "med": "M", "medium": "M",
        "l": "L", "lg": "L", "lrg": "L", "large": "L",
        "xl": "XL", "x-large": "XL", "xlarge": "XL", "x-lg": "XL",
        "xxl": "XL", "2xl": "XL", "xx-large": "XL", "xxlarge": "XL",
    }
    return size_map.get(raw.strip().lower())


def normalize_gender(raw: Optional[str]) -> Optional[str]:
    """Normalize gender string to 'girl' / 'boy' / 'unknown'."""
    if not raw:
        return None
    v = raw.strip().lower()
    if "girl" in v:
        return "girl"
    if "boy" in v:
        return "boy"
    if "unknown" in v or "surprise" in v:
        return "unknown"
    return None


def normalize_daddy(raw: Optional[str]) -> bool:
    """Return True if customer opted for daddy item."""
    if not raw:
        return False
    return raw.strip().lower().startswith("yes")


def parse_name(shipping_name: Optional[str]) -> tuple:
    """'Taylor Phillips' -> ('Taylor', 'Phillips').  Single word -> (word, '')."""
    if not shipping_name:
        return ("", "")
    parts = shipping_name.strip().split(" ", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def parse_ship_date(created_at: Optional[str], fallback: date) -> date:
    """Parse CREATED AT ISO timestamp -> date.  Falls back to provided date."""
    if not created_at:
        return fallback
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        logger.warning(
            f"[DATE] Could not parse '{created_at}', using fallback {fallback}"
        )
        return fallback


def parse_due_date(raw: Optional[str]) -> Optional[str]:
    """
    Parse due-date string -> ISO 'YYYY-MM-DD' string (or None).
    Handles multiple formats.
    """
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(raw.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    logger.warning(f"[DATE] Could not parse due_date '{raw}' -- skipping due_date")
    return None


def parse_kit_sku(raw: str) -> str:
    """
    Strip the ' Kits' / ' Kit' suffix from a kit header value.
      'OBB-CK-41 Kits'  -> 'OBB-CK-41'
      'OBB-BW-21 Kit'   -> 'OBB-BW-21'
    """
    sku = re.sub(r"\s+Kits?$", "", raw.strip(), flags=re.IGNORECASE)
    return sku.strip()


def extract_trimester_from_sku(kit_sku: str) -> Optional[int]:
    """
    Extract the trimester integer from a kit SKU.
      'OBB-CK-41' -> 4   (first digit of last '-' segment)
      'OBB-BW-21' -> 2
      'OBB-BP-11' -> 1
    Returns None if parsing fails.
    """
    parts = kit_sku.split("-")
    for part in reversed(parts):
        if part and part[0].isdigit():
            t = int(part[0])
            if 1 <= t <= 4:
                return t
    return None


# -----------------------------------------------------------------------------
# ROW TYPE DETECTION
# -----------------------------------------------------------------------------

def is_kit_header(row: list) -> bool:
    """
    Kit header: col A ends with 'Kits' or 'Kit', col B (order#) empty,
    col C (email) empty.
    """
    while len(row) < 3:
        row.append("")
    col_a = row[0].strip()
    col_b = row[1].strip()
    col_c = row[2].strip()
    if not col_a or col_b or col_c:
        return False
    return bool(re.search(r"\bKits?\b", col_a, re.IGNORECASE))


def is_customer_row(row: list) -> bool:
    """Customer rows have a recognisable email in col C (index 2)."""
    while len(row) < 3:
        row.append("")
    email = row[2].strip()
    return bool(email and "@" in email and "." in email.split("@")[-1])


# -----------------------------------------------------------------------------
# FILE SORTING & METADATA EXTRACTION
# -----------------------------------------------------------------------------

MONTH_ABBR_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUG": 8, "SEPT": 9, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def batch_sort_key(p: Path) -> int:
    """
    Sort CSV files chronologically by the 2-letter batch code at end of name.
    BM (oldest) -> CK (newest).
    """
    match = re.search(r"([A-Z]{2})\.csv$", p.name)
    if not match:
        return 9999
    code = match.group(1)
    return (ord(code[0]) - ord("A")) * 26 + (ord(code[1]) - ord("A"))


def get_csv_files_sorted(history_dir: Path) -> list:
    """Return all CSV files in history_dir sorted oldest -> newest."""
    files = list(history_dir.glob("*.csv"))
    return sorted(files, key=batch_sort_key)


def extract_month_meta_safe(filename: str) -> tuple:
    """
    Safe version: handles all month abbreviations robustly.
    Returns (label, year, month_int).
    """
    for abbr, num in MONTH_ABBR_MAP.items():
        if abbr in filename.upper():
            year_match = re.search(r"'(\d{2})", filename)
            year = 2000 + int(year_match.group(1)) if year_match else 2025
            batch_match = re.search(r"([A-Z]{2})\.csv$", filename)
            batch = batch_match.group(1) if batch_match else "??"
            label = f"{abbr} '{year - 2000:02d} {batch}"
            return label, year, num
    return (filename, 2025, 1)


# -----------------------------------------------------------------------------
# KIT LOOKUP CACHE
# -----------------------------------------------------------------------------

class KitCache:
    """Pre-loaded kits + kit_items from DB for fast O(1) lookups.

    The DB stores kits as 'OBB-CK-41 KITS' (VeraCore format with trailing KITS).
    The CSV headers parse to 'OBB-CK-41' (stripped). We build a normalized
    secondary index so both formats resolve to the same kit record.
    """

    def __init__(self, kits: list, kit_items: list):
        self.by_sku: dict = {}         # canonical db sku -> {id, trimester, sku}
        self.by_normalized: dict = {}  # stripped sku -> canonical db sku
        self.items_by_kit: dict = {}   # kit_id -> [item_id, ...]

        for kit in kits:
            db_sku = kit["sku"]
            entry = {
                "id": kit["id"],
                "trimester": kit["trimester"],
                "sku": db_sku,
            }
            self.by_sku[db_sku] = entry
            # Also index stripped form: 'OBB-CK-41 KITS' -> 'OBB-CK-41'
            # so CSV-parsed SKUs (already stripped) can find the kit.
            normalized = re.sub(r"\s+KITS?$", "", db_sku, flags=re.IGNORECASE).strip()
            if normalized != db_sku:
                self.by_normalized[normalized] = db_sku

        for ki in kit_items:
            self.items_by_kit.setdefault(ki["kit_id"], []).append(ki["item_id"])

    def get_kit(self, sku: str) -> Optional[dict]:
        """Lookup by exact sku OR normalized (stripped) sku."""
        result = self.by_sku.get(sku)
        if result:
            return result
        # Fallback: 'OBB-CK-41' -> finds 'OBB-CK-41 KITS'
        canonical = self.by_normalized.get(sku)
        if canonical:
            return self.by_sku.get(canonical)
        return None

    def canonical_sku(self, sku: str) -> str:
        """Return the canonical DB sku (e.g. 'OBB-CK-41 KITS'), or original if not found."""
        kit = self.get_kit(sku)
        return kit["sku"] if kit else sku

    def get_items(self, kit_id: str) -> list:
        return self.items_by_kit.get(kit_id, [])

    def __len__(self):
        return len(self.by_sku)


def load_kit_cache(db: Client) -> KitCache:
    """Load all kits + kit_items from Supabase into a KitCache."""
    logger.info("[SETUP] Loading kit cache from Supabase...")
    try:
        kits = db.table("kits").select("id, sku, trimester").execute()
        kit_items = db.table("kit_items").select("kit_id, item_id").execute()
        cache = KitCache(kits.data or [], kit_items.data or [])
        logger.info(
            f"[SETUP] Kit cache: {len(cache)} kits, "
            f"{len(cache.items_by_kit)} kits have item mappings"
        )
        return cache
    except Exception as e:
        logger.error(f"[SETUP] Failed to load kit cache: {e}")
        return KitCache([], [])


# -----------------------------------------------------------------------------
# PRE-LOAD HELPERS  (one query each, called once at startup)
# -----------------------------------------------------------------------------

def load_all_customers(db: Client) -> dict:
    """
    Load ALL existing customers from DB with pagination.
    Returns {email.lower(): full_customer_dict}.
    """
    logger.info("[SETUP] Pre-loading existing customers from Supabase...")
    existing: dict = {}
    page_size = 1000
    offset = 0
    while True:
        try:
            result = (
                db.table("customers")
                .select("*")
                .range(offset, offset + page_size - 1)
                .execute()
            )
        except Exception as e:
            logger.error(f"[SETUP] Failed to load customers (offset {offset}): {e}")
            break
        rows = result.data or []
        for row in rows:
            existing[row["email"].lower()] = row
        logger.info(f"[SETUP]   Loaded {offset + len(rows)} customers so far...")
        if len(rows) < page_size:
            break
        offset += page_size
    logger.info(f"[SETUP] Pre-loaded {len(existing)} existing customers")
    return existing


def load_existing_ship_keys(db: Client) -> set:
    """
    Load all existing shipments as a dedup set.
    Dedup key: (customer_id, kit_sku, 'YYYY-MM')
    Uses pagination for large tables.
    """
    logger.info("[SETUP] Pre-loading existing shipment keys for dedup...")
    keys: set = set()
    page_size = 1000
    offset = 0
    while True:
        try:
            result = (
                db.table("shipments")
                .select("customer_id, kit_sku, ship_date")
                .range(offset, offset + page_size - 1)
                .execute()
            )
        except Exception as e:
            logger.error(f"[SETUP] Failed to load shipments (offset {offset}): {e}")
            break
        rows = result.data or []
        for row in rows:
            year_month = row["ship_date"][:7]  # "2026-03"
            keys.add((row["customer_id"], row["kit_sku"], year_month))
        if len(rows) < page_size:
            break
        offset += page_size
    logger.info(f"[SETUP] Pre-loaded {len(keys)} existing shipment keys")
    return keys


# -----------------------------------------------------------------------------
# IMPORT STATS TRACKER
# -----------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.files_processed = 0
        self.customers_created = 0
        self.customers_updated = 0
        self.customers_unchanged = 0
        self.shipments_created = 0
        self.shipments_skipped_dup = 0
        self.ship_items_linked = 0
        self.kits_not_found_in_db: set = set()
        self.errors: list = []
        self.warnings: list = []

    def add_error(self, msg: str):
        logger.error(f"[ERROR] {msg}")
        self.errors.append(msg)

    def add_warning(self, msg: str):
        logger.warning(f"[WARN] {msg}")
        self.warnings.append(msg)

    def summary(self) -> str:
        lines = [
            "",
            "=" * 65,
            "  IMPORT SUMMARY",
            "=" * 65,
            f"  Files processed:                {self.files_processed}",
            f"  Customers CREATED:              {self.customers_created}",
            f"  Customers UPDATED (null fields):{self.customers_updated}",
            f"  Customers UNCHANGED:            {self.customers_unchanged}",
            f"  Shipments CREATED:              {self.shipments_created}",
            f"  Shipments SKIPPED (duplicate):  {self.shipments_skipped_dup}",
            f"  Shipment items linked:          {self.ship_items_linked}",
            f"  Kit SKUs not found in DB:       {len(self.kits_not_found_in_db)}",
            f"  Errors:                         {len(self.errors)}",
            f"  Warnings:                       {len(self.warnings)}",
        ]
        if self.kits_not_found_in_db:
            lines.append("\n  Kit SKUs missing from DB (shipments stored as text-only):")
            for sku in sorted(self.kits_not_found_in_db):
                lines.append(f"    - {sku}")
        if self.errors:
            lines.append("\n  ERRORS (first 30):")
            for e in self.errors[:30]:
                lines.append(f"    - {e}")
        if self.warnings:
            lines.append(f"\n  WARNINGS (first 20 of {len(self.warnings)}):")
            for w in self.warnings[:20]:
                lines.append(f"    - {w}")
        lines.append("=" * 65)
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# PHASE 1 -- CSV COLLECTION  (zero DB calls)
# -----------------------------------------------------------------------------

def collect_from_csv(
    csv_path: Path,
    customers_by_email: dict,
    shipments_collected: list,
    stats: Stats,
    verbose: bool,
) -> None:
    """
    Parse one CSV file. No DB calls -- just fills:
      - customers_by_email: {email -> merged latest customer data dict}
      - shipments_collected: list of shipment dicts with 'email' key

    Processing oldest->newest means later files overwrite earlier values
    for any non-None field, so the newest CSV data always wins per customer.
    """
    label, year, month_num = extract_month_meta_safe(csv_path.name)
    fallback_ship_date = date(year, month_num, 10)

    logger.info("")
    logger.info(f"{'-' * 60}")
    logger.info(f"[FILE] {label}  ({csv_path.name})")

    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            rows = list(csv.reader(f))
    except Exception as e:
        stats.add_error(f"Could not read '{csv_path.name}': {e}")
        return

    if len(rows) < 3:
        stats.add_warning(f"'{csv_path.name}': too few rows ({len(rows)}), skipping")
        return

    current_kit_sku: Optional[str] = None
    file_customers = 0
    file_skipped = 0
    missing_due_dates = 0
    unknown_sizes = []

    for row_idx, row in enumerate(rows):
        # Pad row to 23 cols so all indexing is safe
        while len(row) < 23:
            row.append("")

        if row_idx == 0:   # empty separator row
            continue
        if row_idx == 1:   # column-header row
            continue

        if is_kit_header(row):
            current_kit_sku = parse_kit_sku(row[0])
            logger.info(f"  KIT GROUP -> {current_kit_sku}")
            continue

        if not is_customer_row(row):
            continue  # item-only or blank row

        if not current_kit_sku:
            stats.add_warning(
                f"Row {row_idx} in '{label}': customer row before any kit header -- skipping"
            )
            file_skipped += 1
            continue

        # -- Extract all fields ------------------------------------------------
        email_raw = row[2].strip().lower()
        order_id = clean(row[1])
        first_name, last_name = parse_name(clean(row[4]))
        address_line1 = clean(row[5])
        address_line2 = clean(row[6])
        # col 7: SHIPPING COMPANY -- skip
        city = clean(row[8])
        province = clean(row[9])     # header says "SHIPPING CITY" but is actually STATE
        zip_code = clean(row[10])
        country = clean(row[11])
        phone = clean(row[12])
        # col 13: SHIPMENT ID -- skip
        created_at = clean(row[14])
        # col 15: ALL ORDER COUNT -- log only
        all_order_count = clean(row[15])
        # col 16: Q1 previous_obb -- not stored
        wants_daddy_raw = clean(row[17])
        gender_raw = clean(row[18])
        size_raw = clean(row[19])
        due_date_raw = clean(row[20])

        clothing_size = normalize_size(size_raw)
        baby_gender = normalize_gender(gender_raw)
        wants_daddy = normalize_daddy(wants_daddy_raw)
        due_date_str = parse_due_date(due_date_raw)
        ship_date = parse_ship_date(created_at, fallback_ship_date)
        trimester_at_ship = extract_trimester_from_sku(current_kit_sku)

        if not clothing_size and size_raw:
            unknown_sizes.append(f"row {row_idx}: {size_raw!r}")
        if not due_date_str:
            missing_due_dates += 1
        if not trimester_at_ship:
            stats.add_warning(
                f"Row {row_idx} in '{label}': "
                f"cannot extract trimester from '{current_kit_sku}'"
            )

        # -- Merge customer data (latest non-None wins) ------------------------
        new_cust = {
            "email": email_raw,
            "first_name": first_name or None,
            "last_name": last_name or None,
            "address_line1": address_line1,
            "address_line2": address_line2,
            "city": city,
            "province": province,
            "zip": zip_code,
            "country": country or None,
            "phone": phone,
            "clothing_size": clothing_size,
            "baby_gender": baby_gender,
            "wants_daddy_item": wants_daddy if wants_daddy else None,
            "due_date": due_date_str,
        }
        existing_cust = customers_by_email.get(email_raw, {})
        for field, val in new_cust.items():
            if val is not None:
                existing_cust[field] = val
        existing_cust["email"] = email_raw  # always keep email
        customers_by_email[email_raw] = existing_cust

        # -- Collect shipment record -------------------------------------------
        shipments_collected.append({
            "email": email_raw,
            "kit_sku": current_kit_sku,
            "ship_date": ship_date,
            "trimester_at_ship": trimester_at_ship,
            "order_id": order_id,
        })

        file_customers += 1

        if verbose:
            logger.info(
                f"  row {row_idx:3d}: {email_raw:40s} "
                f"kit={current_kit_sku:15s} date={ship_date} "
                f"T{trimester_at_ship} sz={clothing_size} orders={all_order_count}"
            )

    logger.info(
        f"  -> {label}: {file_customers} customers collected, "
        f"{file_skipped} skipped"
    )
    if missing_due_dates:
        logger.info(f"  -> {missing_due_dates} rows had no due_date")
    if unknown_sizes:
        logger.info(f"  -> Unrecognised sizes: {unknown_sizes[:5]}")

    stats.files_processed += 1


# -----------------------------------------------------------------------------
# PHASE 2 -- BATCH CUSTOMER WRITES
# -----------------------------------------------------------------------------

def batch_insert_new_customers(
    db: Client,
    customers_to_insert: list,
    stats: Stats,
) -> dict:
    """
    Batch INSERT customers that are not yet in DB.
    Returns {email: customer_id} for all newly inserted rows.
    Inserts in chunks of 50 to stay within Supabase payload limits.
    """
    result_ids: dict = {}
    if not customers_to_insert:
        logger.info("[PHASE 2] No new customers to insert.")
        return result_ids

    logger.info(
        f"[PHASE 2] Inserting {len(customers_to_insert)} new customers "
        f"in batches of 50..."
    )
    batch_size = 50

    for i in range(0, len(customers_to_insert), batch_size):
        batch = customers_to_insert[i : i + batch_size]
        # Add required fields for new rows
        records = []
        for c in batch:
            rec = {k: v for k, v in c.items() if v is not None}
            rec.setdefault("country", "US")
            rec.setdefault("platform", "shopify")
            rec.setdefault("subscription_status", "active")
            # wants_daddy_item defaults to False if missing
            if "wants_daddy_item" not in rec:
                rec["wants_daddy_item"] = False
            records.append(rec)

        try:
            result = db.table("customers").insert(records).execute()
            inserted = result.data or []
            for row in inserted:
                result_ids[row["email"].lower()] = row["id"]
            stats.customers_created += len(inserted)
            logger.info(
                f"  Batch {i // batch_size + 1}: inserted {len(inserted)} customers"
            )
        except Exception as e:
            stats.add_error(
                f"Batch customer insert failed (batch {i // batch_size + 1}): {e}"
            )
            # Attempt individual inserts so we skip the bad row, not the whole batch
            for rec in records:
                try:
                    r = db.table("customers").insert(rec).execute()
                    if r.data:
                        result_ids[r.data[0]["email"].lower()] = r.data[0]["id"]
                        stats.customers_created += 1
                except Exception as e2:
                    stats.add_error(f"  Individual insert failed for {rec.get('email')}: {e2}")

    logger.info(f"[PHASE 2] Inserted {stats.customers_created} new customers total")
    return result_ids


def update_existing_customers(
    db: Client,
    customers_by_email: dict,
    existing_customers: dict,
    stats: Stats,
) -> None:
    """
    For customers already in DB, update ONLY fields that are currently NULL.
    subscription_status is never touched.
    platform is never downgraded from 'both'.
    Usually 0 customers to update on a fresh DB -- runs fast.
    """
    updatable_fields = [
        "first_name", "last_name",
        "address_line1", "address_line2", "city", "province",
        "zip", "country", "phone",
        "clothing_size", "baby_gender", "due_date",
    ]
    to_update_count = 0

    for email, new_data in customers_by_email.items():
        if email not in existing_customers:
            continue  # new customers handled by batch_insert_new_customers()

        existing = existing_customers[email]
        updates: dict = {}

        for field in updatable_fields:
            if new_data.get(field) and not existing.get(field):
                updates[field] = new_data[field]

        # wants_daddy_item: only set to True, never force to False
        if new_data.get("wants_daddy_item") and not existing.get("wants_daddy_item"):
            updates["wants_daddy_item"] = True

        # platform: set to shopify if unset (never downgrade from 'both')
        if not existing.get("platform"):
            updates["platform"] = "shopify"

        if not updates:
            stats.customers_unchanged += 1
            continue

        to_update_count += 1
        try:
            db.table("customers").update(updates).eq("id", existing["id"]).execute()
            stats.customers_updated += 1
            logger.info(
                f"  [UPD] {email}: filled null fields: {list(updates.keys())}"
            )
        except Exception as e:
            stats.add_error(f"Customer update failed for {email}: {e}")

    if to_update_count == 0:
        logger.info("[PHASE 2] No existing customers needed field updates.")


# -----------------------------------------------------------------------------
# PHASE 3 -- BATCH SHIPMENT WRITES
# -----------------------------------------------------------------------------

def batch_insert_shipments(
    db: Client,
    shipments_collected: list,
    all_customer_ids: dict,
    kit_cache: KitCache,
    existing_ship_keys: set,
    stats: Stats,
    verbose: bool,
) -> None:
    """
    Resolve email -> customer_id, filter duplicates, then batch insert
    shipments (100 per call) and their shipment_items.
    """
    to_insert: list = []
    local_dedup = set()  # prevent within-batch duplicates

    for s in shipments_collected:
        email = s["email"]
        customer_id = all_customer_ids.get(email)
        if not customer_id:
            stats.add_warning(f"No customer_id for {email} -- shipment skipped")
            continue

        year_month = s["ship_date"].strftime("%Y-%m")
        # Resolve to canonical DB sku BEFORE dedup so keys are consistent across runs.
        # DB stores 'OBB-CK-41 KITS'; CSV parses to 'OBB-CK-41'. Use DB format so
        # shipment.kit_sku matches kits.sku in the engine's received_kit_skus check.
        canonical_sku = kit_cache.canonical_sku(s["kit_sku"])
        dedup_key = (customer_id, canonical_sku, year_month)

        if dedup_key in existing_ship_keys or dedup_key in local_dedup:
            stats.shipments_skipped_dup += 1
            if verbose:
                logger.info(
                    f"  [DUP] {email} kit={canonical_sku} {year_month} -- skipped"
                )
            continue
        local_dedup.add(dedup_key)

        kit = kit_cache.get_kit(s["kit_sku"])
        if not kit:
            stats.kits_not_found_in_db.add(s["kit_sku"])
            logger.warning(
                f"  [KIT NOT FOUND] '{s['kit_sku']}' not in DB -- shipment stored as text-only, item dedup disabled for this kit"
            )

        rec: dict = {
            "customer_id": customer_id,
            "kit_sku": canonical_sku,  # canonical DB format (e.g. 'OBB-CK-41 KITS')
            "ship_date": s["ship_date"].isoformat(),
            "platform": "shopify",
        }
        if kit:
            rec["kit_id"] = kit["id"]
        effective_t = s.get("trimester_at_ship") or (kit["trimester"] if kit else None)
        if effective_t:
            rec["trimester_at_ship"] = effective_t
        if s.get("order_id"):
            rec["order_id"] = s["order_id"]

        to_insert.append(rec)

    logger.info(
        f"[PHASE 3] Inserting {len(to_insert)} shipments "
        f"({stats.shipments_skipped_dup} duplicates skipped)..."
    )

    batch_size = 100
    for i in range(0, len(to_insert), batch_size):
        batch = to_insert[i : i + batch_size]
        try:
            result = db.table("shipments").insert(batch).execute()
            inserted_ships = result.data or []
            stats.shipments_created += len(inserted_ships)

            # -- Batch insert shipment_items for this batch --------------------
            items_to_insert: list = []
            for row in inserted_ships:
                kid = row.get("kit_id")
                if kid:
                    for iid in kit_cache.get_items(kid):
                        items_to_insert.append(
                            {"shipment_id": row["id"], "item_id": iid}
                        )

            if items_to_insert:
                item_batch_size = 100
                for j in range(0, len(items_to_insert), item_batch_size):
                    try:
                        db.table("shipment_items").insert(
                            items_to_insert[j : j + item_batch_size]
                        ).execute()
                    except Exception as e_items:
                        stats.add_warning(
                            f"shipment_items batch {j // item_batch_size + 1} "
                            f"failed (ship batch {i // batch_size + 1}): {e_items}"
                        )
                stats.ship_items_linked += len(items_to_insert)

            logger.info(
                f"  Batch {i // batch_size + 1}: "
                f"{len(inserted_ships)} shipments, "
                f"{len(items_to_insert)} items linked"
            )

        except Exception as e:
            stats.add_error(
                f"Shipment batch insert failed (batch {i // batch_size + 1}): {e}"
            )
            # Fallback: insert one by one so we can isolate bad rows
            for rec in batch:
                try:
                    r = db.table("shipments").insert(rec).execute()
                    if r.data:
                        stats.shipments_created += 1
                        sid = r.data[0]["id"]
                        kid = r.data[0].get("kit_id")
                        if kid:
                            for iid in kit_cache.get_items(kid):
                                try:
                                    db.table("shipment_items").insert(
                                        {"shipment_id": sid, "item_id": iid}
                                    ).execute()
                                    stats.ship_items_linked += 1
                                except Exception:
                                    pass
                except Exception as e2:
                    stats.add_error(
                        f"  Individual shipment insert failed "
                        f"({rec.get('customer_id', '')[:8]}/{rec.get('kit_sku')}): {e2}"
                    )

    logger.info(
        f"[PHASE 3] Done -- {stats.shipments_created} created, "
        f"{stats.ship_items_linked} items linked"
    )


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="OBB Customer History Import -- CSV -> Supabase (batch mode)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse + validate everything, write NOTHING to DB. Safe to run repeatedly.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all 24 monthly CSV files (oldest first).",
    )
    parser.add_argument(
        "--file",
        type=str,
        metavar="MONTH",
        help=(
            "Process a single month (partial, case-insensitive filename match). "
            "E.g. --file \"MAR '26 CK\""
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log every row (very detailed output).",
    )
    args = parser.parse_args()

    if not args.all and not args.file:
        parser.print_help()
        print("\nError: must specify --all or --file MONTH")
        sys.exit(1)

    # -- Pre-flight banner -----------------------------------------------------
    logger.info("=" * 65)
    logger.info("  OBB CUSTOMER HISTORY IMPORT  (batch mode)")
    logger.info(
        f"  Mode:   {'DRY RUN -- NO DB WRITES' if args.dry_run else 'LIVE IMPORT'}"
    )
    logger.info(
        f"  Target: {'ALL MONTHS' if args.all else f'SINGLE FILE: {args.file}'}"
    )
    logger.info("=" * 65)

    # -- Validate paths --------------------------------------------------------
    if not ORDER_HISTORY_DIR.exists():
        logger.error(
            f"Order history folder not found: {ORDER_HISTORY_DIR}\n"
            f"Expected at: {SCRIPT_DIR.parent / 'order history'}"
        )
        sys.exit(1)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are not set in .env"
        )
        sys.exit(1)

    # -- Connect ---------------------------------------------------------------
    try:
        db = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("[SETUP] Supabase connection established")
    except Exception as e:
        logger.error(f"[SETUP] Supabase connection failed: {e}")
        sys.exit(1)

    # -- Find CSV files --------------------------------------------------------
    all_files = get_csv_files_sorted(ORDER_HISTORY_DIR)
    if not all_files:
        logger.error(f"No CSV files found in: {ORDER_HISTORY_DIR}")
        sys.exit(1)

    if args.file:
        target = args.file.lower()
        files_to_process = [f for f in all_files if target in f.name.lower()]
        if not files_to_process:
            logger.error(f"No file matching '{args.file}' -- available files:")
            for f in all_files:
                logger.info(f"  {f.name}")
            sys.exit(1)
    else:
        files_to_process = all_files

    logger.info(f"[SETUP] Files to process ({len(files_to_process)}):")
    for f in files_to_process:
        _, year, month = extract_month_meta_safe(f.name)
        logger.info(f"  {year}-{month:02d}  {f.name}")

    # -- PHASE 0 -- Pre-load ----------------------------------------------------
    kit_cache = load_kit_cache(db)
    if len(kit_cache) == 0:
        logger.warning(
            "[SETUP] No kits in DB -- shipments will be stored without kit_id/items. "
            "Run after Sheena has entered kits for best results."
        )

    existing_customers = load_all_customers(db)

    # -- PHASE 1 -- CSV collection ----------------------------------------------
    logger.info("")
    logger.info("[PHASE 1] Parsing CSV files (no DB calls)...")
    customers_by_email: dict = {}
    shipments_collected: list = []
    stats = Stats()

    for csv_path in files_to_process:
        collect_from_csv(
            csv_path=csv_path,
            customers_by_email=customers_by_email,
            shipments_collected=shipments_collected,
            stats=stats,
            verbose=args.verbose,
        )

    new_emails = [e for e in customers_by_email if e not in existing_customers]
    upd_emails = [e for e in customers_by_email if e in existing_customers]

    logger.info("")
    logger.info(
        f"[PHASE 1] Done -- {len(customers_by_email)} unique customers, "
        f"{len(shipments_collected)} shipments collected"
    )
    logger.info(
        f"  {len(new_emails)} new  |  {len(upd_emails)} existing  "
        f"(will fill only null fields)"
    )

    # -- DRY RUN -- report and exit ---------------------------------------------
    if args.dry_run:
        stats.customers_created = len(new_emails)
        stats.customers_updated = len(upd_emails)  # upper bound; actual may be lower
        stats.shipments_created = len(shipments_collected)

        # Compute duplicate count estimate using in-memory dedup only
        local_dedup: set = set()
        for s in shipments_collected:
            key = (s["email"], s["kit_sku"], s["ship_date"].strftime("%Y-%m"))
            if key in local_dedup:
                stats.shipments_created -= 1
                stats.shipments_skipped_dup += 1
            else:
                local_dedup.add(key)

        # Kit coverage check
        for s in shipments_collected:
            if not kit_cache.get_kit(s["kit_sku"]):
                stats.kits_not_found_in_db.add(s["kit_sku"])

        logger.info(stats.summary())
        logger.info("")
        logger.info("DRY RUN COMPLETE -- nothing written to DB.")
        logger.info(
            "Review summary above, then run without --dry-run to import."
        )
        return

    # -- Confirm live import ---------------------------------------------------
    logger.info("")
    logger.info("LIVE IMPORT -- this will write to Supabase.")
    logger.info(
        f"  ~{len(new_emails)} customer inserts + "
        f"~{len(shipments_collected)} shipment inserts"
    )
    logger.info("  Press Enter to continue, or Ctrl+C to abort.")
    try:
        input()
    except KeyboardInterrupt:
        logger.info("Aborted.")
        sys.exit(0)

    # -- PHASE 2 -- Customer writes ---------------------------------------------
    logger.info("")
    logger.info("[PHASE 2] Writing customers...")

    new_customer_ids = batch_insert_new_customers(
        db=db,
        customers_to_insert=[customers_by_email[e] for e in new_emails],
        stats=stats,
    )
    update_existing_customers(
        db=db,
        customers_by_email=customers_by_email,
        existing_customers=existing_customers,
        stats=stats,
    )

    # Build complete email -> id map
    all_customer_ids: dict = {
        e: existing_customers[e]["id"] for e in existing_customers
    }
    all_customer_ids.update(new_customer_ids)

    missing_ids = [e for e in customers_by_email if e not in all_customer_ids]
    if missing_ids:
        logger.warning(
            f"[PHASE 2] {len(missing_ids)} emails have no customer_id after upsert "
            f"-- their shipments will be skipped. First 10: {missing_ids[:10]}"
        )

    # -- PHASE 3 -- Shipment writes ---------------------------------------------
    logger.info("")
    logger.info("[PHASE 3] Loading existing shipment keys for dedup...")
    existing_ship_keys = load_existing_ship_keys(db)

    batch_insert_shipments(
        db=db,
        shipments_collected=shipments_collected,
        all_customer_ids=all_customer_ids,
        kit_cache=kit_cache,
        existing_ship_keys=existing_ship_keys,
        stats=stats,
        verbose=args.verbose,
    )

    # -- Final summary ---------------------------------------------------------
    logger.info(stats.summary())
    logger.info("")
    logger.info("IMPORT COMPLETE")
    logger.info("  Next steps:")
    logger.info("  1. Supabase -> customers table (expect ~1,600 rows)")
    logger.info("  2. Supabase -> shipments table (expect ~7,200 rows)")
    logger.info("  3. Spot-check 10 customers against original spreadsheets")
    logger.info("  4. Re-curate test customers in engine to verify history")
    logger.info(f"  Log saved to: {SCRIPT_DIR / 'import_history.log'}")


if __name__ == "__main__":
    main()
