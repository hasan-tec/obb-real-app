#!/usr/bin/env python3
"""
import_wk_history.py
=====================
Imports Welcome Kit (WK) order history into Supabase.

Two phases:
  PHASE 1: Seed missing WK kits from the SKU legend CSV
  PHASE 2: Import WK orders from the 3 WK CSV files (2024, 2025, 2026)

USAGE
-----
  python import_wk_history.py --dry-run
      Parse + validate everything, write NOTHING to DB.

  python import_wk_history.py --live
      Full import (explicit --live flag required to prevent accidents).

  python import_wk_history.py --dry-run --verbose
      Per-row logging.

PATHS
-----
  WK CSVs:  opus-obb-prototype/Wk order history/*.csv
  Legend:   general spreedsheet/General Spreadsheets - OBB Box Sku List.csv

WK CSV FORMAT (flat, 21 columns, 0-indexed)
-------------------------------------------
  Row 0: empty
  Row 1: headers
  Row 2+: one customer per row

  Col  0: ORDER NUMBER
  Col  1: ORDER_EMAIL  (email -- col 2 in monthly CSVs)
  Col  2: LINE ITEM SKU
  Col  3: SHIPPING NAME
  Col  4: SHIPPING ADDRESS 1
  Col  5: SHIPPING ADDRESS 2
  Col  6: SHIPPING COMPANY
  Col  7: SHIPPING CITY
  Col  8: SHIPPING CITY (actually STATE/PROVINCE)
  Col  9: SHIPPING ZIP
  Col 10: SHIPPING COUNTRY
  Col 11: SHIPPING PHONE
  Col 12: SHIPMENT ID (skip)
  Col 13: CREATED AT  -> ship_date
  Col 14: ALL ORDER COUNT (log only)
  Col 15: Q1 previous OBB (skip)
  Col 16: Q2 wants_daddy_item
  Col 17: Q3 baby_gender
  Col 18: Q4 clothing_size
  Col 19: Q5 due_date
  Col 20: WK KIT (e.g. 'OBB-WK-C1 KITS') -- already canonical DB format

SKU LEGEND FORMAT
-----------------
  Sections: "WELCOME KITS 2021" through "WELCOME KITS 2026"
  Header row: col0=section label, col1..N=kit codes (e.g. WKD1, WKC3)
  Item rows:  col0=blank, col1..N=item name strings
  Skip rows where cell starts with '[' (annotation rows like "[built April 2024]")
"""

import csv
import logging
import os
import re
import sys
import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
    or ""
)

SCRIPT_DIR = Path(__file__).parent
WK_DIR     = SCRIPT_DIR.parent / "Wk order history"
SKU_LEGEND = (
    SCRIPT_DIR.parent.parent
    / "general spreedsheet"
    / "General Spreadsheets - OBB Box Sku List.csv"
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            SCRIPT_DIR / "import_wk_history.log", mode="w", encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("obb_wk_import")


# ===========================================================================
# SHARED UTILITIES  (from import_history.py + seed_old_kits.py)
# ===========================================================================

def clean(val: str) -> Optional[str]:
    if not val:
        return None
    v = val.strip()
    if v.lower() in ("", "(blank)", "#error!", "none", "null", "n/a"):
        return None
    return v


def normalize_size(raw: Optional[str]) -> Optional[str]:
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
    if not raw:
        return False
    return raw.strip().lower().startswith("yes")


def parse_name(shipping_name: Optional[str]) -> tuple:
    if not shipping_name:
        return ("", "")
    parts = shipping_name.strip().split(" ", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def parse_ship_date(created_at: Optional[str], fallback: date) -> date:
    if not created_at:
        return fallback
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        logger.warning(f"[DATE] Could not parse '{created_at}', using fallback {fallback}")
        return fallback


def parse_due_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(raw.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    logger.warning(f"[DATE] Could not parse due_date '{raw}' -- skipping")
    return None


def compute_age_rank(sku: str) -> int:
    """Replicates app.py compute_age_rank_from_sku exactly."""
    if not sku:
        return 0
    s = sku.strip().upper()
    if s.startswith("RW-"):
        s = s[3:]
    elif s.startswith("RW"):
        s = s[2:]
    if s.startswith("OBB-"):
        s = s[4:]
    if s.endswith(" KITS"):
        s = s[:-5]
    elif s.endswith(" KIT"):
        s = s[:-4]
    s = s.replace("-", "")
    prefix = ""
    for ch in s:
        if ch.isalpha():
            prefix += ch
        else:
            break
    if not prefix:
        return 0
    if prefix.startswith("WK"):
        suffix = prefix[2:]
        if suffix == "":
            return 10001
        if len(suffix) == 1 and suffix.isalpha():
            return 10001 + (ord(suffix) - ord("A") + 1)
        return 0
    if len(prefix) == 1:
        return ord(prefix[0]) - ord("A") + 1
    if len(prefix) == 2:
        return (ord(prefix[0]) - ord("A") + 1) * 26 + (ord(prefix[1]) - ord("A") + 1)
    if len(prefix) == 3:
        a = ord(prefix[0]) - ord("A") + 1
        b = ord(prefix[1]) - ord("A") + 1
        c = ord(prefix[2]) - ord("A") + 1
        return a * 676 + b * 26 + c
    return 0


# ---------------------------------------------------------------------------
# KIT CACHE
# ---------------------------------------------------------------------------

class KitCache:
    def __init__(self, kits: list, kit_items: list):
        self.by_sku: dict = {}
        self.by_normalized: dict = {}
        self.items_by_kit: dict = {}
        for kit in kits:
            db_sku = kit["sku"]
            entry = {"id": kit["id"], "trimester": kit["trimester"], "sku": db_sku}
            self.by_sku[db_sku] = entry
            normalized = re.sub(r"\s+KITS?$", "", db_sku, flags=re.IGNORECASE).strip()
            if normalized != db_sku:
                self.by_normalized[normalized] = db_sku
        for ki in kit_items:
            self.items_by_kit.setdefault(ki["kit_id"], []).append(ki["item_id"])

    def get_kit(self, sku: str) -> Optional[dict]:
        result = self.by_sku.get(sku)
        if result:
            return result
        canonical = self.by_normalized.get(sku)
        if canonical:
            return self.by_sku.get(canonical)
        return None

    def get_items(self, kit_id: str) -> list:
        return self.items_by_kit.get(kit_id, [])

    def __len__(self):
        return len(self.by_sku)


def load_kit_cache(db: Client) -> KitCache:
    logger.info("[SETUP] Loading kit cache from Supabase...")
    try:
        kits = db.table("kits").select("id, sku, trimester").execute()
        kit_items = db.table("kit_items").select("kit_id, item_id").execute()
        cache = KitCache(kits.data or [], kit_items.data or [])
        logger.info(f"[SETUP] Kit cache: {len(cache)} kits loaded")
        return cache
    except Exception as e:
        logger.error(f"[SETUP] Failed to load kit cache: {e}")
        return KitCache([], [])


def load_all_customers(db: Client) -> dict:
    logger.info("[SETUP] Pre-loading existing customers...")
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
        if len(rows) < page_size:
            break
        offset += page_size
    logger.info(f"[SETUP] Pre-loaded {len(existing)} existing customers")
    return existing


def load_existing_ship_keys(db: Client) -> set:
    logger.info("[SETUP] Pre-loading existing shipment dedup keys...")
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
            if not row.get("ship_date") or not row.get("kit_sku"):
                continue
            year_month = row["ship_date"][:7]
            keys.add((row["customer_id"], row["kit_sku"], year_month))
        if len(rows) < page_size:
            break
        offset += page_size
    logger.info(f"[SETUP] Pre-loaded {len(keys)} existing shipment keys")
    return keys


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.files_processed = 0
        self.customers_created = 0
        self.customers_updated = 0
        self.customers_unchanged = 0
        self.shipments_created = 0
        self.shipments_skipped_dup = 0
        self.ship_items_linked = 0
        self.kits_seeded = 0
        self.items_seeded = 0
        self.kit_items_linked = 0
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
            "  WK IMPORT SUMMARY",
            "=" * 65,
            f"  WK kits seeded:                  {self.kits_seeded}",
            f"  Items seeded:                    {self.items_seeded}",
            f"  Kit-item links added (seed):     {self.kit_items_linked}",
            f"  Files processed:                 {self.files_processed}",
            f"  Customers CREATED:               {self.customers_created}",
            f"  Customers UPDATED (null fields): {self.customers_updated}",
            f"  Customers UNCHANGED:             {self.customers_unchanged}",
            f"  Shipments CREATED:               {self.shipments_created}",
            f"  Shipments SKIPPED (duplicate):   {self.shipments_skipped_dup}",
            f"  Shipment items linked:           {self.ship_items_linked}",
            f"  Kit SKUs not found in DB:        {len(self.kits_not_found_in_db)}",
            f"  Errors:                          {len(self.errors)}",
            f"  Warnings:                        {len(self.warnings)}",
        ]
        if self.kits_not_found_in_db:
            lines.append("\n  Kit SKUs missing from DB (shipments stored as text-only):")
            for sku in sorted(self.kits_not_found_in_db):
                lines.append(f"    - {sku}")
        if self.errors:
            lines.append("\n  ERRORS (first 30):")
            for e in self.errors[:30]:
                lines.append(f"    - {e}")
        lines.append("=" * 65)
        return "\n".join(lines)


# ===========================================================================
# PHASE 1: SEED MISSING WK KITS FROM SKU LEGEND
# ===========================================================================

def normalize_wk_code(code: str) -> str:
    """
    Convert WK code from SKU legend to canonical DB SKU format.
      'WKD1'  -> 'OBB-WK-D1 KITS'
      'WK A1' -> 'OBB-WK-A1 KITS'
    """
    code = code.strip().upper().replace(" ", "")
    if code.startswith("WK"):
        code = code[2:]
    return f"OBB-WK-{code} KITS"


def wk_trimester_from_db_sku(db_sku: str) -> int:
    """
    OBB-WK-D1 KITS -> 1
    OBB-WK-B3 KITS -> 3
    Defaults to 1 if unparseable.
    """
    s = re.sub(r"^OBB-WK-", "", db_sku, flags=re.IGNORECASE)
    s = re.sub(r"\s+KITS?$", "", s, flags=re.IGNORECASE).strip()
    if s and s[-1].isdigit():
        return int(s[-1])
    return 1


def extract_trimester_regular_sku(sku: str) -> int:
    """
    For non-WK-prefix kits like OBB-BC-21 KITS, OBB-BK-24 KITS.
    Splits on '-', takes last segment, returns its first digit as trimester.
      OBB-BC-21  -> '21' -> T2
      OBB-BB-C41 -> 'C41' -> first digit is 4 -> T4
    """
    s = re.sub(r"\s+KITS?$", "", sku, flags=re.IGNORECASE).strip()
    parts = s.split("-")
    suffix = parts[-1] if parts else ""
    for ch in suffix:
        if ch.isdigit():
            return int(ch)
    return 1


def seed_and_mark_non_wk_kits(
    db: Client,
    non_wk_skus: set,
    kit_cache: KitCache,
    stats: Stats,
    dry_run: bool,
) -> KitCache:
    """
    For kit SKUs found in WK CSVs that lack the OBB-WK- prefix:
      - If missing from DB: INSERT with is_welcome_kit=True
      - If present with is_welcome_kit=False: UPDATE to True
    Returns refreshed KitCache (or same cache on dry-run).
    """
    if not non_wk_skus:
        return kit_cache

    logger.info("")
    logger.info(
        f"[PHASE 1.5] Processing {len(non_wk_skus)} non-WK-prefix kits "
        f"from WK CSVs..."
    )

    # Fetch current DB state for these SKUs
    resp = db.table("kits").select("id, sku, is_welcome_kit").in_("sku", list(non_wk_skus)).execute()
    existing_in_db: dict = {r["sku"]: r for r in (resp.data or [])}

    to_insert = []
    to_update = []  # [(sku, id)]

    for sku in sorted(non_wk_skus):
        if sku in existing_in_db:
            rec = existing_in_db[sku]
            if not rec["is_welcome_kit"]:
                to_update.append((sku, rec["id"]))
                logger.info(f"  [MARK WK] {sku} — already in DB, will set is_welcome_kit=True")
            else:
                logger.info(f"  [OK]      {sku} — already in DB with is_welcome_kit=True")
        else:
            to_insert.append(sku)
            logger.info(f"  [SEED]    {sku} — missing from DB, will INSERT")

    if dry_run:
        logger.info(
            f"[PHASE 1.5] DRY RUN -- would INSERT {len(to_insert)}, "
            f"UPDATE {len(to_update)} kits."
        )
        return kit_cache

    # Update existing kits
    for sku, kit_id in to_update:
        try:
            db.table("kits").update({"is_welcome_kit": True}).eq("id", kit_id).execute()
            logger.info(f"  [UPDATED] {sku} -> is_welcome_kit=True")
        except Exception as e:
            stats.add_error(f"Failed to update is_welcome_kit for {sku}: {e}")

    # Insert missing kits
    for sku in to_insert:
        trimester  = extract_trimester_regular_sku(sku)
        age_rank   = compute_age_rank(sku)
        code = re.sub(r"^OBB-", "", sku, flags=re.IGNORECASE)
        code = re.sub(r"\s+KITS?$", "", code, flags=re.IGNORECASE).strip()
        name = code.replace("-", " ").title()
        rec = {
            "sku":              sku,
            "name":             name,
            "trimester":        trimester,
            "is_welcome_kit":   True,
            "age_rank":         age_rank,
            "age_rank_source":  "auto",
            "quantity_available": 0,
            "is_universal":     False,
        }
        try:
            resp = db.table("kits").upsert(rec, on_conflict="sku").execute()
            if resp.data:
                stats.kits_seeded += 1
                logger.info(
                    f"  [INSERTED] {sku}  T{trimester}  age_rank={age_rank}"
                )
            else:
                stats.add_error(f"No data returned for non-WK kit upsert: {sku}")
        except Exception as e:
            stats.add_error(f"Non-WK kit upsert failed for {sku}: {e}")

    logger.info("[PHASE 1.5] Reloading kit cache...")
    return load_kit_cache(db)


def parse_sku_legend_wk_sections(legend_path: Path) -> dict:
    """
    Parse the SKU legend CSV and return WK kit -> item SKUs mapping.

    Only covers 'WELCOME KITS 20XX' sections (2021-2026).
    Skips annotation rows (cells starting with '[').

    Returns:
        {'OBB-WK-D1 KITS': ['OBB-AMMINAHSKINCARE+BROWNSUGARBUMPSCRUB', ...], ...}
    """
    logger.info(f"[LEGEND] Parsing SKU legend: {legend_path.name}")
    try:
        with open(legend_path, "r", encoding="utf-8-sig", errors="replace") as f:
            rows = list(csv.reader(f))
    except Exception as e:
        logger.error(f"[LEGEND] Failed to read legend: {e}")
        return {}

    wk_kits: dict = {}      # db_sku -> [item_sku, ...]
    kit_cols: dict = {}     # col_index -> db_sku  (active for current section)
    in_wk_section = False

    for row in rows:
        col0 = row[0].strip() if row else ""

        # Detect WK section header row: "WELCOME KITS 2021", "WELCOME KITS 2024", etc.
        if col0.upper().startswith("WELCOME KITS 20"):
            in_wk_section = True
            kit_cols = {}
            # Kit codes are in this same row starting at col 1
            for idx in range(1, len(row)):
                code = row[idx].strip()
                if not code or code.startswith("[") or "/" in code:
                    continue
                if not code.upper().startswith("WK"):
                    continue
                db_sku = normalize_wk_code(code)
                kit_cols[idx] = db_sku
                if db_sku not in wk_kits:
                    wk_kits[db_sku] = []
            logger.info(
                f"[LEGEND] Section '{col0}': {len(kit_cols)} kits "
                f"-> {list(kit_cols.values())}"
            )
            continue

        # Non-blank col0 that isn't a WK section header = new major section, stop tracking
        if col0 and not col0.startswith("["):
            if not col0.upper().startswith("WELCOME KITS"):
                in_wk_section = False
                kit_cols = {}
            continue

        if not in_wk_section or not kit_cols:
            continue

        # Item row: col0 blank, items in kit_cols column indices
        for col_idx, db_sku in kit_cols.items():
            if col_idx >= len(row):
                continue
            cell = row[col_idx].strip()
            if not cell or cell.startswith("["):
                continue
            item_sku = (
                cell.upper()
                if cell.upper().startswith("OBB-")
                else "OBB-" + cell.upper()
            )
            if item_sku not in wk_kits[db_sku]:
                wk_kits[db_sku].append(item_sku)

    total_items = sum(len(v) for v in wk_kits.values())
    logger.info(
        f"[LEGEND] Parsed {len(wk_kits)} WK kits, "
        f"{total_items} item entries total"
    )
    return wk_kits


def seed_wk_kits(
    db: Client,
    wk_kits_from_legend: dict,
    kit_cache: KitCache,
    stats: Stats,
    dry_run: bool,
) -> KitCache:
    """
    Insert WK kits, items, and kit_items links that are missing from DB.
    Returns a freshly reloaded KitCache after seeding (or the original on dry-run).
    """
    logger.info("")
    logger.info("[PHASE 1] Seeding missing WK kits...")

    # Load existing items + kit_items from DB
    items_resp = db.table("items").select("id, sku").execute()
    db_items_by_sku: dict = {}
    for i in items_resp.data or []:
        if i["sku"]:
            db_items_by_sku[i["sku"].strip().upper()] = i["id"]
    logger.info(f"[PHASE 1] {len(db_items_by_sku)} items already in DB")

    ki_resp = db.table("kit_items").select("kit_id, item_id").execute()
    existing_kit_items: set = set()
    for ki in ki_resp.data or []:
        existing_kit_items.add((ki["kit_id"], ki["item_id"]))

    # Identify missing kits
    missing_kits: dict = {}
    for db_sku, item_skus in wk_kits_from_legend.items():
        if kit_cache.get_kit(db_sku) is None:
            missing_kits[db_sku] = item_skus

    logger.info(f"[PHASE 1] {len(missing_kits)} WK kits missing from DB:")
    for sku in sorted(missing_kits):
        logger.info(f"  MISSING: {sku}  ({len(missing_kits[sku])} items in legend)")

    # Items that need to be created
    items_to_create = {}
    for item_skus in missing_kits.values():
        for sku in item_skus:
            if sku not in db_items_by_sku and sku not in items_to_create:
                items_to_create[sku] = sku
    logger.info(
        f"[PHASE 1] {len(items_to_create)} new items to create"
    )

    if dry_run:
        logger.info("[PHASE 1] DRY RUN -- no DB writes for kit seeding.")
        stats.kits_seeded = len(missing_kits)
        stats.items_seeded = len(items_to_create)
        stats.kit_items_linked = sum(len(v) for v in missing_kits.values())
        return kit_cache

    # 1. Create missing items
    if items_to_create:
        logger.info(f"[PHASE 1] Creating {len(items_to_create)} new items...")
        for upper_sku in items_to_create:
            raw = upper_sku.removeprefix("OBB-")
            name = raw.replace("+", " ").replace("&", " & ").title()
            try:
                resp = db.table("items").insert({
                    "sku": upper_sku,
                    "name": name,
                    "has_sizing": False,
                    "is_therabox": False,
                }).execute()
                if resp.data:
                    db_items_by_sku[upper_sku] = resp.data[0]["id"]
                    stats.items_seeded += 1
                    logger.info(f"  [CREATED item] {upper_sku}")
                else:
                    stats.add_error(f"No data returned for item insert: {upper_sku}")
            except Exception as e:
                stats.add_error(f"Item insert failed for {upper_sku}: {e}")

    # 2. Create missing kit records (upsert for idempotency)
    if missing_kits:
        logger.info(f"[PHASE 1] Creating {len(missing_kits)} WK kit records...")
        for db_sku in sorted(missing_kits):
            trimester = wk_trimester_from_db_sku(db_sku)
            age_rank = compute_age_rank(db_sku)
            code = re.sub(r"^OBB-WK-", "", db_sku, flags=re.IGNORECASE)
            code = re.sub(r"\s+KITS?$", "", code, flags=re.IGNORECASE).strip()
            name = f"WK-{code}"
            rec = {
                "sku": db_sku,
                "name": name,
                "trimester": trimester,
                "is_welcome_kit": True,
                "age_rank": age_rank,
                "age_rank_source": "auto",
                "quantity_available": 0,
                "is_universal": False,
            }
            try:
                resp = db.table("kits").upsert(rec, on_conflict="sku").execute()
                if resp.data:
                    stats.kits_seeded += 1
                    logger.info(
                        f"  [UPSERTED kit] {db_sku}  T{trimester}  age_rank={age_rank}"
                    )
                else:
                    stats.add_error(f"No data returned for kit upsert: {db_sku}")
            except Exception as e:
                stats.add_error(f"Kit upsert failed for {db_sku}: {e}")

    # 3. Create kit_items links -- reload kit IDs first
    logger.info("[PHASE 1] Linking items to kits...")
    kits_resp = db.table("kits").select("id, sku").execute()
    db_kits_by_sku: dict = {k["sku"].strip().upper(): k["id"] for k in kits_resp.data or []}

    links_to_insert = []
    for db_sku, item_skus in missing_kits.items():
        kit_id = db_kits_by_sku.get(db_sku.upper())
        if not kit_id:
            stats.add_warning(
                f"No kit_id for {db_sku} after upsert -- skipping item links"
            )
            continue
        for item_sku in item_skus:
            item_id = db_items_by_sku.get(item_sku.upper())
            if not item_id:
                stats.add_warning(f"Item {item_sku} not in DB -- skipping link")
                continue
            if (kit_id, item_id) not in existing_kit_items:
                links_to_insert.append({"kit_id": kit_id, "item_id": item_id})
                existing_kit_items.add((kit_id, item_id))

    if links_to_insert:
        chunk_size = 50
        for i in range(0, len(links_to_insert), chunk_size):
            chunk = links_to_insert[i:i + chunk_size]
            try:
                db.table("kit_items").insert(chunk).execute()
                stats.kit_items_linked += len(chunk)
                logger.info(
                    f"  [kit_items] Batch {i // chunk_size + 1}: {len(chunk)} links"
                )
            except Exception as e:
                stats.add_error(f"kit_items batch {i // chunk_size + 1} failed: {e}")
    else:
        logger.info("[PHASE 1] No new kit_items links to insert.")

    # Reload kit cache with fresh WK records
    logger.info("[PHASE 1] Reloading kit cache after seeding...")
    return load_kit_cache(db)


# ===========================================================================
# PHASE 2: IMPORT WK ORDERS AS SHIPMENTS
# ===========================================================================

def collect_from_wk_csv(
    csv_path: Path,
    customers_by_email: dict,
    shipments_collected: list,
    stats: Stats,
    verbose: bool,
) -> None:
    """
    Parse one flat WK CSV file.
    Fills customers_by_email and shipments_collected in-place. Zero DB calls.
    """
    logger.info("")
    logger.info(f"{'─' * 60}")
    logger.info(f"[FILE] {csv_path.name}")

    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            rows = list(csv.reader(f))
    except Exception as e:
        stats.add_error(f"Could not read '{csv_path.name}': {e}")
        return

    if len(rows) < 3:
        stats.add_warning(f"'{csv_path.name}': too few rows ({len(rows)}), skipping")
        return

    # Derive fallback year from filename (e.g. "WK ORDER 2024 (1)" -> 2024)
    year_guess = 2024
    m = re.search(r"(\d{4})", csv_path.stem)
    if m:
        year_guess = int(m.group(1))
    fallback_date = date(year_guess, 1, 1)

    file_customers = 0
    file_skipped = 0

    for row_idx, row in enumerate(rows):
        while len(row) < 21:
            row.append("")

        if row_idx == 0:  # blank separator
            continue
        if row_idx == 1:  # headers
            continue

        # Email is col 1 in WK CSVs (not col 2 like monthly CSVs)
        email_raw = row[1].strip().lower()
        if not email_raw or "@" not in email_raw or "." not in email_raw.split("@")[-1]:
            file_skipped += 1
            continue

        wk_sku_raw = clean(row[20])
        if not wk_sku_raw:
            stats.add_warning(
                f"Row {row_idx} in '{csv_path.name}': no WK KIT value -- skipping"
            )
            file_skipped += 1
            continue

        # Normalize: ensure space before KITS (e.g. 'OBB-WK-C1KITS' -> 'OBB-WK-C1 KITS')
        wk_sku = re.sub(r"(?<! )KITS$", " KITS", wk_sku_raw.strip(), flags=re.IGNORECASE)


        order_id      = clean(row[0])
        first_name, last_name = parse_name(clean(row[3]))
        address_line1 = clean(row[4])
        address_line2 = clean(row[5])
        city          = clean(row[7])
        province      = clean(row[8])
        zip_code      = clean(row[9])
        country       = clean(row[10])
        phone         = clean(row[11])
        created_at    = clean(row[13])

        clothing_size     = normalize_size(clean(row[18]))
        baby_gender       = normalize_gender(clean(row[17]))
        wants_daddy       = normalize_daddy(clean(row[16]))
        due_date_str      = parse_due_date(clean(row[19]))
        ship_date         = parse_ship_date(created_at, fallback_date)
        trimester_at_ship = wk_trimester_from_db_sku(wk_sku)

        # Merge customer data (latest non-None wins across files)
        new_cust = {
            "email":            email_raw,
            "first_name":       first_name or None,
            "last_name":        last_name or None,
            "address_line1":    address_line1,
            "address_line2":    address_line2,
            "city":             city,
            "province":         province,
            "zip":              zip_code,
            "country":          country or None,
            "phone":            phone,
            "clothing_size":    clothing_size,
            "baby_gender":      baby_gender,
            "wants_daddy_item": wants_daddy if wants_daddy else None,
            "due_date":         due_date_str,
        }
        existing_cust = customers_by_email.get(email_raw, {})
        for field, val in new_cust.items():
            if val is not None:
                existing_cust[field] = val
        existing_cust["email"] = email_raw
        customers_by_email[email_raw] = existing_cust

        shipments_collected.append({
            "email":             email_raw,
            "kit_sku":           wk_sku,        # already canonical DB format from CSV
            "ship_date":         ship_date,
            "trimester_at_ship": trimester_at_ship,
            "order_id":          order_id,
        })

        file_customers += 1

        if verbose:
            logger.info(
                f"  row {row_idx:4d}: {email_raw:40s} "
                f"kit={wk_sku:22s} date={ship_date} T{trimester_at_ship}"
            )

    logger.info(
        f"  -> {csv_path.name}: {file_customers} customers, {file_skipped} skipped"
    )
    stats.files_processed += 1


# ---------------------------------------------------------------------------
# Batch customer writes
# ---------------------------------------------------------------------------

def batch_insert_new_customers(
    db: Client,
    customers_to_insert: list,
    stats: Stats,
) -> dict:
    result_ids: dict = {}
    if not customers_to_insert:
        logger.info("[PHASE 2] No new customers to insert.")
        return result_ids

    logger.info(
        f"[PHASE 2] Inserting {len(customers_to_insert)} new customers (batches of 50)..."
    )
    batch_size = 50

    for i in range(0, len(customers_to_insert), batch_size):
        batch = customers_to_insert[i:i + batch_size]
        records = []
        for c in batch:
            rec = {k: v for k, v in c.items() if v is not None}
            rec.setdefault("country", "US")
            rec.setdefault("platform", "shopify")
            rec.setdefault("subscription_status", "active")
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
            for rec in records:
                try:
                    r = db.table("customers").insert(rec).execute()
                    if r.data:
                        result_ids[r.data[0]["email"].lower()] = r.data[0]["id"]
                        stats.customers_created += 1
                except Exception as e2:
                    stats.add_error(
                        f"Individual insert failed for {rec.get('email')}: {e2}"
                    )

    logger.info(f"[PHASE 2] Inserted {stats.customers_created} new customers total")
    return result_ids


def update_existing_customers(
    db: Client,
    customers_by_email: dict,
    existing_customers: dict,
    stats: Stats,
) -> None:
    updatable_fields = [
        "first_name", "last_name",
        "address_line1", "address_line2", "city", "province",
        "zip", "country", "phone",
        "clothing_size", "baby_gender", "due_date",
    ]

    for email, new_data in customers_by_email.items():
        if email not in existing_customers:
            continue
        existing = existing_customers[email]
        updates: dict = {}
        for field in updatable_fields:
            if new_data.get(field) and not existing.get(field):
                updates[field] = new_data[field]
        if new_data.get("wants_daddy_item") and not existing.get("wants_daddy_item"):
            updates["wants_daddy_item"] = True
        if not existing.get("platform"):
            updates["platform"] = "shopify"
        if not updates:
            stats.customers_unchanged += 1
            continue
        try:
            db.table("customers").update(updates).eq("id", existing["id"]).execute()
            stats.customers_updated += 1
            logger.info(f"  [UPD] {email}: filled: {list(updates.keys())}")
        except Exception as e:
            stats.add_error(f"Customer update failed for {email}: {e}")


def batch_insert_shipments(
    db: Client,
    shipments_collected: list,
    all_customer_ids: dict,
    kit_cache: KitCache,
    existing_ship_keys: set,
    stats: Stats,
    verbose: bool,
) -> None:
    to_insert: list = []
    local_dedup: set = set()

    for s in shipments_collected:
        email       = s["email"]
        customer_id = all_customer_ids.get(email)
        if not customer_id:
            stats.add_warning(f"No customer_id for {email} -- shipment skipped")
            continue

        year_month    = s["ship_date"].strftime("%Y-%m")
        canonical_sku = s["kit_sku"]   # WK CSVs already have canonical format
        dedup_key     = (customer_id, canonical_sku, year_month)

        if dedup_key in existing_ship_keys or dedup_key in local_dedup:
            stats.shipments_skipped_dup += 1
            if verbose:
                logger.info(
                    f"  [DUP] {email} kit={canonical_sku} {year_month} -- skipped"
                )
            continue
        local_dedup.add(dedup_key)

        kit = kit_cache.get_kit(canonical_sku)
        if not kit:
            stats.kits_not_found_in_db.add(canonical_sku)

        rec: dict = {
            "customer_id": customer_id,
            "kit_sku":     canonical_sku,
            "ship_date":   s["ship_date"].isoformat(),
            "platform":    "shopify",
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
        f"[PHASE 2] Inserting {len(to_insert)} shipments "
        f"({stats.shipments_skipped_dup} duplicates skipped)..."
    )

    batch_size = 100
    for i in range(0, len(to_insert), batch_size):
        batch = to_insert[i:i + batch_size]
        try:
            result = db.table("shipments").insert(batch).execute()
            inserted_ships = result.data or []
            stats.shipments_created += len(inserted_ships)

            items_to_insert: list = []
            for row in inserted_ships:
                kid = row.get("kit_id")
                if kid:
                    for iid in kit_cache.get_items(kid):
                        items_to_insert.append({"shipment_id": row["id"], "item_id": iid})

            if items_to_insert:
                for j in range(0, len(items_to_insert), 100):
                    try:
                        db.table("shipment_items").insert(
                            items_to_insert[j:j + 100]
                        ).execute()
                    except Exception as e_items:
                        stats.add_warning(
                            f"shipment_items sub-batch {j // 100 + 1} failed: {e_items}"
                        )
                stats.ship_items_linked += len(items_to_insert)

            logger.info(
                f"  Batch {i // batch_size + 1}: "
                f"{len(inserted_ships)} shipments, {len(items_to_insert)} items"
            )

        except Exception as e:
            stats.add_error(
                f"Shipment batch insert failed (batch {i // batch_size + 1}): {e}"
            )
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
                        f"Individual shipment failed "
                        f"({rec.get('customer_id', '')}/{rec.get('kit_sku')}): {e2}"
                    )

    logger.info(
        f"[PHASE 2] Done -- {stats.shipments_created} created, "
        f"{stats.ship_items_linked} items linked"
    )


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="OBB WK Order History Import -- CSV -> Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse + validate everything, write NOTHING to DB.",
    )
    group.add_argument(
        "--live",
        action="store_true",
        help="Full import: seed WK kits + import WK shipments.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log every row (very detailed).",
    )
    parser.add_argument(
        "--file",
        metavar="FILENAME",
        help="Process only this CSV filename inside the Wk order history folder.",
    )
    args = parser.parse_args()

    # Banner
    logger.info("=" * 65)
    logger.info("  OBB WELCOME KIT HISTORY IMPORT")
    logger.info(
        f"  Mode: {'DRY RUN -- NO DB WRITES' if args.dry_run else 'LIVE IMPORT'}"
    )
    logger.info("=" * 65)

    # Validate paths
    if not WK_DIR.exists():
        logger.error(f"WK order history folder not found: {WK_DIR}")
        sys.exit(1)
    if not SKU_LEGEND.exists():
        logger.error(f"SKU legend not found: {SKU_LEGEND}")
        sys.exit(1)

    if args.file:
        target = WK_DIR / args.file
        if not target.exists():
            logger.error(f"[SETUP] --file not found: {target}")
            sys.exit(1)
        wk_csv_files = [target]
    else:
        wk_csv_files = sorted(WK_DIR.glob("*.csv"))
    if not wk_csv_files:
        logger.error(f"No CSV files found in: {WK_DIR}")
        sys.exit(1)

    logger.info(f"[SETUP] WK CSV files ({len(wk_csv_files)}):")
    for f in wk_csv_files:
        logger.info(f"  {f.name}")

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY not set in .env")
        sys.exit(1)

    try:
        db = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("[SETUP] Supabase connection established")
    except Exception as e:
        logger.error(f"[SETUP] Supabase connection failed: {e}")
        sys.exit(1)

    stats = Stats()

    # Phase 0: Pre-load
    kit_cache          = load_kit_cache(db)
    existing_customers = load_all_customers(db)

    # Phase 1: Seed missing WK kits from legend
    wk_kits_from_legend = parse_sku_legend_wk_sections(SKU_LEGEND)
    kit_cache = seed_wk_kits(
        db=db,
        wk_kits_from_legend=wk_kits_from_legend,
        kit_cache=kit_cache,
        stats=stats,
        dry_run=args.dry_run,
    )

    # Phase 2: Collect WK CSV data
    logger.info("")
    logger.info("[PHASE 2] Parsing WK CSV files (no DB calls)...")
    customers_by_email: dict = {}
    shipments_collected: list = []

    for csv_path in wk_csv_files:
        collect_from_wk_csv(
            csv_path=csv_path,
            customers_by_email=customers_by_email,
            shipments_collected=shipments_collected,
            stats=stats,
            verbose=args.verbose,
        )

    new_emails = [e for e in customers_by_email if e not in existing_customers]
    upd_emails = [e for e in customers_by_email if e in existing_customers]

    logger.info(
        f"\n[PHASE 2] {len(customers_by_email)} unique customers, "
        f"{len(shipments_collected)} WK shipments collected"
    )
    logger.info(f"  {len(new_emails)} new  |  {len(upd_emails)} existing")

    # Phase 1.5: Seed / mark non-WK-prefix kit SKUs found in the WK CSVs
    non_wk_skus = {
        s["kit_sku"] for s in shipments_collected
        if not s["kit_sku"].upper().startswith("OBB-WK-")
    }
    if non_wk_skus:
        kit_cache = seed_and_mark_non_wk_kits(
            db=db,
            non_wk_skus=non_wk_skus,
            kit_cache=kit_cache,
            stats=stats,
            dry_run=args.dry_run,
        )

    # Dry-run exit
    if args.dry_run:
        stats.customers_created = len(new_emails)
        stats.customers_updated = len(upd_emails)
        stats.shipments_created = len(shipments_collected)
        local_dedup: set = set()
        for s in shipments_collected:
            key = (s["email"], s["kit_sku"], s["ship_date"].strftime("%Y-%m"))
            if key in local_dedup:
                stats.shipments_created -= 1
                stats.shipments_skipped_dup += 1
            else:
                local_dedup.add(key)
        for s in shipments_collected:
            if not kit_cache.get_kit(s["kit_sku"]):
                stats.kits_not_found_in_db.add(s["kit_sku"])
        logger.info(stats.summary())
        logger.info("")
        logger.info("DRY RUN COMPLETE -- nothing written to DB.")
        logger.info("Use --live to run the actual import.")
        return

    # Confirm live import
    logger.info("")
    logger.info("LIVE IMPORT -- this will write to Supabase.")
    logger.info(
        f"  ~{len(new_emails)} customer inserts + "
        f"~{len(shipments_collected)} shipment inserts"
    )
    logger.info("  Press Enter to continue, or Ctrl+C to abort.")
    try:
        input()
    except (KeyboardInterrupt, EOFError):
        if isinstance(sys.exc_info()[1], EOFError):
            logger.info("Non-interactive mode -- proceeding automatically.")
        else:
            logger.info("Aborted.")
            sys.exit(0)

    # Write customers
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

    all_customer_ids: dict = {e: existing_customers[e]["id"] for e in existing_customers}
    all_customer_ids.update(new_customer_ids)

    missing_ids = [e for e in customers_by_email if e not in all_customer_ids]
    if missing_ids:
        logger.warning(
            f"[PHASE 2] {len(missing_ids)} emails have no customer_id after upsert "
            f"-- shipments will be skipped. First 10: {missing_ids[:10]}"
        )

    # Write shipments
    logger.info("")
    logger.info("[PHASE 2] Loading existing shipment keys for dedup...")
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

    logger.info(stats.summary())
    logger.info("")
    logger.info("IMPORT COMPLETE")
    logger.info(
        "  Verify: SELECT count(*) FROM shipments WHERE kit_sku LIKE 'OBB-WK-%'"
    )
    logger.info(f"  Log: {SCRIPT_DIR / 'import_wk_history.log'}")


if __name__ == "__main__":
    main()
