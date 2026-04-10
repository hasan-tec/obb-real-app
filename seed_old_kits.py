"""
seed_old_kits.py
-----------------
Seeds missing historical kit records + items into Supabase.

The import_history.py script found 44 kit SKUs in the CSV files that do not
exist in the DB kits table. Without these records, imported shipments cannot
have kit_id set, which breaks item-level dedup in the decision engine.

WHAT THIS SCRIPT DOES:
  1. Loads all 97+ existing kits from DB
  2. Parses all 24 CSV files to collect kit groups and the variable items
     found in column 0 of each kit section (one item per customer row)
  3. Identifies kit SKUs that are MISSING from DB
  4. For each missing kit:
     a. Inserts a kit record (sku, name, trimester, size_variant, age_rank, ...)
     b. Links items from the CSV col0 values via kit_items
        - Looks up item by OBB-{col0_value} in items table
        - If item not in DB: creates a minimal item record first
        - Then inserts kit_items link

ITEM COVERAGE:
  The CSV col0 only captures the per-customer variable item (one item slot
  out of ~8 total). Fixed items (same for everyone in that kit month) are
  NOT in the CSV and cannot be recovered here. This gives partial coverage
  but is much better than zero coverage.

SAFETY:
  - Idempotent: uses upsert for kits (by sku) and inserts only missing items
  - --dry-run flag shows what WOULD be inserted without touching DB
  - Reads all 24 CSVs, correct for all missing batches

RUN:
  python seed_old_kits.py --dry-run   # preview
  python seed_old_kits.py             # apply to DB
"""

import csv
import glob
import logging
import os
import re
import sys
import argparse
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
    or ""
)

SCRIPT_DIR = Path(__file__).parent
ORDER_HISTORY_DIR = SCRIPT_DIR.parent / "order history"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            SCRIPT_DIR / "seed_old_kits.log", mode="w", encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("obb_seed_kits")

# ---------------------------------------------------------------------------
# Age Rank (same formula as app.py compute_age_rank_from_sku)
# ---------------------------------------------------------------------------
def compute_age_rank(sku: str) -> int:
    """
    Compute FIFO age rank from kit SKU.
    Replicates app.py compute_age_rank_from_sku exactly.

    Examples:
      OBB-BM-41 KITS -> prefix BM -> 2*26+13 = 65
      OBB-CK-41 KITS -> prefix CK -> 3*26+11 = 89
    """
    if not sku:
        return 0
    clean = sku.strip().upper()
    if clean.startswith("RW-"):
        clean = clean[3:]
    elif clean.startswith("RW"):
        clean = clean[2:]
    if clean.startswith("OBB-"):
        clean = clean[4:]
    if clean.endswith(" KITS"):
        clean = clean[:-5]
    elif clean.endswith(" KIT"):
        clean = clean[:-4]
    clean = clean.replace("-", "")

    prefix = ""
    for ch in clean:
        if ch.isalpha():
            prefix += ch
        else:
            break

    if not prefix:
        logger.warning(f"[AGE_RANK] Cannot parse prefix from '{sku}' -- returning 0")
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
        first = ord(prefix[0]) - ord("A") + 1
        second = ord(prefix[1]) - ord("A") + 1
        return first * 26 + second
    if len(prefix) == 3:
        a = ord(prefix[0]) - ord("A") + 1
        b = ord(prefix[1]) - ord("A") + 1
        c = ord(prefix[2]) - ord("A") + 1
        return a * 676 + b * 26 + c

    logger.warning(f"[AGE_RANK] Prefix '{prefix}' too long for '{sku}' -- returning 0")
    return 0


def extract_trimester(sku: str) -> int:
    """
    Extract trimester from SKU.
    OBB-BM-41 -> segment '41' -> first char '4' -> trimester 4
    OBB-BW-4  -> segment '4'  -> first char '4' -> trimester 4
    """
    # Strip OBB- and any KITS suffix
    clean = re.sub(r"^OBB-", "", sku, flags=re.IGNORECASE)
    clean = re.sub(r"\s+KITS?$", "", clean, flags=re.IGNORECASE)
    # Get last segment after final dash
    parts = clean.split("-")
    if len(parts) >= 2:
        last = parts[-1].strip()
        if last and last[0].isdigit():
            return int(last[0])
    return 0


def extract_size_variant(sku: str) -> Optional[int]:
    """
    Extract size variant from SKU.
    OBB-BM-41 -> segment '41' -> second char '1' -> size 1
    OBB-BM-33 -> segment '33' -> second char '3' -> size 3
    Returns None if not available.
    """
    clean = re.sub(r"^OBB-", "", sku, flags=re.IGNORECASE)
    clean = re.sub(r"\s+KITS?$", "", clean, flags=re.IGNORECASE)
    parts = clean.split("-")
    if len(parts) >= 2:
        last = parts[-1].strip()
        if len(last) >= 2 and last[0].isdigit() and last[1].isdigit():
            return int(last[1])
    return None


def sku_to_db_format(sku: str) -> str:
    """Convert parsed CSV sku to DB format: OBB-BM-41 -> OBB-BM-41 KITS"""
    if not sku.upper().endswith(" KITS"):
        return sku + " KITS"
    return sku


def make_kit_name(batch: str, trimester: int, size_variant: Optional[int], month_label: str) -> str:
    """Build a human-readable kit name."""
    size_str = f"-{size_variant}" if size_variant else ""
    return f"{month_label} - {batch}{size_str}"


def item_sku_from_col0(col0: str) -> str:
    """Convert CSV col0 value to DB item SKU format (OBB- prefix, uppercase)."""
    v = col0.strip().upper()
    if v.startswith("OBB-"):
        return v
    return f"OBB-{v}"


def item_name_from_col0(col0: str) -> str:
    """Convert CSV col0 value to a readable item name."""
    return col0.strip().title().replace("+", " ").replace("  ", " ")


# ---------------------------------------------------------------------------
# CSV Parsing
# ---------------------------------------------------------------------------
MONTH_NUM = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUNE": "06", "JUN": "06",
    "JULY": "07", "JUL": "07",
    "AUG": "08", "SEPT": "09", "SEP": "09",
    "OCT": "10", "NOV": "11", "DEC": "12",
}
MONTH_ABBR = {  # reverse: num -> 3-letter label
    "01": "JAN", "02": "FEB", "03": "MAR", "04": "APR",
    "05": "MAY", "06": "JUN", "07": "JUL", "08": "AUG",
    "09": "SEP", "10": "OCT", "11": "NOV", "12": "DEC",
}


def _parse_month_year_from_name(name: str) -> tuple:
    """
    Parse month and year from CSV filename like:
      "Oh Baby Boxes ... - MAR '24 BM"
      "Oh Baby Boxes ... - JUNE '25 CB"
    Returns (month_num, full_year) e.g. ("03", "2024") or ("", "").
    """
    # Try to match "MON '24" or "MON '25" etc
    pattern = r"([A-Z]+)\s+'(\d{2})\s+[A-Z]"
    m = re.search(pattern, name.upper())
    if m:
        mon_str = m.group(1)
        yr_str = m.group(2)
        month_num = MONTH_NUM.get(mon_str, "")
        if month_num and yr_str:
            return month_num, "20" + yr_str
    return "", ""


def get_month_label_from_path(filepath: Path) -> str:
    """Return a label like 'MAR 2024' parsed from filename like '... - MAR '24 BM'."""
    month_num, year = _parse_month_year_from_name(filepath.stem)
    if month_num and year:
        return f"{MONTH_ABBR.get(month_num, month_num)} {year}"
    return "UNKNOWN"


def get_year_month_from_path(filepath: Path) -> str:
    """Return YYYY-MM parsed from filename."""
    month_num, year = _parse_month_year_from_name(filepath.stem)
    if month_num and year:
        return f"{year}-{month_num}"
    return "0000-00"


def parse_kit_sku(raw: str) -> str:
    """
    Parse kit SKU from CSV col0 kit header row.
    'OBB-CK-41 Kits' -> 'OBB-CK-41'
    'OBB-BM-41 Kits' -> 'OBB-BM-41'
    """
    v = raw.strip()
    v = re.sub(r"\s+Kits?", "", v, flags=re.IGNORECASE)
    return v.strip()


def is_kit_header(col0: str, col2: str) -> bool:
    """Row is a kit group header if col0 has OBB- and col2 is empty (no email)."""
    return "OBB-" in col0 and "@" not in col0 and not col2.strip()


def is_order_only_row(col0: str) -> bool:
    """Row has no item in col0 (just an order number)."""
    return col0.startswith("#OBB-") or col0.startswith("OBB-") and not any(c.isdigit() for c in col0[4:6])


def parse_csvs_for_kit_items() -> dict:
    """
    Parse all 24 CSVs.
    Returns: {
        "OBB-BM-41": {
            "month_label": "MAR 2024",
            "year_month": "2024-03",
            "col0_items": {"ECLAT+REFRESHINGHYALURONICBODYWASH", ...}
        },
        ...
    }
    Only includes kit groups found in CSV files.
    """
    csv_files = sorted(glob.glob(str(ORDER_HISTORY_DIR / "*.csv")))
    logger.info(f"[PARSE] Found {len(csv_files)} CSV files in {ORDER_HISTORY_DIR}")

    kit_data: dict = {}

    for filepath_str in csv_files:
        filepath = Path(filepath_str)
        month_label = get_month_label_from_path(filepath)
        year_month = get_year_month_from_path(filepath)

        try:
            with open(filepath, encoding="utf-8-sig", errors="replace") as f:
                rows = list(csv.reader(f))
        except Exception as e:
            logger.error(f"[PARSE] Failed to read {filepath.name}: {e}")
            continue

        current_kit_sku = None

        for row in rows[2:]:  # skip row 0 (empty) and row 1 (headers)
            col0 = row[0].strip() if row else ""
            col2 = row[2].strip() if len(row) > 2 else ""

            if not col0:
                continue

            if is_kit_header(col0, col2):
                current_kit_sku = parse_kit_sku(col0)
                if current_kit_sku not in kit_data:
                    kit_data[current_kit_sku] = {
                        "month_label": month_label,
                        "year_month": year_month,
                        "col0_items": set(),
                    }
                continue

            # Skip rows that are pure order-number rows (no item in col0)
            if col0.startswith("#OBB-"):
                continue

            # Customer data row with an item in col0
            if current_kit_sku and "@" in col2 and col0:
                item_val = col0.strip()
                # Skip cells that contain multiple items (ORR/OR separators indicate
                # it's a 'one of these' notation, not a clean item name)
                if " OR " in item_val.upper() or "ORR" in item_val.upper() or "MYSTERY" in item_val.upper():
                    logger.debug(f"[PARSE] Skipping multi-item cell: {item_val[:60]}")
                    continue
                # Skip excessively long values (likely data entry notes)
                if len(item_val) > 100:
                    logger.debug(f"[PARSE] Skipping long cell ({len(item_val)} chars): {item_val[:60]}")
                    continue
                kit_data[current_kit_sku]["col0_items"].add(item_val)

    logger.info(f"[PARSE] Total kit groups found across all CSVs: {len(kit_data)}")
    return kit_data


# ---------------------------------------------------------------------------
# Main seeding logic
# ---------------------------------------------------------------------------
def run(dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info("=" * 65)
    logger.info(f"  OBB HISTORICAL KIT SEEDER  ({mode})")
    logger.info("=" * 65)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("[SETUP] Missing SUPABASE_URL or key. Check .env file.")
        sys.exit(1)

    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("[SETUP] Supabase connected.")

    # ------------------------------------------------------------------
    # 1. Load existing data from DB
    # ------------------------------------------------------------------
    logger.info("[SETUP] Loading existing kits from DB...")
    kits_resp = db.table("kits").select("id, sku").execute()
    db_kits_by_sku: dict[str, str] = {}  # canonical_sku -> id
    for k in kits_resp.data or []:
        db_kits_by_sku[k["sku"].strip().upper()] = k["id"]
    logger.info(f"[SETUP] {len(db_kits_by_sku)} kits in DB.")

    logger.info("[SETUP] Loading existing items from DB...")
    items_resp = db.table("items").select("id, sku, name").execute()
    db_items_by_sku: dict[str, dict] = {}  # UPPER_SKU -> {id, name}
    for i in items_resp.data or []:
        if i["sku"]:
            db_items_by_sku[i["sku"].strip().upper()] = {"id": i["id"], "name": i["name"]}
    logger.info(f"[SETUP] {len(db_items_by_sku)} items in DB.")

    logger.info("[SETUP] Loading existing kit_items from DB...")
    ki_resp = db.table("kit_items").select("kit_id, item_id").execute()
    existing_kit_items: set[tuple] = set()  # (kit_id, item_id)
    for ki in ki_resp.data or []:
        existing_kit_items.add((ki["kit_id"], ki["item_id"]))
    logger.info(f"[SETUP] {len(existing_kit_items)} kit_items links in DB.")

    # ------------------------------------------------------------------
    # 2. Parse CSVs
    # ------------------------------------------------------------------
    kit_data = parse_csvs_for_kit_items()

    # ------------------------------------------------------------------
    # 3. Identify missing kits
    # ------------------------------------------------------------------
    missing_kits: dict = {}  # csv_sku -> data
    for csv_sku, data in kit_data.items():
        db_sku = sku_to_db_format(csv_sku).upper()  # OBB-BM-41 KITS
        if db_sku not in db_kits_by_sku:
            missing_kits[csv_sku] = data

    logger.info(f"[ANALYZE] {len(missing_kits)} kit SKUs from CSVs are MISSING from DB:")
    for sku in sorted(missing_kits.keys()):
        count = len(missing_kits[sku]["col0_items"])
        logger.info(f"  {sku} ({missing_kits[sku]['month_label']}) -- {count} variable items in CSV")

    if not missing_kits:
        logger.info("[DONE] No missing kits -- nothing to seed.")
        return

    # ------------------------------------------------------------------
    # 4. Collect all unique items needed
    # ------------------------------------------------------------------
    all_needed_item_skus: dict[str, str] = {}  # UPPER_SKU -> original col0 value
    for data in missing_kits.values():
        for col0_val in data["col0_items"]:
            abb_sku_upper = item_sku_from_col0(col0_val)
            if abb_sku_upper not in all_needed_item_skus:
                all_needed_item_skus[abb_sku_upper] = col0_val

    # Split into found/not found
    items_found = {}    # upper_sku -> item_id
    items_to_create = {}  # upper_sku -> col0_original

    for upper_sku, col0_val in all_needed_item_skus.items():
        if upper_sku in db_items_by_sku:
            items_found[upper_sku] = db_items_by_sku[upper_sku]["id"]
        else:
            items_to_create[upper_sku] = col0_val

    logger.info(f"\n[ITEMS] {len(all_needed_item_skus)} unique items referenced across missing kits:")
    logger.info(f"  Already in DB:  {len(items_found)}")
    logger.info(f"  Need to create: {len(items_to_create)}")

    if items_to_create:
        logger.info("[ITEMS] Items that will be CREATED:")
        for sku, col0_val in sorted(items_to_create.items()):
            name = item_name_from_col0(col0_val)
            logger.info(f"  CREATE item: sku={sku} | name={name}")

    # ------------------------------------------------------------------
    # 5. Compute kit records to insert
    # ------------------------------------------------------------------
    logger.info(f"\n[KITS] Kits that will be CREATED ({len(missing_kits)}):")
    kit_records_to_insert = []
    for csv_sku, data in sorted(missing_kits.items()):
        db_sku = sku_to_db_format(csv_sku)  # OBB-BM-41 KITS
        trimester = extract_trimester(csv_sku)
        size_variant = extract_size_variant(csv_sku)
        age_rank = compute_age_rank(db_sku)

        # Build kit name from month_label and SKU code
        # e.g. csv_sku=OBB-BM-41, batch=BM-41 -> name="MAR 2024 - BM-41"
        batch_code = re.sub(r"^OBB-", "", csv_sku, flags=re.IGNORECASE)
        name = f"{data['month_label']} - {batch_code}"
        is_universal = bool(size_variant == 1 and trimester != 0)

        rec = {
            "sku": db_sku,
            "name": name,
            "trimester": trimester,
            "size_variant": size_variant,
            "is_welcome_kit": False,
            "age_rank": age_rank,
            "age_rank_source": "auto",
            "quantity_available": 0,
            "is_universal": is_universal,
        }
        kit_records_to_insert.append(rec)
        logger.info(
            f"  CREATE kit: {db_sku} | T{trimester} size={size_variant} "
            f"age_rank={age_rank} | {name}"
        )

    # ------------------------------------------------------------------
    # 6. Execute (or just log for dry run)
    # ------------------------------------------------------------------
    stats = {
        "items_created": 0,
        "kits_created": 0,
        "kit_items_linked": 0,
        "errors": 0,
    }

    if dry_run:
        logger.info("\n[DRY RUN] No changes written to DB.")
        stats["items_created"] = len(items_to_create)
        stats["kits_created"] = len(kit_records_to_insert)
        # Estimate kit_items
        link_count = 0
        for csv_sku, data in missing_kits.items():
            for col0_val in data["col0_items"]:
                link_count += 1
        stats["kit_items_linked"] = link_count
    else:
        # 6a. Create missing items
        if items_to_create:
            logger.info(f"\n[ITEMS] Creating {len(items_to_create)} new items...")
            for upper_sku, col0_val in items_to_create.items():
                name = item_name_from_col0(col0_val)
                try:
                    resp = db.table("items").insert({
                        "sku": upper_sku,
                        "name": name,
                        "has_sizing": "[" in col0_val,  # size suffix like [LARGE]
                        "is_therabox": False,
                    }).execute()
                    if resp.data:
                        new_id = resp.data[0]["id"]
                        db_items_by_sku[upper_sku] = {"id": new_id, "name": name}
                        items_found[upper_sku] = new_id
                        stats["items_created"] += 1
                        logger.info(f"  [CREATED item] {upper_sku} -> {new_id}")
                    else:
                        logger.error(f"  [FAIL] Could not create item {upper_sku}: no data returned")
                        stats["errors"] += 1
                except Exception as e:
                    logger.error(f"  [FAIL] Error creating item {upper_sku}: {e}")
                    stats["errors"] += 1

        # 6b. Create missing kit records (upsert by sku for idempotency)
        logger.info(f"\n[KITS] Creating {len(kit_records_to_insert)} kit records...")
        for rec in kit_records_to_insert:
            try:
                resp = db.table("kits").upsert(rec, on_conflict="sku").execute()
                if resp.data:
                    kit_id = resp.data[0]["id"]
                    # Update our local cache
                    db_kits_by_sku[rec["sku"].upper()] = kit_id
                    stats["kits_created"] += 1
                    logger.info(f"  [CREATED kit] {rec['sku']} -> {kit_id}")
                else:
                    logger.error(f"  [FAIL] No data returned for kit {rec['sku']}")
                    stats["errors"] += 1
            except Exception as e:
                logger.error(f"  [FAIL] Error creating kit {rec['sku']}: {e}")
                stats["errors"] += 1

        # 6c. Create kit_items links
        logger.info("\n[KIT_ITEMS] Linking items to kits...")
        kit_items_to_insert = []
        for csv_sku, data in missing_kits.items():
            db_sku_upper = sku_to_db_format(csv_sku).upper()
            kit_id = db_kits_by_sku.get(db_sku_upper)
            if not kit_id:
                logger.warning(f"  [SKIP] No kit_id found for {csv_sku} -- skipping item links")
                continue

            for col0_val in data["col0_items"]:
                upper_sku = item_sku_from_col0(col0_val)
                item_rec = db_items_by_sku.get(upper_sku)
                if not item_rec:
                    logger.warning(f"  [SKIP] Item {upper_sku} not in DB cache -- skipping")
                    continue
                item_id = item_rec["id"]
                if (kit_id, item_id) not in existing_kit_items:
                    kit_items_to_insert.append({"kit_id": kit_id, "item_id": item_id})
                    existing_kit_items.add((kit_id, item_id))

        if kit_items_to_insert:
            logger.info(f"  Inserting {len(kit_items_to_insert)} kit_items records...")
            CHUNK = 50
            for i in range(0, len(kit_items_to_insert), CHUNK):
                chunk = kit_items_to_insert[i:i+CHUNK]
                try:
                    db.table("kit_items").insert(chunk).execute()
                    stats["kit_items_linked"] += len(chunk)
                    logger.info(f"  [OK] Batch {i//CHUNK+1}: inserted {len(chunk)} links")
                except Exception as e:
                    logger.error(f"  [FAIL] kit_items batch {i//CHUNK+1}: {e}")
                    stats["errors"] += 1
        else:
            logger.info("  No new kit_items to insert.")

    # ------------------------------------------------------------------
    # 7. Summary
    # ------------------------------------------------------------------
    logger.info("\n" + "=" * 65)
    logger.info("  SEED SUMMARY")
    logger.info("=" * 65)
    logger.info(f"  Mode:                  {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"  Missing kits found:    {len(missing_kits)}")
    logger.info(f"  Items created:         {stats['items_created']}")
    logger.info(f"  Kits created:          {stats['kits_created']}")
    logger.info(f"  Kit-item links added:  {stats['kit_items_linked']}")
    logger.info(f"  Errors:                {stats['errors']}")
    logger.info("=" * 65)
    if dry_run:
        logger.info("\nDRY RUN -- no DB changes. Remove --dry-run to apply.")
    else:
        logger.info("\nDONE. Re-run import_history.py to link kit_ids on all shipments.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed missing historical OBB kit records from CSV files.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be seeded without touching the DB.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
