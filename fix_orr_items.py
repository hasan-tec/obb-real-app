#!/usr/bin/env python3
"""
fix_orr_items.py
-----------------
Fixes missing kit_items caused by seed_old_kits.py skipping "OR/ORR/ORRR"
alternative items in CSV column 0.

What it does:
1. Parses all 24 CSVs to find OR/ORR/ORRR item lines
2. Splits each into individual item names
3. For each item: finds or creates it in the items table
4. Links missing items to the correct kit (kit_items)
5. Backfills missing shipment_items for affected shipments

RUN:
  python fix_orr_items.py --dry-run   # preview changes
  python fix_orr_items.py             # apply to DB
"""

import csv
import glob
import os
import re
import sys
import logging
import argparse
from pathlib import Path
from collections import defaultdict

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""

SCRIPT_DIR = Path(__file__).parent
ORDER_HISTORY_DIR = SCRIPT_DIR.parent / "order history"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "fix_orr_items.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("fix_orr")

# ─── DEDUP MAP ───────────────────────────────────────────────────────────────
# Variant SKUs that map to existing items with slightly different spellings.
# Verified manually — prevents creating duplicate items.
DEDUP_MAP = {
    "OBB-GRAYCATCOOLINGSLEEPEYEMASK":                     "38912808-f03b-4ad8-b6d8-8918692f6505",  # existing: OBB-RAYCATCOOLINGSLEEPEYEMASK
    "OBB-HS+GRAYCATCOOLINGSLEEPEYEMASK":                  "38912808-f03b-4ad8-b6d8-8918692f6505",  # same (HS = TheraBox prefix)
    "OBB-AMMINAHSKINCARE+STRETCHMARKSERUMW/GOLDBEADS":    "734cd311-e1b4-46ce-9b79-4efab82d6ee7",  # existing: AMINNAHSKINCARE (single M)
    "OBB-WILLOWCOLLECTIVE+SOUPERMOM&BABYSPOONSET":        "a77c5aac-3ed7-4384-acc2-0cacbcfce434",  # existing: SOUPERMOMANDBABYSPOONSETOF2
    "OBB-JANUARYMOON+SPOON&FORKFEEDINGSETMIXEDCOLORS":   "8f56d42d-60ff-4459-a801-491480a16052",  # existing: SPOON&FORKFEEDINGSET
    "OBB-EARTHHARBORNATURALS+AQUAAURAEYECREAM":           "ccd2cca7-3019-481b-a4f2-2eb5fbb7fdf8",  # existing: EARTHHARBOR (no NATURALS)
    "OBB-KEABABIES+HORIZONESILICONEBIBS":                 "b7613bad-853c-4354-a3a5-d991324a204f",  # existing: HORIZONSILICONEBIBS (no E)
}

# ORR-embedded item that needs merging into its clean version
ORR_MERGE = {
    "bad_id":  "2c90e858-8618-4ec5-bcfc-072c09d29380",  # SKU has " ORR AMMINAHSKINCARE..." appended
    "good_id": "fadb8789-5b70-4607-a7dd-b2e2953446dd",  # OBB-ATTITUDE+STRETCHMARKBODYOILBLOOMINGBELLY (clean)
}

# ─── BETTER NAMES ────────────────────────────────────────────────────────────────
# Proper readable names for newly created items (raw CSV names are ugly)
BETTER_NAMES = {
    "OBB-WINKNATURALS+CHESTRUB": "Wink Naturals - Chest Rub",
    "OBB-MOMMYKNOWSBEST+NIPPLECREAM": "Mommy Knows Best - Nipple Cream",
    "OBB-AMINNAHSKINCARE+BIRTHDAYCAKEBODYBUTTER": "Aminnah Skincare - Birthday Cake Body Butter",
    "OBB-TRUEARTH+BABYECOLAUNDRYSTRIPS": "True Earth - Baby Eco Laundry Strips",
    "OBB-TRUEARTH+FRESHLINEN": "True Earth - Fresh Linen",
    "OBB-AYNIL+GRAYGRADIENTSCARF": "Aynil - Gray Gradient Scarf",
    "OBB-WHITEBOXBABYANDMAMABRACELETS": "White Box Baby and Mama Bracelets",
}


def item_sku_from_raw(raw: str) -> str:
    """Convert raw CSV item name to DB item SKU (OBB- prefix, uppercase)."""
    v = raw.strip().upper()
    # Remove EXP dates, trailing numbers, stock counts
    v = re.sub(r'\s+EXP\s+\d+/\d+.*', '', v)
    v = re.sub(r'\s+-\s+\d+$', '', v)  # " - 130" trailing stock count
    v = re.sub(r'\s+\d+$', '', v)      # trailing numbers like " 58" or " 43"
    v = v.strip()
    if v.startswith("OBB-"):
        return v
    return f"OBB-{v}"


def item_name_from_raw(raw: str) -> str:
    """Convert raw CSV item name to readable name."""
    v = raw.strip()
    v = re.sub(r'\s+EXP\s+\d+/\d+.*', '', v, flags=re.IGNORECASE)
    v = re.sub(r'\s+-\s+\d+$', '', v)
    v = re.sub(r'\s+\d+$', '', v)
    return v.strip().title().replace("+", " ").replace("  ", " ")


def parse_orr_items() -> list:
    """
    Parse all CSVs to find OR/ORR/ORRR items.
    Returns list of dicts: { kit_sku, items: [(raw, sku, name), ...] }
    """
    csv_files = sorted(glob.glob(str(ORDER_HISTORY_DIR / "*.csv")))
    results = []

    for filepath_str in csv_files:
        with open(filepath_str, encoding="utf-8-sig", errors="replace") as f:
            rows = list(csv.reader(f))

        current_kit = None
        for row in rows[2:]:
            col0 = row[0].strip() if row else ""
            col2 = row[2].strip() if len(row) > 2 else ""
            if not col0:
                continue
            if "OBB-" in col0 and "@" not in col0 and not col2.strip():
                sku = re.sub(r"\s+Kits?", "", col0.strip(), flags=re.IGNORECASE).strip()
                current_kit = sku
                continue
            if current_kit and "@" in col2:
                upper = col0.upper()
                if " ORR " in upper or " ORRR " in upper or " OR " in upper:
                    # Split into individual items
                    parts = re.split(r'\s+ORRR\s+|\s+ORR\s+|\s+OR\s+', col0.strip(), flags=re.IGNORECASE)
                    items = []
                    for p in parts:
                        p = p.strip()
                        if not p:
                            continue
                        sku = item_sku_from_raw(p)
                        name = item_name_from_raw(p)
                        items.append({"raw": p, "sku": sku, "name": name})

                    kit_db_sku = current_kit
                    if not kit_db_sku.upper().endswith(" KITS"):
                        kit_db_sku += " KITS"
                    results.append({
                        "kit_sku": kit_db_sku.upper(),
                        "items": items,
                        "file": os.path.basename(filepath_str),
                    })

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no DB writes")
    args = parser.parse_args()

    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info(f"{'[DRY RUN] ' if args.dry_run else ''}Starting OR/ORR item fix")

    # Step 1: Parse all OR/ORR items from CSVs
    orr_entries = parse_orr_items()
    logger.info(f"Found {len(orr_entries)} OR/ORR entries across CSVs")

    # Step 2: Load existing items and kits from DB
    all_items = db.table("items").select("id,name,sku").execute()
    item_by_sku = {i["sku"].upper(): i for i in all_items.data}
    logger.info(f"Loaded {len(item_by_sku)} items from DB")

    # Inject DEDUP_MAP so variant SKUs resolve to existing items automatically
    item_by_id = {i["id"]: i for i in all_items.data}
    for variant_sku, existing_id in DEDUP_MAP.items():
        if existing_id in item_by_id and variant_sku.upper() not in item_by_sku:
            item_by_sku[variant_sku.upper()] = item_by_id[existing_id]
            logger.info(f"[DEDUP] {variant_sku} -> {item_by_id[existing_id]['name']} ({item_by_id[existing_id]['sku']})")
        elif existing_id not in item_by_id:
            logger.warning(f"[DEDUP] Target item {existing_id} not found for {variant_sku}")

    all_kits = db.table("kits").select("id,sku").execute()
    kit_by_sku = {k["sku"].upper(): k for k in all_kits.data}
    logger.info(f"Loaded {len(kit_by_sku)} kits from DB")

    # Load existing kit_items
    all_ki = []
    offset = 0
    while True:
        batch = db.table("kit_items").select("kit_id,item_id").range(offset, offset + 999).execute()
        all_ki.extend(batch.data)
        if len(batch.data) < 1000:
            break
        offset += 1000
    existing_kit_items = set()
    for ki in all_ki:
        existing_kit_items.add((ki["kit_id"], ki["item_id"]))
    logger.info(f"Loaded {len(existing_kit_items)} existing kit_items")

    # Step 2.5: Merge ORR-embedded Attitude item into clean version
    logger.info("=== MERGING ORR-EMBEDDED ITEMS ===")
    bad_id = ORR_MERGE["bad_id"]
    good_id = ORR_MERGE["good_id"]
    bad_item = item_by_id.get(bad_id)

    if bad_item:
        logger.info(f"Merging ORR-embedded item: {bad_item['sku']}")
        logger.info(f"  -> into clean item: {item_by_id[good_id]['sku']}")
        if not args.dry_run:
            # 1. Update kit_items: move references from bad → good
            ki_refs = db.table("kit_items").select("kit_id").eq("item_id", bad_id).execute()
            for ki in ki_refs.data:
                existing = db.table("kit_items").select("kit_id").eq("kit_id", ki["kit_id"]).eq("item_id", good_id).execute()
                if not existing.data:
                    db.table("kit_items").update({"item_id": good_id}).eq("kit_id", ki["kit_id"]).eq("item_id", bad_id).execute()
                    logger.info(f"  Updated kit_item for kit {ki['kit_id']}: bad -> good")
                else:
                    db.table("kit_items").delete().eq("kit_id", ki["kit_id"]).eq("item_id", bad_id).execute()
                    logger.info(f"  Deleted duplicate kit_item for kit {ki['kit_id']}")
                # Keep local cache in sync
                existing_kit_items.discard((ki["kit_id"], bad_id))
                existing_kit_items.add((ki["kit_id"], good_id))

            # 2. Update shipment_items: move all references from bad → good
            si_refs = db.table("shipment_items").select("shipment_id").eq("item_id", bad_id).execute()
            merged_si = 0
            for si in si_refs.data:
                existing = db.table("shipment_items").select("shipment_id").eq("shipment_id", si["shipment_id"]).eq("item_id", good_id).execute()
                if not existing.data:
                    db.table("shipment_items").update({"item_id": good_id}).eq("shipment_id", si["shipment_id"]).eq("item_id", bad_id).execute()
                else:
                    db.table("shipment_items").delete().eq("shipment_id", si["shipment_id"]).eq("item_id", bad_id).execute()
                merged_si += 1
            logger.info(f"  Merged {merged_si} shipment_items")

            # 3. Delete the ORR-embedded item
            db.table("items").delete().eq("id", bad_id).execute()
            logger.info(f"  Deleted ORR-embedded item {bad_id}")

            # Update local cache
            old_sku = bad_item["sku"].upper()
            if old_sku in item_by_sku:
                del item_by_sku[old_sku]
            if bad_id in item_by_id:
                del item_by_id[bad_id]
        else:
            logger.info(f"  [DRY RUN] Would merge kit_items + 25 shipment_items, then delete item")
    else:
        logger.info("ORR-embedded item already cleaned or not found — skipping")

    # Step 3: Process each OR/ORR entry
    items_created = 0
    kit_items_added = 0
    kits_affected = set()

    # Deduplicate: same kit+item might appear in multiple CSVs
    seen_kit_item_pairs = set()

    for entry in orr_entries:
        kit_sku = entry["kit_sku"]
        kit = kit_by_sku.get(kit_sku)
        if not kit:
            logger.warning(f"Kit {kit_sku} not found in DB — skipping")
            continue

        kit_id = kit["id"]

        for item_info in entry["items"]:
            item_sku = item_info["sku"].upper()
            item_name = item_info["name"]
            pair_key = (kit_sku, item_sku)

            if pair_key in seen_kit_item_pairs:
                continue
            seen_kit_item_pairs.add(pair_key)

            # Find or create item
            item = item_by_sku.get(item_sku)
            if not item:
                # Try fuzzy match by removing prefix variations
                alt_sku = item_sku.replace("OBB-HS+", "OBB-HAPPYSHOPPE+")
                item = item_by_sku.get(alt_sku)

            if not item:
                # Use proper name from BETTER_NAMES if available
                display_name = BETTER_NAMES.get(item_sku, item_name)
                logger.info(f"  [CREATE ITEM] {item_sku} -> '{display_name}'")
                if not args.dry_run:
                    try:
                        result = db.table("items").insert({
                            "name": display_name,
                            "sku": item_sku,
                        }).execute()
                        item = result.data[0]
                        item_by_sku[item_sku] = item
                        items_created += 1
                    except Exception as e:
                        logger.error(f"  Failed to create item {item_sku}: {e}")
                        continue
                else:
                    items_created += 1
                    item = {"id": f"DRY-RUN-{item_sku}", "sku": item_sku, "name": display_name}
                    item_by_sku[item_sku] = item

            # Check if kit_items link exists
            item_id = item["id"]
            if (kit_id, item_id) in existing_kit_items:
                logger.debug(f"  Kit {kit_sku} already has item {item_sku}")
                continue

            logger.info(f"  [ADD KIT_ITEM] {kit_sku} <- {item_sku} ({item.get('name', item_name)})")
            if not args.dry_run:
                try:
                    db.table("kit_items").insert({
                        "kit_id": kit_id,
                        "item_id": item_id,
                        "quantity": 1,
                    }).execute()
                    existing_kit_items.add((kit_id, item_id))
                    kit_items_added += 1
                    kits_affected.add(kit_sku)
                except Exception as e:
                    logger.error(f"  Failed to add kit_item: {e}")
            else:
                kit_items_added += 1
                kits_affected.add(kit_sku)

    logger.info(f"\n{'[DRY RUN] ' if args.dry_run else ''}=== SUMMARY ===")
    logger.info(f"Items created: {items_created}")
    logger.info(f"Kit_items added: {kit_items_added}")
    logger.info(f"Kits affected: {len(kits_affected)} -> {sorted(kits_affected)}")

    if not args.dry_run and kits_affected:
        # Step 4: Backfill shipment_items for affected kits
        logger.info(f"\n=== BACKFILLING SHIPMENT_ITEMS FOR {len(kits_affected)} AFFECTED KITS ===")

        # Load all shipment_items for efficient checking
        all_si = set()
        offset = 0
        while True:
            batch = db.table("shipment_items").select("shipment_id,item_id").range(offset, offset + 999).execute()
            for si in batch.data:
                all_si.add((si["shipment_id"], si["item_id"]))
            if len(batch.data) < 1000:
                break
            offset += 1000
        logger.info(f"Loaded {len(all_si)} existing shipment_items for dedup")

        total_backfilled = 0
        total_errors = 0

        for kit_sku in sorted(kits_affected):
            kit = kit_by_sku[kit_sku]
            kit_id = kit["id"]

            # Get current kit_items (after additions)
            kit_items = db.table("kit_items").select("item_id").eq("kit_id", kit_id).execute()
            kit_item_ids = {ki["item_id"] for ki in kit_items.data}

            # Get all shipments for this kit
            shipments = db.table("shipments").select("id").eq("kit_sku", kit_sku).execute()
            logger.info(f"Kit {kit_sku}: {len(kit_item_ids)} items, {len(shipments.data)} shipments")

            for ship in shipments.data:
                sid = ship["id"]
                for item_id in kit_item_ids:
                    if (sid, item_id) not in all_si:
                        try:
                            db.table("shipment_items").insert({
                                "shipment_id": sid,
                                "item_id": item_id,
                            }).execute()
                            all_si.add((sid, item_id))
                            total_backfilled += 1
                        except Exception as e:
                            total_errors += 1
                            if total_errors <= 5:
                                logger.error(f"  Backfill error: {e}")

        logger.info(f"\nShipment_items backfilled: {total_backfilled}")
        logger.info(f"Shipment_items errors: {total_errors}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
