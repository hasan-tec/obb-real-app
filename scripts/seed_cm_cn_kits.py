"""
seed_cm_cn_kits.py — Seed kits, items, kit_items, and item_alternatives
from the May '26 CM and June '26 CN monthly boxing CSV files.

Usage:
    python scripts/seed_cm_cn_kits.py --dry-run   (default)
    python scripts/seed_cm_cn_kits.py --live
"""

import argparse
import csv
import logging
import os
import re
import sys
from pathlib import Path
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from supabase import create_client

logger = logging.getLogger("obb.seed_cm_cn")
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# ─── CSV Paths ────────────────────────────────────────────────────────────────

ORDER_HISTORY = Path(__file__).parent.parent.parent / "order history"

CSV_FILES = [
    ORDER_HISTORY / "Oh Baby Boxes - Monthly Boxing_Customer Kit Assignment - MAY '26 CM.csv",
    ORDER_HISTORY / "Oh Baby Boxes - Monthly Boxing_Customer Kit Assignment - JUNE '26 CN.csv",
]

# ─── Kit metadata ─────────────────────────────────────────────────────────────

def csv_kit_to_canonical_sku(raw: str) -> str:
    """CM41 -> OBB-CM-41 KITS  |  OBB-BP-11 Kits -> OBB-BP-11 KITS"""
    s = raw.strip()
    m = re.match(r"^([A-Z]{2})(\d{2})$", s)
    if m:
        return f"OBB-{m.group(1)}-{m.group(2)} KITS"
    s = re.sub(r"\s+Kits?$", "", s, flags=re.IGNORECASE).strip()
    if not s.upper().endswith(" KITS"):
        s = s + " KITS"
    return s.upper()


def kit_metadata(canonical_sku: str) -> dict:
    """Derive trimester, size_variant, is_universal, age_rank from canonical SKU."""
    m = re.search(r"OBB-([A-Z]{2})-(\d{2})\s+KITS", canonical_sku)
    if not m:
        return {"trimester": 1, "size_variant": 1, "is_universal": False, "age_rank": 0}

    batch = m.group(1)
    digits = m.group(2)
    trimester = int(digits[0])
    size_variant = int(digits[1])

    def letter_pos(c):
        return ord(c.upper()) - ord("A") + 1

    age_rank = letter_pos(batch[0]) * 26 + letter_pos(batch[1])

    # is_universal: CN and BP batches have no size variants; CM T4 has no size variants
    is_universal = (batch in ("CN", "BP")) or (batch == "CM" and trimester == 4)

    return {
        "trimester": trimester,
        "size_variant": size_variant,
        "is_universal": is_universal,
        "age_rank": age_rank,
    }


# ─── CSV parsing ─────────────────────────────────────────────────────────────

OR_PATTERN = re.compile(r"\s+ORR*\s+", re.IGNORECASE)
BONUS_PREFIX = re.compile(r"^BONUS\s*:\s*", re.IGNORECASE)


def clean_item_name(raw: str) -> str:
    return BONUS_PREFIX.sub("", raw.strip()).strip()


def is_kit_header(row: list) -> bool:
    while len(row) < 3:
        row.append("")
    col_a = row[0].strip()
    col_b = row[1].strip()
    col_c = row[2].strip()
    if not col_a or col_b or col_c:
        return False
    if re.search(r"\bKits?\b", col_a, re.IGNORECASE):
        return True
    return bool(re.match(r"^[A-Z]{2}\d{2}$", col_a))


def parse_csv(fpath: Path) -> dict:
    """Returns {canonical_kit_sku: [ [item_a, item_b?], ... ]} where inner list = OR slot."""
    kit_slots: dict = defaultdict(list)
    with open(fpath, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))

    cur_kit = None
    for i, row in enumerate(rows):
        while len(row) < 5:
            row.append("")
        if i < 2:
            continue
        col_a = row[0].strip()

        if is_kit_header(row):
            cur_kit = csv_kit_to_canonical_sku(col_a)
            continue

        if cur_kit and col_a:
            raw = BONUS_PREFIX.sub("", col_a).strip()
            if not raw:
                continue
            parts = OR_PATTERN.split(raw)
            names = [clean_item_name(p) for p in parts if clean_item_name(p)]
            if names:
                kit_slots[cur_kit].append(names)

    return dict(kit_slots)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(live: bool = False):
    mode = "LIVE" if live else "DRY-RUN"
    logger.info("[SEED] Starting in %s mode", mode)

    for f in CSV_FILES:
        if not f.exists():
            logger.error("[SEED] CSV not found: %s", f)
            sys.exit(1)

    # Parse both CSVs and merge
    all_kit_data: dict = {}
    for f in CSV_FILES:
        parsed = parse_csv(f)
        logger.info("[SEED] Parsed %s -> %d kit groups", f.name, len(parsed))
        for kit_sku, slots in parsed.items():
            if kit_sku not in all_kit_data:
                all_kit_data[kit_sku] = []
            all_kit_data[kit_sku].extend(slots)

    # Deduplicate slots per kit (BP-11 appears in both CSVs)
    for kit_sku in all_kit_data:
        seen = set()
        deduped = []
        for slot in all_kit_data[kit_sku]:
            key = tuple(sorted(slot))
            if key not in seen:
                seen.add(key)
                deduped.append(slot)
        all_kit_data[kit_sku] = deduped

    # Collect all unique item names
    all_item_names: set = set()
    for slots in all_kit_data.values():
        for slot in slots:
            for name in slot:
                all_item_names.add(name)

    logger.info("[SEED] Total kits: %d, total unique items: %d", len(all_kit_data), len(all_item_names))

    # Connect to DB
    db = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])

    # Fetch existing state
    existing_kits_rows = db.table("kits").select("id, sku").execute().data or []
    existing_items_rows = db.table("items").select("id, name, sku").execute().data or []
    # Paginate kit_items (can exceed Supabase default 1000-row limit)
    existing_kit_items_rows = []
    offset = 0
    batch_size = 1000
    while True:
        batch = db.table("kit_items").select("kit_id, item_id").range(offset, offset + batch_size - 1).execute()
        if not batch.data:
            break
        existing_kit_items_rows.extend(batch.data)
        if len(batch.data) < batch_size:
            break
        offset += batch_size
    existing_alts_rows = db.table("item_alternatives").select("item_id, alternative_item_id").execute().data or []

    kit_by_sku: dict = {r["sku"]: r["id"] for r in existing_kits_rows}
    item_by_sku: dict = {r["sku"]: r["id"] for r in existing_items_rows}
    kit_items_set: set = {(r["kit_id"], r["item_id"]) for r in existing_kit_items_rows}
    alts_set: set = {(r["item_id"], r["alternative_item_id"]) for r in existing_alts_rows}

    # ── Plan ─────────────────────────────────────────────────────────────────

    items_to_create = []
    items_exist = []
    for name in sorted(all_item_names):
        sku = "OBB-" + name.upper()
        (items_to_create if sku not in item_by_sku else items_exist).append(name)

    kits_to_create = []
    kits_exist = []
    for kit_sku in sorted(all_kit_data.keys()):
        (kits_to_create if kit_sku not in kit_by_sku else kits_exist).append(kit_sku)

    # kit_items: compute based on current DB + items we'll create
    # For dry-run, assume new items/kits will get IDs in live run
    kit_items_plan = []  # (kit_sku, item_name, "CREATE"|"SKIP")
    for kit_sku, slots in sorted(all_kit_data.items()):
        k_id = kit_by_sku.get(kit_sku)
        for slot in slots:
            for name in slot:
                i_id = item_by_sku.get("OBB-" + name.upper())
                if k_id and i_id and (k_id, i_id) in kit_items_set:
                    kit_items_plan.append((kit_sku, name, "SKIP"))
                else:
                    kit_items_plan.append((kit_sku, name, "CREATE"))

    # item_alternatives
    alts_plan = []  # (name_a, name_b, "CREATE"|"SKIP")
    seen_pairs = set()
    for kit_sku, slots in sorted(all_kit_data.items()):
        for slot in slots:
            if len(slot) < 2:
                continue
            for i in range(len(slot)):
                for j in range(i + 1, len(slot)):
                    a, b = slot[i], slot[j]
                    pair_key = tuple(sorted([a, b]))
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)
                    a_id = item_by_sku.get("OBB-" + a.upper())
                    b_id = item_by_sku.get("OBB-" + b.upper())
                    if a_id and b_id and (a_id, b_id) in alts_set:
                        alts_plan.append((a, b, "SKIP"))
                    else:
                        alts_plan.append((a, b, "CREATE"))

    # ── Print report ──────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print(f"  SEED CM/CN KITS  [{mode}]")
    print("=" * 72)

    print(f"\n[KITS] {len(kits_to_create)} to create | {len(kits_exist)} already exist")
    for sku in kits_to_create:
        meta = kit_metadata(sku)
        print(f"  CREATE  {sku}  T{meta['trimester']} size={meta['size_variant']} "
              f"universal={meta['is_universal']} age_rank={meta['age_rank']}")
    for sku in kits_exist:
        print(f"  SKIP    {sku}  (already in DB)")

    print(f"\n[ITEMS] {len(items_to_create)} to create | {len(items_exist)} already exist")
    for name in items_to_create:
        print(f"  CREATE  OBB-{name.upper()}")
    for name in items_exist:
        print(f"  SKIP    OBB-{name.upper()}")

    creates = [(k, n) for k, n, s in kit_items_plan if s == "CREATE"]
    skips = [(k, n) for k, n, s in kit_items_plan if s == "SKIP"]
    print(f"\n[KIT_ITEMS] {len(creates)} to create | {len(skips)} already exist")
    for kit_sku, name in creates:
        print(f"  LINK    {kit_sku}  ->  {name}")
    for kit_sku, name in skips:
        print(f"  SKIP    {kit_sku}  ->  {name}")

    alt_creates = [(a, b) for a, b, s in alts_plan if s == "CREATE"]
    alt_skips = [(a, b) for a, b, s in alts_plan if s == "SKIP"]
    print(f"\n[ITEM_ALTERNATIVES] {len(alt_creates)} pairs to create | {len(alt_skips)} already exist")
    for a, b in alt_creates:
        print(f"  LINK    {a}  <->  {b}")
    for a, b in alt_skips:
        print(f"  SKIP    {a}  <->  {b}")

    print(f"\n  TOTAL CREATES: kits={len(kits_to_create)} items={len(items_to_create)} "
          f"kit_items={len(creates)} alt_pairs={len(alt_creates)}")
    print("=" * 72)

    if not live:
        print("\nDry run complete. Run with --live to execute.\n")
        return

    # ── LIVE ─────────────────────────────────────────────────────────────────
    print("\nExecuting live creates...\n")
    errors = []

    # 1. Items
    for name in items_to_create:
        sku = "OBB-" + name.upper()
        try:
            result = db.table("items").insert({"name": name, "sku": sku}).execute()
            new_id = result.data[0]["id"]
            item_by_sku[sku] = new_id
            logger.info("[SEED] Created item: sku=%s id=%s", sku, new_id)
            print(f"  OK    item  {sku}")
        except Exception as e:
            if "duplicate" in str(e).lower() or "23505" in str(e):
                row = db.table("items").select("id").eq("sku", sku).execute()
                if row.data:
                    item_by_sku[sku] = row.data[0]["id"]
                print(f"  SKIP  item  {sku}  (dup)")
            else:
                msg = f"item {sku}: {e}"
                logger.error("[SEED] %s", msg, exc_info=True)
                errors.append(msg)
                print(f"  FAIL  item  {sku}  error={e}")

    # 2. Kits
    for kit_sku in kits_to_create:
        meta = kit_metadata(kit_sku)
        try:
            result = db.table("kits").insert({
                "sku": kit_sku,
                "trimester": meta["trimester"],
                "size_variant": meta["size_variant"],
                "is_welcome_kit": False,
                "is_universal": meta["is_universal"],
                "age_rank": meta["age_rank"],
                "age_rank_source": "auto",
            }).execute()
            new_id = result.data[0]["id"]
            kit_by_sku[kit_sku] = new_id
            logger.info("[SEED] Created kit: sku=%s id=%s", kit_sku, new_id)
            print(f"  OK    kit   {kit_sku}")
        except Exception as e:
            if "duplicate" in str(e).lower() or "23505" in str(e):
                row = db.table("kits").select("id").eq("sku", kit_sku).execute()
                if row.data:
                    kit_by_sku[kit_sku] = row.data[0]["id"]
                print(f"  SKIP  kit   {kit_sku}  (dup)")
            else:
                msg = f"kit {kit_sku}: {e}"
                logger.error("[SEED] %s", msg, exc_info=True)
                errors.append(msg)
                print(f"  FAIL  kit   {kit_sku}  error={e}")

    # 3. Kit items
    ki_ok = 0
    ki_fail = 0
    for kit_sku, name, _ in kit_items_plan:
        k_id = kit_by_sku.get(kit_sku)
        i_id = item_by_sku.get("OBB-" + name.upper())
        if not k_id or not i_id:
            logger.warning("[SEED] Cannot link kit_item — kit=%s item=%s k_id=%s i_id=%s",
                           kit_sku, name, k_id, i_id)
            errors.append(f"kit_item no id: {kit_sku} -> {name}")
            ki_fail += 1
            continue
        pair = (k_id, i_id)
        if pair in kit_items_set:
            continue
        try:
            db.table("kit_items").insert({"kit_id": k_id, "item_id": i_id, "quantity": 1}).execute()
            kit_items_set.add(pair)
            ki_ok += 1
        except Exception as e:
            if "duplicate" in str(e).lower() or "23505" in str(e):
                kit_items_set.add(pair)
                ki_ok += 1
            else:
                msg = f"kit_item {kit_sku}->{name}: {e}"
                logger.error("[SEED] %s", msg, exc_info=True)
                errors.append(msg)
                ki_fail += 1

    logger.info("[SEED] kit_items: ok=%d fail=%d", ki_ok, ki_fail)
    print(f"  kit_items: {ki_ok} created, {ki_fail} failed")

    # 4. Item alternatives
    alt_ok = 0
    alt_fail = 0
    for a_name, b_name, _ in alts_plan:
        a_id = item_by_sku.get("OBB-" + a_name.upper())
        b_id = item_by_sku.get("OBB-" + b_name.upper())
        if not a_id or not b_id:
            msg = f"alt no id: {a_name} <-> {b_name}"
            logger.warning("[SEED] %s", msg)
            errors.append(msg)
            alt_fail += 1
            continue
        for x_id, y_id in [(a_id, b_id), (b_id, a_id)]:
            if (x_id, y_id) in alts_set:
                continue
            try:
                db.table("item_alternatives").insert(
                    {"item_id": x_id, "alternative_item_id": y_id}
                ).execute()
                alts_set.add((x_id, y_id))
                alt_ok += 1
            except Exception as e:
                if "duplicate" in str(e).lower() or "23505" in str(e):
                    alts_set.add((x_id, y_id))
                    alt_ok += 1
                else:
                    msg = f"alt {a_name}<->{b_name}: {e}"
                    logger.error("[SEED] %s", msg, exc_info=True)
                    errors.append(msg)
                    alt_fail += 1

    logger.info("[SEED] item_alternatives: ok=%d fail=%d", alt_ok, alt_fail)
    print(f"  item_alternatives: {alt_ok} direction-rows created, {alt_fail} failed")

    if errors:
        print(f"\n  ERRORS ({len(errors)}):")
        for err in errors:
            print(f"    - {err}")
    else:
        print("\n  All creates succeeded with 0 errors.")

    print("\nLive run complete.\n")


if __name__ == "__main__":
    from _legacy_seed_guard import assert_allowed   # quarantined: see hazard H2
    assert_allowed("seed_cm_cn_kits.py")
    parser = argparse.ArgumentParser(description="Seed CM/CN kits, items, and links")
    parser.add_argument("--live", action="store_true", help="Execute creates (default is dry-run)")
    args = parser.parse_args()
    main(live=args.live)
