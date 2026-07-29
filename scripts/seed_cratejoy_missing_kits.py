#!/usr/bin/env python3
"""
seed_cratejoy_missing_kits.py
=============================
Seeds the 4 kit families that Sheena's Cratejoy history references but that are
missing from the `kits` table (surfaced by import_cratejoy_history.py --dry-run):

    OBB-AA-31
    OBB-AB-31 / OBB-AB-32 / OBB-AB-33 / OBB-AB-34
    OBB-AW-31
    OBB-Y-31 / OBB-Y-32 / OBB-Y-33 / OBB-Y-34

Kit SKU  : 'OBB-AA-31 KITS'   (canonical DB format, trailing ' KITS')
Kit name : 'OBB-AA-31'        (the code, matching the SKU prefix)
Trimester: first digit of the last SKU segment   (…-31 -> T3)
Size var : second digit                          (…-32 -> size 2)
is_welcome_kit: False (regular monthly kits)
is_universal  : True only when the family has a single size variant (AA, AW)

Items  : sku = name = 'OBB-' + the line EXACTLY as Hasan wrote it (verbatim —
         case, spaces, punctuation preserved). This matches the existing
         'OBB-OBB - HELLO BABY JOURNAL' style already in the items table, so
         items that already exist are reused (matched by sku), never duplicated.
         has_sizing = the line contains '[' (e.g. a '[S, M, L, XL]' suffix).

Idempotent: kits upserted by sku; items inserted only if their sku is new;
kit_items links inserted only if missing. Safe to re-run.

USAGE
  python scripts/seed_cratejoy_missing_kits.py --dry-run
  python scripts/seed_cratejoy_missing_kits.py --live
"""
import argparse
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Source data — kit families and their items, verbatim from Hasan's message.
# Each family: list of kit codes (as written) + the shared item lines.
# ---------------------------------------------------------------------------
FAMILIES = [
    {
        "codes": ["OBB-AA-31"],
        "items": [
            "AMINNAH SKINCARE - JELLY BELLY OVERNIGHT MASK",
            "VIOALA VANN - FACIAL CUPPING SET (SET OF 3)",
            "ESSENTIAL ROSE - ROSE GOLD FACE OIL",
            "OBB - MAMA NECKLACE",
            "THE HAPPY SHOPPE - UNPLUG (DIGITAL DETOX CARD)",
            "OBB - BRA EXTENDERS",
            "OBB - WOODEN MILESTONE BLOCK SET",
            "J&L NATURALS - SHINE FACE MASK",
        ],
    },
    {
        "codes": ["OBB-AB-31", "OBB-AB-32", "OBB-AB-33", "OBB-AB-34"],
        "items": [
            "VITAMASQUES - ROSE GOLD HYDROGLOW CREAM",
            "MINIMO - FLAWLESS CHARCOAL FACE SCRUB EXP: 09/2022",
            "MEG COSMETICS - STEAMING HAIR MASK",
            "OBB - HELLO BABY JOURNAL",
            "OBB - MATERNITY MOTHERHOOD TSHIRT [S, M, L, XL]",
            "JOSS + LYN - GLITTER  PEEL OFF MASK",
            "CHLOE EMERALD - ROSE QUARTZ GUA SHA",
            "WILLOW COLLECTIVE - SOUPER MOM AND BABY SPOON SET OF 2",
        ],
    },
    {
        "codes": ["OBB-AW-31"],
        "items": [
            "AYNIL+PLUSHSWADDLEBLANKET(DARKBROWN)",
            "WILLOWCOLLECTIVE+SOYWAX&TEALIGHTS | WILLOWCOLLECTIVE+AUTUMNSTROLLWARMERSTANDPLATE",
            "WILLOWCOLLECTIVE+BABYBANDANADROOLBIB&TEETHINGTOY",
            "SPONGELLE+AUTUMNBLOOMBOXEDFLOWERBUFFER",
            'BEAUTYKITCHEN+LAVENDER"TAKEABREATHER"SUGARSCRUB',
            "MAMABEANIE",
            "REWORK: BIGLITTLEBAR+CHOCOLATECRANBERRYLEMON (YESBAR+SALTEDMAPLEPECAN)",
            "GOLDENBRICKROAD+MAMASGOTTAGROWBOOK",
        ],
    },
    {
        "codes": ["OBB-Y-31", "OBB-Y-32", "OBB-Y-33", "OBB-Y-34"],
        "items": [
            "FIG & YARROW- ROSEHIP ARGAN SERUM",
            "GEN 3 JEWELS - AMAZONITE & LAVA DIFFUSER BRACELET",
            "WAY OF WILL - #03 SOOTHE AND COOL MASSAGE OIL",
            "OBB - MINI BOSS ONESIE",
            "OBB - BIRTHING AFFIRMATION CARDS",
            "AMINNAH SKINCARE - UNICORN BELLY BUTTER",
            "OBB - BELLY BRACE [S, M, L, XL, XXL]",
            "OBB - MAMA BEANIE",
        ],
    },
]

# ---------------------------------------------------------------------------
# Environment / logging
# ---------------------------------------------------------------------------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""
SCRIPT_DIR = Path(__file__).parent

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "seed_cratejoy_missing_kits.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("obb_seed_cj_kits")


# ---------------------------------------------------------------------------
# SKU helpers (mirror seed_old_kits.py / app.py exactly)
# ---------------------------------------------------------------------------
def compute_age_rank(sku: str) -> int:
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
        a, b, c = (ord(prefix[i]) - ord("A") + 1 for i in range(3))
        return a * 676 + b * 26 + c
    return 0


def trimester_of(code: str) -> int:
    """OBB-AA-31 -> last segment '31' -> first char '3' -> T3."""
    seg = code.split("-")[-1].strip()
    return int(seg[0]) if seg and seg[0].isdigit() else 0


def size_variant_of(code: str) -> int:
    """OBB-AB-32 -> '32' -> second char '2' -> size 2. Default 1."""
    seg = code.split("-")[-1].strip()
    if len(seg) >= 2 and seg[1].isdigit():
        return int(seg[1])
    return 1


def kit_sku_db(code: str) -> str:
    return code if code.upper().endswith(" KITS") else f"{code} KITS"


def item_sku_name(line: str) -> str:
    """
    sku = name = 'OBB-' + the line (verbatim, minus any leading 'OBB' brand token).

    Many OBB-own lines already start with an 'OBB - ' brand prefix
    (e.g. 'OBB - HELLO BABY JOURNAL'). We must NOT double it into
    'OBB-OBB - HELLO BABY JOURNAL' — strip that leading token first so the
    result carries exactly ONE 'OBB-' prefix: 'OBB-HELLO BABY JOURNAL'.
    Vendor lines (e.g. 'AMINNAH SKINCARE - ...') are untouched.
    """
    s = line.strip()
    s = re.sub(r"^OBB\b[\s\-]*", "", s, flags=re.IGNORECASE)  # drop leading 'OBB', ' - ', '-', etc.
    return "OBB-" + s


def is_messy(line: str) -> Optional[str]:
    """Flag lines that likely need a human eye before they become a permanent SKU."""
    s = line.strip()
    if "|" in s:
        return "contains '|' (two products on one line?)"
    if s.upper().startswith("REWORK"):
        return "starts with 'REWORK:' (substitution note?)"
    if "EXP:" in s.upper():
        return "contains an expiry note ('EXP:')"
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Seed the 4 missing Cratejoy kit families + their items.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="Preview everything, write NOTHING.")
    g.add_argument("--live", action="store_true", help="Upsert kits, insert items, link kit_items.")
    args = ap.parse_args()

    logger.info("=" * 70)
    logger.info("  SEED MISSING CRATEJOY KITS  —  %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("=" * 70)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set in .env")
        sys.exit(1)

    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # --- Load current DB state ---------------------------------------------
    db_kits = {k["sku"].strip().upper(): k["id"]
               for k in (db.table("kits").select("id, sku").execute().data or [])}
    db_items = {i["sku"].strip().upper(): i["id"]
                for i in (db.table("items").select("id, sku").execute().data or []) if i.get("sku")}
    db_links = {(ki["kit_id"], ki["item_id"])
                for ki in (db.table("kit_items").select("kit_id, item_id").execute().data or [])}
    logger.info("[SETUP] %d kits, %d items, %d kit_items links currently in DB",
                len(db_kits), len(db_items), len(db_links))

    # --- Build the plan -----------------------------------------------------
    kits_plan: list = []      # kit records to upsert
    items_plan: dict = {}     # UPPER_sku -> {sku, name, has_sizing}
    links_plan: list = []     # (kit_code_db_sku_upper, item_upper_sku)
    messy: list = []          # (item_sku, reason)

    for fam in FAMILIES:
        single_variant = len(fam["codes"]) == 1

        # Items (shared across the family's kits)
        fam_item_skus: list = []
        for line in fam["items"]:
            sku = item_sku_name(line)
            up = sku.upper()
            fam_item_skus.append(up)
            if up not in items_plan:
                items_plan[up] = {
                    "sku": sku,
                    "name": sku,               # name = sku, per instruction
                    "has_sizing": "[" in line,
                }
                reason = is_messy(line)
                if reason:
                    messy.append((sku, reason))

        # Kits
        for code in fam["codes"]:
            db_sku = kit_sku_db(code)
            kits_plan.append({
                "sku": db_sku,
                "name": code,                  # name matches the SKU prefix/code
                "trimester": trimester_of(code),
                "size_variant": size_variant_of(code),
                "is_welcome_kit": False,
                "age_rank": compute_age_rank(db_sku),
                "age_rank_source": "auto",
                "quantity_available": 0,
                "is_universal": single_variant,
            })
            for up in fam_item_skus:
                links_plan.append((db_sku.upper(), up))

    # Split new vs existing
    kits_new = [k for k in kits_plan if k["sku"].upper() not in db_kits]
    kits_exist_skus = {k["sku"].upper() for k in kits_plan if k["sku"].upper() in db_kits}
    items_new = {up: v for up, v in items_plan.items() if up not in db_items}
    items_exist = {up: v for up, v in items_plan.items() if up in db_items}

    # --- Report -------------------------------------------------------------
    logger.info("")
    logger.info("KITS (%d total: %d new, %d already exist):", len(kits_plan), len(kits_new), len(kits_exist_skus))
    for k in kits_plan:
        tag = "NEW " if k["sku"].upper() not in db_kits else "have"
        logger.info("  [%s] %-16s T%d sz=%d age_rank=%d uni=%s | name=%s",
                    tag, k["sku"], k["trimester"], k["size_variant"], k["age_rank"], k["is_universal"], k["name"])

    logger.info("")
    logger.info("ITEMS (%d unique: %d new, %d already exist):", len(items_plan), len(items_new), len(items_exist))
    for up, v in items_plan.items():
        tag = "NEW " if up not in db_items else "have"
        logger.info("  [%s] sizing=%s | %s", tag, "Y" if v["has_sizing"] else "n", v["sku"])

    if messy:
        logger.info("")
        logger.info("!! MESSY ITEM LINES — review before --live (they become permanent SKUs verbatim):")
        for sku, reason in messy:
            logger.info("     - %s   <<%s>>", sku, reason)

    logger.info("")
    logger.info("KIT_ITEMS: %d (kit,item) links planned across the families.", len(links_plan))

    # --- Dry run stops here -------------------------------------------------
    if args.dry_run:
        logger.info("")
        logger.info("DRY RUN COMPLETE — nothing written. Re-run with --live to seed.")
        logger.info("Log: %s", SCRIPT_DIR / "seed_cratejoy_missing_kits.log")
        return

    # --- LIVE: items -> kits -> links --------------------------------------
    created_items = created_kits = linked = 0

    # 1. Items
    for up, v in items_new.items():
        try:
            r = db.table("items").insert({
                "sku": v["sku"], "name": v["name"],
                "has_sizing": v["has_sizing"], "is_therabox": False,
            }).execute()
            if r.data:
                db_items[up] = r.data[0]["id"]
                created_items += 1
                logger.info("  [ITEM +] %s", v["sku"])
        except Exception as e:
            logger.error("  [ITEM x] %s: %s", v["sku"], e)

    # 2. Kits (upsert by sku)
    for k in kits_plan:
        try:
            r = db.table("kits").upsert(k, on_conflict="sku").execute()
            if r.data:
                db_kits[k["sku"].upper()] = r.data[0]["id"]
                if k["sku"].upper() not in kits_exist_skus:
                    created_kits += 1
                logger.info("  [KIT +] %s -> %s", k["sku"], r.data[0]["id"])
        except Exception as e:
            logger.error("  [KIT x] %s: %s", k["sku"], e)

    # 3. kit_items links
    to_link = []
    for kit_sku_up, item_up in links_plan:
        kit_id = db_kits.get(kit_sku_up)
        item_id = db_items.get(item_up)
        if not kit_id or not item_id:
            logger.warning("  [LINK skip] kit=%s item=%s (missing id)", kit_sku_up, item_up)
            continue
        if (kit_id, item_id) in db_links:
            continue
        to_link.append({"kit_id": kit_id, "item_id": item_id})
        db_links.add((kit_id, item_id))
    for i in range(0, len(to_link), 50):
        chunk = to_link[i:i + 50]
        try:
            db.table("kit_items").insert(chunk).execute()
            linked += len(chunk)
        except Exception as e:
            logger.error("  [LINK x] batch %d: %s", i // 50 + 1, e)

    logger.info("")
    logger.info("=" * 70)
    logger.info("  SEED COMPLETE — items +%d, kits +%d, kit_items +%d", created_items, created_kits, linked)
    logger.info("=" * 70)
    logger.info("  Next: re-run  python scripts/import_cratejoy_history.py --dry-run")
    logger.info("  to confirm the 4 unknown-SKU warnings are gone (kits now resolve).")


if __name__ == "__main__":
    from _legacy_seed_guard import assert_allowed   # quarantined: see hazard H2
    assert_allowed("seed_cratejoy_missing_kits.py")
    main()
