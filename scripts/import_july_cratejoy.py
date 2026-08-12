#!/usr/bin/env python3
"""
import_july_cratejoy.py — July 2026 Cratejoy renewals
=====================================================
Imports the four "UPDATED JULY RENEWALS - CRATEJOY T{1..4}.csv" files that Sheena
re-exported WITH the subscriber email column.

WHY A SEPARATE SCRIPT
---------------------
These files are NOT the 23-column Shopify export that import_history.py understands.
They are a hand-built 8-column layout:

    [0] item SKU        (or a kit header row, or blank)
    [1] customer name   (recipient — often NOT the account holder, gift subs)
    [2] EMAIL           <- the identity key; added in the "UPDATED" re-export
    [3] due date        ISO  YYYY-MM-DD
    [4] clothing size   SM / MED / L / XL
    [5] baby gender
    [6] wants daddy item

Kit header rows carry the kit SKU: bare ("CO21") or semi-formatted ("OBB-BP-11 Kits").
Rows under a header belong to that kit. Column [0] item names are IGNORED — shipment
contents come from the kit's existing kit_items rows, never from parsing those strings.

HARD RULE — NEVER SEED kits OR items
------------------------------------
This script only READS `kits` and `kit_items` to resolve a SKU to an existing row.
If any kit SKU in the CSVs does not resolve, it ABORTS and reports it so Sheena can
create the kit in the app. It never inserts a kit or an item, under any circumstance.

IDENTITY
--------
Match is by email only. Names frequently differ from the DB record because Cratejoy
gift subscriptions carry the purchaser's email and the recipient's name; this was
verified — 83 of 84 matched rows agree with the DB on BOTH due_date and clothing_size.

USAGE
  python scripts/import_july_cratejoy.py --dry-run
  python scripts/import_july_cratejoy.py --execute
"""
import argparse
import csv
import logging
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""

SCRIPT_DIR = Path(__file__).parent
CSV_DIR = Path.home() / "Downloads"
SHIP_DATE = "2026-07-15"          # July 2026 cycle
YEAR_MONTH = "2026-07"
PLATFORM = "cratejoy"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "import_july_cratejoy.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("obb_cj_july")

# Corrupted addresses in the source file, corrected by Hasan after confirming the real
# values. Pattern was a find/replace mishap: "user@" + "user@domain" + "domain".
EMAIL_FIXES = {
    "robbynielsen@robbynielsen@gmail.comgmail.com": "robbynielsen@gmail.com",
    "mrsreault@mrsreault@yahoo.comyahoo.com": "mrsreault@yahoo.com",
}

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
SIZE_MAP = {"s": "S", "sm": "S", "small": "S", "m": "M", "med": "M", "medium": "M",
            "l": "L", "lg": "L", "large": "L", "xl": "XL", "xxl": "XL", "2xl": "XL"}


def paginate(db, table, cols):
    """Supabase caps a bare .execute() at 1000 rows — always page."""
    rows, offset = [], 0
    while True:
        batch = db.table(table).select(cols).range(offset, offset + 999).execute()
        rows.extend(batch.data or [])
        if len(batch.data or []) < 1000:
            return rows
        offset += 1000


def parse_kit_sku(raw: str) -> str:
    """Same normalisation as import_history.py: 'CO21'->'OBB-CO-21 KITS'; strip ' Kits'."""
    s = raw.strip()
    m = re.match(r"^([A-Z]{2})(\d{2})$", s)
    if m:
        return f"OBB-{m.group(1)}-{m.group(2)} KITS"
    return re.sub(r"\s+Kits?$", "", s, flags=re.IGNORECASE).strip()


def is_kit_header(col_a: str, col_b: str) -> bool:
    a = col_a.strip()
    if not a or col_b.strip():
        return False
    if re.search(r"\bKits?\b", a, re.IGNORECASE):
        return True
    return bool(re.match(r"^[A-Z]{2}\d{2}$", a))


def parse_rows() -> tuple[list, list]:
    """Parse all four CSVs. Returns (usable_rows, skipped_rows)."""
    usable, skipped = [], []
    for tri in (1, 2, 3, 4):
        path = CSV_DIR / f"UPDATED JULY RENEWALS - CRATEJOY T{tri}.csv"
        if not path.exists():
            logger.error("Missing file: %s", path)
            sys.exit(1)
        current_kit = None
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line_no, row in enumerate(csv.reader(fh), start=1):
                while len(row) < 8:
                    row.append("")
                col_a = row[0].strip()
                if re.fullmatch(r"T\d", col_a, re.IGNORECASE) and not row[1].strip():
                    continue                      # "T1"/"T2" banner row
                if is_kit_header(col_a, row[1]):
                    current_kit = col_a
                    continue
                name, email = row[1].strip(), row[2].strip().lower()
                if not name and not email:
                    continue                      # item-only or blank row
                email = EMAIL_FIXES.get(email, email)
                rec = {
                    "trimester": tri, "kit_raw": current_kit, "name": name, "email": email,
                    "due_date": row[3].strip(), "size": SIZE_MAP.get(row[4].strip().lower()),
                    "gender": row[5].strip(), "daddy": row[6].strip(),
                    "src": f"T{tri} line {line_no}",
                }
                if not email or not EMAIL_RE.match(email):
                    rec["skip_reason"] = "missing or malformed email — cannot identify customer"
                    skipped.append(rec)
                elif not current_kit:
                    rec["skip_reason"] = "row appears before any kit header"
                    skipped.append(rec)
                else:
                    usable.append(rec)
    return usable, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Parse and report, write NOTHING")
    ap.add_argument("--execute", action="store_true", help="Write to Supabase")
    args = ap.parse_args()
    if not args.dry_run and not args.execute:
        ap.print_help()
        print("\nError: must specify --dry-run or --execute")
        sys.exit(1)

    logger.info("=" * 66)
    logger.info("  JULY 2026 CRATEJOY IMPORT — %s",
                "DRY RUN (no writes)" if args.dry_run else "LIVE")
    logger.info("=" * 66)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set")
        sys.exit(1)
    db = create_client(SUPABASE_URL, SUPABASE_KEY)

    rows, skipped = parse_rows()
    logger.info("[PARSE] usable rows=%d  skipped=%d", len(rows), len(skipped))
    for s in skipped:
        logger.warning("[SKIP] %s %r (%s) — %s", s["src"], s["name"], s["email"], s["skip_reason"])

    # ── Kit resolution — READ ONLY, abort on any miss ──
    kits = paginate(db, "kits", "id, sku, trimester")
    kit_items = paginate(db, "kit_items", "kit_id, item_id")
    by_sku = {k["sku"]: k for k in kits}
    by_norm = {}
    for k in kits:
        n = re.sub(r"\s+KITS?$", "", k["sku"], flags=re.IGNORECASE).strip()
        if n != k["sku"]:
            by_norm[n] = k["sku"]
    items_by_kit = defaultdict(list)
    for ki in kit_items:
        items_by_kit[ki["kit_id"]].append(ki["item_id"])
    logger.info("[KITS] %d kits, %d kit_items rows across %d kits",
                len(kits), len(kit_items), len(items_by_kit))

    resolved, unresolved = {}, set()
    for raw in {r["kit_raw"] for r in rows}:
        norm = parse_kit_sku(raw)
        kit = by_sku.get(norm) or by_sku.get(by_norm.get(norm, ""))
        if kit:
            resolved[raw] = kit
            logger.info("[KITS] %-16s -> %-20s T%s items=%d",
                        raw, kit["sku"], kit["trimester"], len(items_by_kit.get(kit["id"], [])))
        else:
            unresolved.add(raw)
    if unresolved:
        logger.error("ABORTING — kit SKU(s) not in the kits table: %s", sorted(unresolved))
        logger.error("Ask Sheena to create them in the app. NOTHING was written.")
        sys.exit(1)

    # ── Customer resolution by email ──
    customers = paginate(db, "customers", "id, email, first_name, last_name, due_date, clothing_size")
    by_email = {(c.get("email") or "").lower(): c for c in customers}
    matched = [r for r in rows if r["email"] in by_email]
    to_create = [r for r in rows if r["email"] not in by_email]
    logger.info("[CUSTOMERS] matched existing=%d  would create=%d", len(matched), len(to_create))
    for r in to_create:
        logger.info("[CUSTOMERS] NEW %s %r %s due=%s", r["src"], r["name"], r["email"], r["due_date"])

    # Flag rows whose DB record disagrees on due_date — possible wrong-account match
    for r in matched:
        c = by_email[r["email"]]
        if c.get("due_date") and r["due_date"] and c["due_date"] != r["due_date"]:
            logger.warning(
                "[SUSPECT] %s %r %s — csv due=%s but DB due=%s (DB name %r). "
                "Email may point at the wrong account; verify with Sheena.",
                r["src"], r["name"], r["email"], r["due_date"], c["due_date"],
                f"{c.get('first_name') or ''} {c.get('last_name') or ''}".strip())

    existing = {(s["customer_id"], s.get("kit_sku"), (s.get("ship_date") or "")[:7])
                for s in paginate(db, "shipments", "customer_id, kit_sku, ship_date")}

    if args.dry_run:
        would = dup = 0
        for r in matched:
            key = (by_email[r["email"]]["id"], resolved[r["kit_raw"]]["sku"], YEAR_MONTH)
            if key in existing:
                dup += 1
            else:
                would += 1
        items = sum(len(items_by_kit.get(resolved[r["kit_raw"]]["id"], [])) for r in matched)
        logger.info("")
        logger.info("  WOULD INSERT — customers=%d  shipments=%d (+%d already present)",
                    len(to_create), would, dup)
        logger.info("  WOULD INSERT — shipment_items~%d", items)
        logger.info("  kits=0  items=0   <- rule: never seed kits/items")
        logger.info("  SKIPPED rows needing Sheena: %d", len(skipped))
        logger.info("")
        logger.info("DRY RUN COMPLETE — nothing written.")
        return

    # ── LIVE ──
    email_to_id = {e: c["id"] for e, c in by_email.items()}
    created = 0
    for r in to_create:
        rec = {"email": r["email"], "platform": PLATFORM, "subscription_status": "active",
               "country": "US"}
        parts = r["name"].split(" ", 1)
        if parts and parts[0]:
            rec["first_name"] = parts[0]
            if len(parts) > 1:
                rec["last_name"] = parts[1]
        if r["due_date"]:
            rec["due_date"] = r["due_date"]
        if r["size"]:
            rec["clothing_size"] = r["size"]
        if r["daddy"]:
            rec["wants_daddy_item"] = r["daddy"].strip().lower().startswith("yes")
        try:
            res = db.table("customers").insert(rec).execute()
            if res.data:
                email_to_id[r["email"]] = res.data[0]["id"]
                created += 1
                logger.info("[CUSTOMER] created %s -> %s", r["email"], res.data[0]["id"])
        except Exception as exc:
            logger.error("[CUSTOMER] insert failed for %s: %s", r["email"], exc)

    ship_records, local = [], set()
    for r in rows:
        cid = email_to_id.get(r["email"])
        if not cid:
            logger.warning("[SHIP] no customer_id for %s — skipped", r["email"])
            continue
        kit = resolved[r["kit_raw"]]
        key = (cid, kit["sku"], YEAR_MONTH)
        if key in existing or key in local:
            continue
        local.add(key)
        ship_records.append({
            "customer_id": cid, "kit_id": kit["id"], "kit_sku": kit["sku"],
            "ship_date": SHIP_DATE, "platform": PLATFORM,
            "trimester_at_ship": kit["trimester"],
        })

    logger.info("[SHIP] inserting %d shipments", len(ship_records))
    inserted_ships = inserted_items = 0
    for i in range(0, len(ship_records), 100):
        batch = ship_records[i:i + 100]
        res = db.table("shipments").insert(batch).execute()
        got = res.data or []
        inserted_ships += len(got)
        pending = [{"shipment_id": s["id"], "item_id": iid}
                   for s in got for iid in items_by_kit.get(s.get("kit_id"), [])]
        for j in range(0, len(pending), 100):
            db.table("shipment_items").upsert(pending[j:j + 100],
                                              on_conflict="shipment_id,item_id").execute()
        inserted_items += len(pending)
        logger.info("  batch %d: %d shipments, %d items", i // 100 + 1, len(got), len(pending))

    logger.info("")
    logger.info("=" * 66)
    logger.info("  customers created : %d", created)
    logger.info("  shipments created : %d", inserted_ships)
    logger.info("  shipment_items    : %d", inserted_items)
    logger.info("  kits created      : 0")
    logger.info("  items created     : 0")
    logger.info("  rows left for Sheena: %d", len(skipped))
    logger.info("=" * 66)


if __name__ == "__main__":
    main()
