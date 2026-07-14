#!/usr/bin/env python3
"""
repair_recurate_shipto.py
=========================
Retroactively repair decisions that the OLD recurate bug left 'bare' — it stripped
ship-to / billing / order_id / platform from re-curated GIFT decisions (fixed for
future re-curations in commit cad8c34, but existing rows stayed corrupted).

For each corrupted decision we copy the missing fields from the SAME customer's
SOURCE decision (the original order-ingested one that still carries the real
ship-to + order_id). 100% DB-sourced — no Shopify API needed (verified: all
affected decisions have an intact source on the same customer).

Corrupted   = reason starts '[Re-curated]' AND ship_first_name is NULL AND order_id is NULL
Source      = same customer's decision WITH ship_first_name AND order_id (prefer that;
              else any decision with ship_first_name)
Idempotent  = skips a decision whose ship_first_name is already set.
Never touches kit_id/kit_sku/status/trimester/created_at — only backfills identity.

USAGE
  python scripts/repair_recurate_shipto.py --dry-run
  python scripts/repair_recurate_shipto.py --live     # BACK UP FIRST
"""
import argparse
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client, Client

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
        logging.FileHandler(SCRIPT_DIR / "repair_recurate_shipto.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("obb_repair_shipto")

COPY_FIELDS = ("ship_first_name", "ship_last_name", "billing_first_name", "billing_last_name",
               "billing_address1", "billing_city", "billing_state", "billing_zip",
               "billing_country", "order_id", "platform")


def fetch_all(db, table, cols):
    rows, off = [], 0
    while True:
        b = db.table(table).select(cols).range(off, off + 999).execute().data or []
        rows.extend(b)
        if len(b) < 1000:
            return rows
        off += 1000


def main():
    ap = argparse.ArgumentParser(description="Repair recurate-stripped ship-to on gift decisions.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--live", action="store_true")
    args = ap.parse_args()

    logger.info("=" * 72)
    logger.info("  REPAIR RECURATE-STRIPPED SHIP-TO  —  %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("=" * 72)
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE creds not set in .env"); sys.exit(1)
    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    sel = "id, customer_id, status, reason, kit_sku, created_at, " + ", ".join(COPY_FIELDS)
    decs = fetch_all(db, "decisions", sel)
    custs = {c["id"]: c for c in fetch_all(db, "customers", "id, email")}
    by_cust = defaultdict(list)
    for d in decs:
        by_cust[d.get("customer_id")].append(d)

    planned = []  # (corrupted_decision, source_decision, email)
    for cid, ds in by_cust.items():
        source = next((d for d in sorted(ds, key=lambda x: x.get("created_at") or "")
                       if d.get("ship_first_name") and d.get("order_id")), None) \
                 or next((d for d in ds if d.get("ship_first_name")), None)
        if not source or not source.get("ship_first_name"):
            continue
        for d in ds:
            corrupted = (str(d.get("reason") or "").startswith("[Re-curated]")
                         and not d.get("ship_first_name") and not d.get("order_id"))
            if corrupted:
                planned.append((d, source, (custs.get(cid) or {}).get("email", "?")))

    logger.info("Corrupted gift decisions to repair: %d", len(planned))
    logger.info("")
    for d, s, email in planned:
        logger.info("  %-34s [%s %s]  ship_to: (none) -> %s %s   order_id: (none) -> %s",
                    email, d.get("status"), d.get("kit_sku"),
                    s.get("ship_first_name"), s.get("ship_last_name"), s.get("order_id"))

    if args.dry_run:
        logger.info("\nDRY RUN — nothing written. Re-run with --live to apply.")
        return

    done = 0
    for d, s, email in planned:
        patch = {f: s.get(f) for f in COPY_FIELDS}
        try:
            db.table("decisions").update(patch).eq("id", d["id"]).execute()
            done += 1
            logger.info("  [REPAIRED] %s <- %s %s", email, s.get("ship_first_name"), s.get("ship_last_name"))
        except Exception as e:
            logger.error("  [FAIL] %s: %s", email, e, exc_info=True)

    logger.info("\n" + "=" * 72)
    logger.info("  DONE — repaired %d of %d corrupted gift decisions", done, len(planned))
    logger.info("=" * 72)


if __name__ == "__main__":
    main()
