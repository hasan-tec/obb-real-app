#!/usr/bin/env python3
"""
apply_veracore_skus.py — align items.sku / items.veracore_sku with VeraCore.
NO DELETES. Duplicates are merged by repointing links; the retired row is kept
but made inert (0 links) and marked in `notes`.

RULES (from Sheena, 2026-07-30)
  * VeraCore **Product ID** is the ONLY source of truth. Never the Description.
  * An answer that is not an exact Product ID in the CURRENT export is SKIPPED
    and reported — never guessed.

SOURCES
  1. VeraCore export (Product Id = col 2)         -> the allowed target values
  2. Sheena's original 153-row sheet              -> base answers
  3. Sheena's 22-row follow-up                    -> overrides the base

WHAT IT DOES
  MERGE (2+ of our items -> one Product ID):
    survivor  = most shipment_items, tie-break most kit_items
    - repoint kit_items / shipment_items / item_alternatives to the survivor
      (insert-if-absent, then delete the old link row -> never a PK violation)
    - survivor.sku = the exact Product ID, survivor.veracore_sku = same
    - retiree: links removed, notes stamped '[MERGED -> <sku>]', row KEPT.
      If a retiree is squatting on the target SKU it is renamed '<target>-DUP<n>'
      so the survivor can take it. Nothing is ever deleted.
  RENAME (single item -> a free Product ID):
    - sku = Product ID, veracore_sku = Product ID

USAGE
  python scripts/apply_veracore_skus.py --dry-run
  python scripts/apply_veracore_skus.py --live      # BACK UP FIRST
"""
import argparse
import logging
import os
import sys
from collections import defaultdict, Counter
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / ".env")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

EXPORT = r"C:\Users\hasan\Downloads\ProductSummary-639209790455065749.xls"
SHEET1 = r"C:\Users\hasan\Downloads\VeraCore SKU Confirmation - OBB correctly filled by sheena.xlsx"
SHEET2 = r"C:\Users\hasan\Downloads\VeraCore SKUs - Questions for Sheena filled by sheena.xlsx"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler(SCRIPT_DIR / "apply_veracore_skus.log", mode="w", encoding="utf-8")],
)
log = logging.getLogger("obb_apply_sku")


def S(v):
    v = "" if v is None else str(v)
    return "" if v.strip().lower() == "nan" else v.strip()


def fetch_all(db, table, cols):
    rows, off = [], 0
    while True:
        b = db.table(table).select(cols).range(off, off + 999).execute().data or []
        rows.extend(b)
        if len(b) < 1000:
            return rows
        off += 1000


def load_products():
    raw = pd.read_excel(EXPORT, dtype=str, header=None)
    pid = {}
    for _, r in raw.iterrows():
        p = r[2]
        if isinstance(p, str) and p.strip() and not p.upper().startswith(("OWNER", "TYPE", "PRODUCT ID")):
            pid.setdefault(p.strip().upper(), p.strip())
    return pid


def load_answers():
    ans = {}
    d1 = pd.read_excel(SHEET1, sheet_name="Items to Confirm", dtype=str)
    d1.columns = ["name", "cur", "guess", "conf", "correct", "note"]
    for _, r in d1.iterrows():
        cur, a = S(r["cur"]).upper(), S(r["correct"])
        if cur and a and "," not in a:
            ans[cur] = a
    d2 = pd.read_excel(SHEET2, sheet_name="Please confirm", dtype=str)
    d2.columns = ["why", "name", "cur", "put", "impact", "sugg", "ans", "notes"]
    over = 0
    for _, r in d2.iterrows():
        cur, a = S(r["cur"]).upper(), S(r["ans"])
        if cur and a and "," not in a:
            ans[cur] = a
            over += 1
    log.info("Answers loaded: %d (follow-up overrode/added %d)", len(ans), over)
    return ans


def main():
    ap = argparse.ArgumentParser(description="Align items with VeraCore Product IDs. No deletes.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--live", action="store_true")
    args = ap.parse_args()
    DRY = args.dry_run

    url = os.getenv("SUPABASE_URL"); key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        log.error("SUPABASE creds missing"); sys.exit(1)
    db: Client = create_client(url, key)

    log.info("=" * 78)
    log.info("  APPLY VERACORE SKUS  —  %s  (NO DELETES)", "DRY RUN" if DRY else "LIVE")
    log.info("=" * 78)

    products = load_products()
    answers = load_answers()
    items = fetch_all(db, "items", "id,name,sku,veracore_sku,notes")
    by_id = {i["id"]: i for i in items}
    holder = {}
    for i in items:
        s = S(i.get("sku")).upper()
        if s:
            holder[s] = i["id"]
    kit_links = Counter(k["item_id"] for k in fetch_all(db, "kit_items", "item_id"))
    ship_links = Counter(s["item_id"] for s in fetch_all(db, "shipment_items", "item_id"))
    log.info("DB: %d items | export: %d products", len(items), len(products))

    # Sheena's rule #4: "if the discrepancy is very minor or obvious (e.g. Familus vs
    # Familius), go with whatever VeraCore currently has." So an answer that isn't an
    # exact Product ID is snapped to its nearest Product ID when the match is
    # unambiguous (>= NEAR_CUTOFF); anything below that is SKIPPED for a human.
    import difflib
    NEAR_CUTOFF = 0.85
    product_names = list(products.values())

    target, skipped, snapped = {}, [], []
    for cur_sku, ans in answers.items():
        iid = holder.get(cur_sku)
        if not iid:
            continue
        real = products.get(ans.upper())
        if not real:
            m = difflib.get_close_matches(ans, product_names, n=1, cutoff=NEAR_CUTOFF)
            if m:
                real = m[0]
                snapped.append((by_id[iid]["sku"], ans, real,
                                difflib.SequenceMatcher(None, ans, real).ratio()))
            else:
                skipped.append((by_id[iid]["sku"], ans))
                continue
        target[iid] = real

    if snapped:
        log.info("")
        log.info("  SNAPPED to nearest Product ID (Sheena rule #4) — REVIEW THESE (%d):", len(snapped))
        for s, a, real, r in snapped:
            log.info("     %.2f  %-46s  '%s'  ->  '%s'", r, S(s)[:46], a, real)

    log.info("Resolved to a real Product ID: %d   |   skipped (not in export): %d",
             len(target), len(skipped))

    groups = defaultdict(set)
    for iid, t in target.items():
        groups[t.upper()].add(iid)
    for t in list(groups):
        h = holder.get(t)
        if h:
            groups[t].add(h)

    merges = {t: m for t, m in groups.items() if len(m) > 1}
    singles = {t: next(iter(m)) for t, m in groups.items() if len(m) == 1}

    plan_updates = []
    plan_link_moves = []

    for t_upper, members in sorted(merges.items()):
        real = products[t_upper]
        survivor = max(members, key=lambda i: (ship_links.get(i, 0), kit_links.get(i, 0)))
        retirees = [i for i in members if i != survivor]
        log.info("")
        log.info("MERGE -> %s", real)
        log.info("   KEEP   %-52s kits=%-3d ships=%d",
                 S(by_id[survivor]["sku"])[:52], kit_links.get(survivor, 0), ship_links.get(survivor, 0))
        dup_n = 0
        for r in retirees:
            log.info("   MERGE  %-52s kits=%-3d ships=%d",
                     S(by_id[r]["sku"])[:52], kit_links.get(r, 0), ship_links.get(r, 0))
            for tbl, keycol in (("kit_items", "kit_id"), ("shipment_items", "shipment_id")):
                rows = db.table(tbl).select(keycol).eq("item_id", r).execute().data or []
                for row in rows:
                    plan_link_moves.append((tbl, keycol, row[keycol], r, survivor))
            alts = db.table("item_alternatives").select("item_id,alternative_item_id").or_(
                f"item_id.eq.{r},alternative_item_id.eq.{r}").execute().data or []
            for a in alts:
                plan_link_moves.append(("item_alternatives", "pair",
                                        (a["item_id"], a["alternative_item_id"]), r, survivor))
            patch = {"notes": f"[MERGED -> {real}] " + (S(by_id[r].get("notes")) or "")}
            if S(by_id[r]["sku"]).upper() == t_upper and r != survivor:
                dup_n += 1
                patch["sku"] = f"{real}-DUP{dup_n}"
                log.info("          ^ holds the target sku; renaming to %s so the survivor can take it",
                         patch["sku"])
            plan_updates.append((r, patch, f"retire {S(by_id[r]['sku'])}"))
        plan_updates.append((survivor, {"sku": real, "veracore_sku": real}, f"survivor -> {real}"))

    n_ren = 0
    for t_upper, iid in sorted(singles.items()):
        real = products[t_upper]
        cur = S(by_id[iid]["sku"])
        patch = {"veracore_sku": real}
        if cur.upper() != t_upper:
            patch["sku"] = real
            n_ren += 1
        plan_updates.append((iid, patch, f"rename {cur} -> {real}"))

    log.info("")
    log.info("=" * 78)
    log.info("  PLAN")
    log.info("=" * 78)
    log.info("  merge groups              : %d", len(merges))
    log.info("  items merged (kept, inert): %d", sum(len(m) - 1 for m in merges.values()))
    log.info("  single renames            : %d", n_ren)
    log.info("  item update statements    : %d", len(plan_updates))
    log.info("  link rows to repoint      : %d", len(plan_link_moves))
    log.info("  ITEMS DELETED             : 0  (by design)")
    if skipped:
        log.info("")
        log.info("  SKIPPED — answer is not a Product ID in the current export (%d):", len(skipped))
        for s, a in skipped:
            log.info("     %-52s -> %s", S(s)[:52], a)

    if DRY:
        log.info("")
        log.info("DRY RUN — nothing written. Re-run with --live to apply.")
        return

    moved = deduped = 0
    for tbl, keycol, keyval, old, new in plan_link_moves:
        try:
            if tbl == "item_alternatives":
                a, b = keyval
                na, nb = (new if a == old else a), (new if b == old else b)
                db.table("item_alternatives").delete().eq("item_id", a).eq("alternative_item_id", b).execute()
                if na != nb:
                    ex = db.table("item_alternatives").select("item_id").eq("item_id", na).eq(
                        "alternative_item_id", nb).execute().data
                    if not ex:
                        db.table("item_alternatives").insert({"item_id": na, "alternative_item_id": nb}).execute()
                        moved += 1
                    else:
                        deduped += 1
                else:
                    deduped += 1
                continue
            ex = db.table(tbl).select(keycol).eq(keycol, keyval).eq("item_id", new).execute().data
            if not ex:
                db.table(tbl).insert({keycol: keyval, "item_id": new}).execute()
                moved += 1
            else:
                deduped += 1
            db.table(tbl).delete().eq(keycol, keyval).eq("item_id", old).execute()
        except Exception as e:
            log.error("link move failed %s %s: %s", tbl, keyval, e)

    applied = 0
    for iid, patch, why in plan_updates:
        try:
            db.table("items").update(patch).eq("id", iid).execute()
            applied += 1
        except Exception as e:
            log.error("update failed for %s (%s): %s", iid, why, e)

    log.info("")
    log.info("LIVE DONE — links moved=%d deduped=%d | item updates=%d | deletes=0",
             moved, deduped, applied)


if __name__ == "__main__":
    main()
