"""
Import Sheena's manually-processed AUGUST 2026 renewal kit assignments from her CSV.

CSV LAYOUT (important, non-obvious):
  Column A ("KIT ASSIGNMENT + KIT SKU") holds TWO unrelated things stacked in one column:
    - section marker rows (CP41 / CP31 / CP21 / BP11) where ALL other columns are empty
    - that kit's 8-item manifest, one item per row, running down the page
  The item text sitting on a customer's row is NOT that customer's data. A customer's kit
  is determined solely by which section they fall under.

WHAT THIS DOES (Hasan's decisions, 2026-08-23):
  Phase 1  DEDUPE    remove duplicate August shipment rows (same customer+kit), keep oldest
  Phase 2  SUPERSEDE where the CSV kit disagrees with the August kit already recorded,
                     remove the stale row so the CSV becomes the record of truth
  Phase 3  INSERT    create the missing shipments plus their shipment_items
  STOCK              never touched (these boxes already physically shipped)

NEVER creates or edits kits or items. Kits are looked up read-only by SKU; an unresolved
SKU aborts the whole run rather than inventing anything.

Dry run (default, writes nothing):
    python scripts/import_august_manual_assignments.py
Apply:
    python scripts/import_august_manual_assignments.py --apply
"""
import csv
import io
import os
import sys
from collections import Counter, defaultdict

import httpx

APPLY = "--apply" in sys.argv
CSV_PATH = os.environ.get(
    "AUG_CSV_PATH",
    r"C:\Users\hasan\Downloads\Oh Baby Boxes - Monthly Boxing_Customer Kit Assignment - AUG '26 CP.csv",
)
CYCLE = "2026-08"
SHIP_DATE = "2026-08-22"  # the date these were physically processed

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
_env = os.path.join(_root, ".env")
if os.path.exists(_env):
    for line in open(_env, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

from supabase import create_client  # noqa: E402

db = create_client(
    os.environ["SUPABASE_URL"],
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ["SUPABASE_ANON_KEY"],
)


def _paginate(query, size=1000):
    out, off, base = [], 0, str(query.params)
    while True:
        query.params = httpx.QueryParams(base)
        batch = query.range(off, off + size - 1).execute()
        out.extend(batch.data or [])
        if len(batch.data or []) < size:
            break
        off += size
    return out


def parse_csv():
    """Return (section_markers, customer_records). See CSV LAYOUT note above."""
    rows = list(csv.reader(io.open(CSV_PATH, encoding="utf-8-sig")))
    markers = [
        (i, r[0].strip())
        for i, r in enumerate(rows)
        if r and r[0].strip() and not any(c.strip() for c in r[1:])
    ]
    # A marker is a kit code only if it looks like 2 letters + 2 digits (CP41, BP11).
    # Everything else on a lone row is part of the item manifest and is ignored.
    kit_markers = [
        (i, m) for i, m in markers
        if len(m) == 4 and m[:2].isalpha() and m[2:].isdigit()
    ]
    recs = []
    for idx, (start, code) in enumerate(kit_markers):
        end = kit_markers[idx + 1][0] if idx + 1 < len(kit_markers) else len(rows)
        sku = f"OBB-{code[:2]}-{code[2:]} KITS"
        for i in range(start + 1, end):
            r = rows[i]
            if len(r) < 15 or not r[1].strip():
                continue
            recs.append({
                "code": code,
                "kit_sku": sku,
                "order": r[1].strip().lstrip("#"),
                "email": r[2].strip().lower(),
                "name": r[4].strip(),
                "row": i + 1,
            })
    return kit_markers, recs


def main():
    print("MODE:", "APPLY (WILL WRITE)" if APPLY else "DRY RUN (no writes)")
    kit_markers, recs = parse_csv()
    print(f"CSV sections: {[m for _, m in kit_markers]}")
    print(f"CSV customer rows: {len(recs)}   unique order numbers: {len(set(r['order'] for r in recs))}")
    print("  per section:", dict(Counter(r["code"] for r in recs)))

    # ---- read-only kit lookup; abort rather than create ----
    kits = _paginate(db.table("kits").select("id, sku, trimester, quantity_available"))
    kit_by_sku = {k["sku"]: k for k in kits}
    wanted_skus = sorted({r["kit_sku"] for r in recs})
    unresolved = [s for s in wanted_skus if s not in kit_by_sku]
    if unresolved:
        print("\nABORT — unresolved kit SKUs (this script will not create kits):")
        for s in unresolved:
            print("   ", s)
        return
    print("\nkit SKUs resolved read-only:")
    for s in wanted_skus:
        k = kit_by_sku[s]
        print(f"    {s:20s} T{k['trimester']}  stock={k['quantity_available']} (stock will NOT be changed)")

    # ---- customers ----
    custs = _paginate(db.table("customers").select("id, email"))
    cmap = {c["email"].strip().lower(): c["id"] for c in custs if c.get("email")}
    missing = [r for r in recs if r["email"] not in cmap]
    if missing:
        print(f"\nABORT — {len(missing)} CSV emails have no customer row:")
        for r in missing[:20]:
            print(f"    row {r['row']} {r['email']}")
        return
    print(f"all {len(recs)} CSV emails matched to existing customers")

    # ---- existing August-cycle shipments ----
    ships = _paginate(db.table("shipments").select(
        "id, customer_id, kit_sku, order_id, ship_date, created_at, notes"))
    aug = [
        s for s in ships
        if (s.get("ship_date") or "")[:7] == CYCLE
        or (not s.get("ship_date") and (s.get("created_at") or "")[:7] == CYCLE)
    ]
    print(f"\nexisting August-cycle shipments in DB: {len(aug)}")

    by_cust_kit = defaultdict(list)
    for s in aug:
        by_cust_kit[(s["customer_id"], s.get("kit_sku"))].append(s)
    for v in by_cust_kit.values():
        v.sort(key=lambda s: s.get("created_at") or "")

    # ================= PHASE 1: DEDUPE =================
    dedupe_ids, dk = [], Counter()
    for (cid, kit), group in by_cust_kit.items():
        if len(group) > 1:
            dedupe_ids.extend(s["id"] for s in group[1:])  # keep the oldest
            dk[kit] += len(group) - 1
    print("\n" + "=" * 64)
    print(f"PHASE 1  DEDUPE — {len(dedupe_ids)} duplicate August shipment row(s) to remove")
    for kit, n in dk.most_common():
        print(f"    {kit}: {n} extra row(s)")

    surviving = defaultdict(set)
    for (cid, kit), _g in by_cust_kit.items():
        surviving[cid].add(kit)

    # ================= PHASE 2 (supersede) + PHASE 3 (insert) =================
    csv_wanted = defaultdict(list)
    for r in recs:
        csv_wanted[cmap[r["email"]]].append(r)

    supersede_ids, to_insert, already_ok = [], [], 0
    stale_kinds = Counter()
    for cid, rlist in csv_wanted.items():
        want = Counter(r["kit_sku"] for r in rlist)
        have_kits = surviving.get(cid, set())
        for kit in (have_kits - set(want)):
            supersede_ids.extend(s["id"] for s in by_cust_kit[(cid, kit)])
            stale_kinds[(rlist[0]["code"], kit)] += 1
        for kit, n_want in want.items():
            n_have = 1 if kit in have_kits else 0  # post-dedupe there is at most one
            already_ok += min(n_want, n_have)
            for r in [x for x in rlist if x["kit_sku"] == kit][n_have:]:
                to_insert.append(r)

    print("\n" + "=" * 64)
    print(f"PHASE 2  SUPERSEDE — {len(supersede_ids)} stale August shipment row(s) to remove")
    for (code, kit), n in stale_kinds.most_common():
        print(f"    CSV says {code} but DB recorded {kit}: {n} customer(s)")

    print("\n" + "=" * 64)
    print(f"PHASE 3  INSERT — {len(to_insert)} new shipment(s)   (already correct, untouched: {already_ok})")
    print("    by kit:", dict(Counter(r["kit_sku"] for r in to_insert)))

    kit_ids = [kit_by_sku[s]["id"] for s in {r["kit_sku"] for r in to_insert}] if to_insert else []
    ki_rows = _paginate(
        db.table("kit_items").select("kit_id, item_id").in_("kit_id", kit_ids)
    ) if kit_ids else []
    items_by_kit = defaultdict(list)
    for ki in ki_rows:
        items_by_kit[ki["kit_id"]].append(ki["item_id"])
    n_items = sum(len(items_by_kit[kit_by_sku[r["kit_sku"]]["id"]]) for r in to_insert)
    print(f"    shipment_items that would be created: {n_items}")
    for sku in sorted({r["kit_sku"] for r in to_insert}):
        print(f"      {sku}: {len(items_by_kit[kit_by_sku[sku]['id']])} items per shipment")

    print("\n" + "=" * 64)
    print("NET EFFECT ON THE AUGUST CYCLE")
    final = len(aug) - len(dedupe_ids) - len(supersede_ids) + len(to_insert)
    print(f"  August shipments now       : {len(aug)}")
    print(f"  minus dedupe               : -{len(dedupe_ids)}")
    print(f"  minus superseded           : -{len(supersede_ids)}")
    print(f"  plus imported              : +{len(to_insert)}")
    print(f"  = final August shipments   : {final}")
    print(f"  CSV expects                : {len(recs)}")
    print(f"  {'MATCHES CSV' if final == len(recs) else '*** DOES NOT MATCH CSV — investigate before applying ***'}")
    print("  STOCK: not touched (boxes already physically shipped)")

    if not APPLY:
        print("\nDRY RUN — nothing written. Re-run with --apply to execute.")
        return

    removal = dedupe_ids + supersede_ids
    print(f"\nAPPLYING — removing {len(removal)} shipment row(s)...")
    for i in range(0, len(removal), 100):
        chunk = removal[i:i + 100]
        db.table("shipment_items").delete().in_("shipment_id", chunk).execute()
        db.table("shipments").delete().in_("id", chunk).execute()
        print(f"  removed {min(i + 100, len(removal))}/{len(removal)}")

    print(f"inserting {len(to_insert)} shipment(s)...")
    made = 0
    for r in to_insert:
        kit = kit_by_sku[r["kit_sku"]]
        res = db.table("shipments").insert({
            "customer_id": cmap[r["email"]],
            "kit_id": kit["id"],
            "kit_sku": kit["sku"],
            "ship_date": SHIP_DATE,
            "trimester_at_ship": kit.get("trimester"),
            "platform": "shopify",
            "order_id": r["order"],
            "notes": f"Imported from Sheena AUG 2026 manual assignment sheet (CSV row {r['row']})",
        }).execute()
        sid = res.data[0]["id"] if res.data else None
        if sid:
            items = [{"shipment_id": sid, "item_id": iid} for iid in items_by_kit[kit["id"]]]
            if items:
                db.table("shipment_items").insert(items).execute()
        made += 1
        if made % 50 == 0:
            print(f"  inserted {made}/{len(to_insert)}")
    print(f"\nDONE — removed {len(removal)}, inserted {made}")


if __name__ == "__main__":
    main()
