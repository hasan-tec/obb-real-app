"""
diag_veracore_mismatch.py — one-off diagnostic (READ ONLY, no writes).

Pulls LIVE VeraCore inventory (GetInventory) and cross-checks it against our
items table for a specific list of names Hasan flagged as "not syncing" —
to find out WHY (truncation on VeraCore's side, case mismatch, wrong mapping,
near-duplicate collision, etc). Makes zero DB writes and zero VeraCore writes.
"""
import os
import sys
import difflib

os.environ["OBB_DISABLE_SCHEDULER"] = "1"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_supabase, get_veracore_client  # noqa: E402

FLAGGED_NAMES = [
    "Baby Mama Tank (Medium)",
    "Bellybrace (Large)",
    "Cleo - Coco+Charcoal Deodorant (Basilmint)",
    "Desert Mystic Goods - Equinox Rose Toner",
    "Gemma Simone - 14K Gold Plated Mama Earrings",
    "Hanatonic Nauseareliefshot",
    "Happy Shoppe - Lavender & Mugwort Eye Pillow",
    "Mother Noun Maternity Tshirt (Medium)",
    "OBB-BELLY BRACE",
    "Obb - Mama Cap",
    "Obb - Hello Baby Journal",
    "Mudmasky Savedbythescrubsforunderarm",
    "Wink Naturals - Chest Rub",
    "Toe Talk - Mantra Grip Socks (Mixed Designs)",
    "OBB-GEN 3 JEWELS - AMAZONITE",
    "Portland Bee Balm - Oregon Mint Balm",
    "Vitamasque - Aloe Vera Hydro Sheet Jelly Mask",
    "OBB-MATERNITY MOTHERHOOD TSHIRT",
    "China - Baby 1st Year Milestone Postcards",
]

db = get_supabase()
vc = get_veracore_client()
if vc is None:
    print("VeraCore client unavailable (creds missing) — aborting")
    sys.exit(1)

print("Pulling live VeraCore inventory...")
inv = vc.get_inventory()
print(f"Pulled {len(inv)} live VeraCore inventory rows\n")

vc_by_upper = {row["sku"].strip().upper(): row["sku"] for row in inv}
vc_all_skus = [row["sku"] for row in inv]

items = db.table("items").select("id, sku, name, veracore_sku").execute().data or []
print(f"Loaded {len(items)} DB items\n")

for frag in FLAGGED_NAMES:
    frag_lower = frag.lower()
    matches = [it for it in items if frag_lower in (it.get("name") or "").lower()]
    if not matches:
        print(f"=== '{frag}' === NO DB ITEM FOUND matching this name fragment\n")
        continue
    for it in matches:
        vsku = (it.get("veracore_sku") or "").strip()
        sku = (it.get("sku") or "").strip()
        print(f"=== DB item: name={it['name']!r} sku={sku!r} veracore_sku={vsku!r} ===")
        if not vsku:
            print("  -> veracore_sku is EMPTY on this row\n")
            continue
        exact = vc_by_upper.get(vsku.upper())
        if exact:
            print(f"  -> EXACT match found live in VeraCore: {exact!r} (sync should work)\n")
            continue
        # No exact match -- find closest live VeraCore SKUs for comparison
        close = difflib.get_close_matches(vsku, vc_all_skus, n=3, cutoff=0.6)
        print(f"  -> NO EXACT MATCH in live VeraCore inventory ({len(vc_all_skus)} rows scanned)")
        if close:
            for c in close:
                same_len_note = f" (len {len(c)} vs our {len(vsku)})" if len(c) != len(vsku) else ""
                truncated_note = ""
                if vsku.upper().startswith(c.upper()) or c.upper().startswith(vsku.upper()):
                    truncated_note = "  <-- ONE IS A PREFIX OF THE OTHER (looks like truncation)"
                print(f"     closest live match: {c!r}{same_len_note}{truncated_note}")
        else:
            print("     no close match at all in live VeraCore inventory (>0.6 similarity)")
        print()
