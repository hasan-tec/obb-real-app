"""
diag_veracore_empty_resolve.py — one-off diagnostic (READ ONLY, no writes).

For the DB items whose veracore_sku is currently EMPTY (flagged by Hasan as
"not syncing"), check whether their own .sku field (or a text fragment of
their name) exact- or near-matches something in the LIVE VeraCore inventory
that the original migration script missed. This tells us which empty rows
can be safely auto-filled vs which genuinely need a fresh answer from Sheena
because the real VeraCore product cannot be confidently identified.
"""
import os
import sys
import difflib

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_supabase, get_veracore_client  # noqa: E402

EMPTY_ITEM_NAME_FRAGMENTS = [
    "Baby Mama Tank (Medium)",
    "Bellybrace (Large)",
    "Cleo - Coco+Charcoal Deodorant",
    "Hanatonic Nauseareliefshot",
    "Mother Noun Maternity Tshirt (Medium)",
    "Obb - Mama Cap",
    "Wink Naturals - Chest Rub",
    "Toe Talk - Mantra Grip Socks",
    "Portland Bee Balm",
    "OBB-MATERNITY MOTHERHOOD TSHIRT",
    "China - Baby 1st Year Milestone Postcards (Chevron)",
    "China - Baby 1st Year Milestone Postcards (No Chevron)",
]

db = get_supabase()
vc = get_veracore_client()
inv = vc.get_inventory()
vc_skus = [r["sku"] for r in inv]
vc_by_stripped_upper = {s.strip().upper(): s for s in vc_skus}

items = db.table("items").select("id, sku, name, veracore_sku").execute().data or []

for frag in EMPTY_ITEM_NAME_FRAGMENTS:
    frag_lower = frag.lower()
    matches = [it for it in items if frag_lower in (it.get("name") or "").lower()]
    for it in matches:
        sku = (it.get("sku") or "").strip()
        print(f"=== name={it['name']!r} own_sku={sku!r} ===")
        exact = vc_by_stripped_upper.get(sku.upper())
        if exact:
            print(f"  -> OWN SKU exact-matches live VeraCore: {exact!r}  [HIGH CONFIDENCE AUTO-FILL]")
        else:
            close = difflib.get_close_matches(sku, vc_skus, n=5, cutoff=0.55)
            if close:
                print(f"  -> no exact match on own sku; closest live candidates:")
                for c in close:
                    print(f"       {c!r}")
            else:
                print(f"  -> no exact or close match on own sku either (>0.55)")
        print()

print("\n--- raw live inventory rows containing 'postcard' + 'china' ---")
for s in vc_skus:
    if "postcard" in s.lower() and "china" in s.lower():
        print(f"   {s!r}")

print("\n--- raw live inventory rows containing 'bellybrace' ---")
for s in vc_skus:
    if "bellybrace" in s.lower():
        print(f"   {s!r}")

print("\n--- raw live inventory rows containing 'tank' ---")
for s in vc_skus:
    if "tank" in s.lower():
        print(f"   {s!r}")

print("\n--- raw live inventory rows containing 'chestrub' ---")
for s in vc_skus:
    if "chestrub" in s.lower().replace(" ", ""):
        print(f"   {s!r}")

print("\n--- raw live inventory rows containing 'mamacap' ---")
for s in vc_skus:
    if "mamacap" in s.lower().replace(" ", "").replace("+", ""):
        print(f"   {s!r}")

print("\n--- raw live inventory rows containing 'mantragrip' ---")
for s in vc_skus:
    if "mantragrip" in s.lower().replace(" ", ""):
        print(f"   {s!r}")

print("\n--- raw live inventory rows containing 'beebalm' ---")
for s in vc_skus:
    if "beebalm" in s.lower().replace(" ", ""):
        print(f"   {s!r}")

print("\n--- raw live inventory rows containing 'motherhood' ---")
for s in vc_skus:
    if "motherhood" in s.lower():
        print(f"   {s!r}")
