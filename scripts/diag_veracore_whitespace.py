"""
diag_veracore_whitespace.py — one-off diagnostic (READ ONLY, no writes).

Checks how many live VeraCore GetInventory 'Id' values have leading/trailing
whitespace, and how many of our DB items.veracore_sku values would fail an
exact case-insensitive match against production's un-stripped sync logic
(veracore_sync.py currently does sku.upper() with NO .strip()).
"""
import os
import sys

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_supabase, get_veracore_client  # noqa: E402

db = get_supabase()
vc = get_veracore_client()
inv = vc.get_inventory()

whitespace_rows = [r["sku"] for r in inv if r["sku"] != r["sku"].strip()]
print(f"Live VeraCore inventory rows: {len(inv)}")
print(f"Rows with leading/trailing whitespace in Id: {len(whitespace_rows)}")
for s in whitespace_rows[:20]:
    print(f"   {s!r}")

items = db.table("items").select("id, sku, name, veracore_sku").execute().data or []
items_with_vsku = [it for it in items if (it.get("veracore_sku") or "").strip()]

vc_exact_upper = {r["sku"].upper() for r in inv}                 # prod behavior (no strip)
vc_stripped_upper = {r["sku"].strip().upper() for r in inv}      # stripped behavior

would_match_prod = 0
would_match_if_stripped = 0
mismatch_only_due_to_whitespace = []
for it in items_with_vsku:
    v = it["veracore_sku"].upper()
    if v in vc_exact_upper:
        would_match_prod += 1
    if v.strip() in vc_stripped_upper:
        would_match_if_stripped += 1
        if v not in vc_exact_upper:
            mismatch_only_due_to_whitespace.append(it)

print(f"\nItems with veracore_sku set: {len(items_with_vsku)}")
print(f"Would match live inventory under CURRENT prod logic (no strip): {would_match_prod}")
print(f"Would match if sync stripped whitespace on both sides: {would_match_if_stripped}")
print(f"Items ONLY failing today because of whitespace (would be fixed by adding .strip()): {len(mismatch_only_due_to_whitespace)}")
for it in mismatch_only_due_to_whitespace[:30]:
    print(f"   name={it['name']!r} veracore_sku={it['veracore_sku']!r}")
