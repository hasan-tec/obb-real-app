"""
diag_inventory_raw_shape.py — one-off diagnostic (READ ONLY, no writes).

Calls VeraCore GetInventory RAW (bypassing get_inventory()'s normalization)
to inspect the actual JSON envelope for any pagination metadata (TotalCount,
PageSize, HasMore, etc.) that our client might be silently ignoring, and to
confirm whether known-missing SKUs (Portland Bee Balm, Happy Shoppe, China
Postcards Chevron/No-Chevron, GEN3 Jewels Amazonite) are truly absent from
the raw payload or just dropped somewhere in our own parsing.
"""
import os
import sys
import json

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_veracore_client  # noqa: E402

vc = get_veracore_client()

raw = vc._request("GET", vc.inventory_path)

print("Top-level keys in raw response:", list(raw.keys()) if isinstance(raw, dict) else type(raw))

if isinstance(raw, dict):
    for k, v in raw.items():
        if isinstance(v, list):
            print(f"  key={k!r} -> list of {len(v)} items")
        else:
            print(f"  key={k!r} -> {v!r}")

# Try to find the actual inventory list regardless of key name
rows = None
if isinstance(raw, dict):
    for key in ("Inventory", "inventory", "data", "Products", "items", "Items"):
        if key in raw and isinstance(raw[key], list):
            rows = raw[key]
            break
elif isinstance(raw, list):
    rows = raw

print(f"\nTotal rows in raw list: {len(rows) if rows else 0}")

KNOWN_MISSING = [
    "OBB-PortlandBeeBalm+OregonMintBalm",
    "OBB-TheHappyShoppe+Lavender&MugwortEyePillows",
    "OBB-China+Baby1stYearMilestonePostcards(Chevron)",
    "OBB-China+Baby1stYearMilestonePostcards",
    "OBB-HS+Amazonite&LavaDiffuserBracelet",
    "OBB-WinkNaturals+ChestRub",
]

if rows:
    ids_upper = set()
    for row in rows:
        if isinstance(row, dict):
            rid = row.get("Id") or row.get("id") or row.get("Sku") or row.get("sku")
            if rid:
                ids_upper.add(str(rid).strip().upper())
    for sku in KNOWN_MISSING:
        present = sku.strip().upper() in ids_upper
        print(f"  {sku!r}: {'FOUND in raw payload' if present else 'NOT in raw payload'}")

# Dump first + last row raw shape for inspection
if rows:
    print("\nFirst raw row:", json.dumps(rows[0], indent=2)[:500])
    print("\nLast raw row:", json.dumps(rows[-1], indent=2)[:500])
