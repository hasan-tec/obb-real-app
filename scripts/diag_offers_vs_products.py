"""
diag_offers_vs_products.py — one-off diagnostic (READ ONLY, no writes).

ROOT-CAUSE TEST for "products missing from GetInventory".

Hypothesis: VeraCore models OFFERS (sellable) separately from PRODUCTS
(physical warehouse SKUs). /api/GetInventory is documented in the tenant
Swagger as filterable by `offerIds` -- i.e. it returns the OFFER catalog.
Products with no linked Offer are invisible to it, which would explain why
in-stock products visible in Product Inquiry never appear in our sync.

This script calls, read-only:
  GET /api/InventoryOwners   -> find the owner id (e.g. OhBaby)
  GET /api/Offers            -> count the OFFER catalog
  GET /api/GetInventory      -> count what our sync currently sees
  GET /api/ProductDetails    -> count the PRODUCT catalog + look up the
                                specific products we know are missing

If ProductDetails returns the missing SKUs while GetInventory does not,
the hypothesis is confirmed and the bug is ours (wrong endpoint for the
job), not VeraCore's.
"""
import os
import sys
import json

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_veracore_client  # noqa: E402

MISSING = [
    "OBB-PortlandBeeBalm+OregonMintBalm",
    "OBB-TheHappyShoppe+Lavender&MugwortEyePillows",
    "OBB-China+Baby1stYearMilestonePostcards(Chevron)",
    "OBB-China+Baby1stYearMilestonePostcards",
    "OBB-HS+Amazonite&LavaDiffuserBracelet",
    "OBB-WinkNaturals+ChestRub",
]

vc = get_veracore_client()


def try_call(label, method, path, params=None):
    print(f"\n{'='*70}\n{label}\n  {method} {path} params={params}\n{'='*70}")
    try:
        r = vc._request(method, path, params=params)
    except Exception as e:
        print(f"  !! FAILED: {type(e).__name__}: {str(e)[:400]}")
        return None
    if isinstance(r, dict):
        for k, v in r.items():
            if isinstance(v, list):
                print(f"  key={k!r} -> list of {len(v)}")
            else:
                print(f"  key={k!r} -> {str(v)[:200]!r}")
    else:
        print(f"  (non-dict response: {type(r)})")
    return r


# 1. Owners
owners = try_call("1. INVENTORY OWNERS", "GET", "/api/InventoryOwners")
if owners:
    print("  raw:", json.dumps(owners)[:600])

# 2. Offers catalog
offers = try_call("2. OFFERS CATALOG", "GET", "/api/Offers")
offer_ids = set()
if isinstance(offers, dict):
    lst = offers.get("Offers") or offers.get("offers") or []
    for o in lst:
        if isinstance(o, dict):
            oid = o.get("Id") or o.get("id")
            if oid:
                offer_ids.add(str(oid).strip().upper())
    print(f"  -> parsed {len(offer_ids)} distinct offer ids")

# 3. GetInventory (what our sync uses today)
inv = try_call("3. GETINVENTORY (current sync source)", "GET", "/api/GetInventory")
inv_ids = set()
if isinstance(inv, dict):
    lst = inv.get("Inventory") or inv.get("inventory") or []
    for o in lst:
        if isinstance(o, dict):
            oid = o.get("Id") or o.get("id")
            if oid:
                inv_ids.add(str(oid).strip().upper())
    print(f"  -> parsed {len(inv_ids)} distinct inventory ids")

# 4. ProductDetails — unfiltered (may require a param; we'll see)
prods = try_call("4. PRODUCTDETAILS (unfiltered)", "GET", "/api/ProductDetails")
prod_ids = set()
if isinstance(prods, dict):
    lst = prods.get("ProductDetails") or prods.get("productDetails") or []
    for o in lst:
        if isinstance(o, dict):
            pid = o.get("ProductID") or o.get("productID") or o.get("productId")
            if pid:
                prod_ids.add(str(pid).strip().upper())
    print(f"  -> parsed {len(prod_ids)} distinct product ids")

# 5. Targeted lookup of each known-missing SKU via ProductDetails
print(f"\n{'='*70}\n5. TARGETED PRODUCTDETAILS LOOKUP FOR KNOWN-MISSING SKUS\n{'='*70}")
for sku in MISSING:
    in_inv = sku.strip().upper() in inv_ids
    in_off = sku.strip().upper() in offer_ids
    in_prod = sku.strip().upper() in prod_ids
    print(f"\n  {sku!r}")
    print(f"     in GetInventory={in_inv}  in Offers={in_off}  in ProductDetails(bulk)={in_prod}")
    try:
        one = vc._request("GET", "/api/ProductDetails", params={"productId": sku})
        lst = one.get("ProductDetails") or one.get("productDetails") or []
        if lst:
            d = lst[0]
            print(f"     -> ProductDetails FOUND: id={d.get('ProductID') or d.get('productID')!r} "
                  f"desc={str(d.get('ProductDescription') or d.get('productDescription'))[:60]!r} "
                  f"buildType={d.get('BuildType') or d.get('buildType')!r}")
        else:
            print(f"     -> ProductDetails returned no rows (error={one.get('Error') or one.get('error')!r})")
    except Exception as e:
        print(f"     -> ProductDetails call FAILED: {type(e).__name__}: {str(e)[:200]}")

# 6. Summary set math
print(f"\n{'='*70}\n6. SET MATH\n{'='*70}")
print(f"  offers        : {len(offer_ids)}")
print(f"  getinventory  : {len(inv_ids)}")
print(f"  productdetails: {len(prod_ids)}")
if offer_ids and inv_ids:
    print(f"  offers == getinventory ? {offer_ids == inv_ids}")
    print(f"  in offers not in inv : {len(offer_ids - inv_ids)}")
    print(f"  in inv not in offers : {len(inv_ids - offer_ids)}")
if prod_ids and inv_ids:
    only_prod = prod_ids - inv_ids
    print(f"  PRODUCTS NOT IN GETINVENTORY: {len(only_prod)}")
    for s in sorted(only_prod)[:25]:
        print(f"     {s}")
