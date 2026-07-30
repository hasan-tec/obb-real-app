"""
diag_offer_status_gap.py — one-off diagnostic (READ ONLY, no writes).

Follow-up to diag_offers_vs_products.py, which established:
  /api/Offers        -> 1135 offers
  /api/GetInventory  ->  992 rows      (143 fewer!)
  /api/ProductDetails-> 403 Missing permission 'WarehouseProducts'

So GetInventory is dropping 143 offers that the Offers endpoint happily
returns. This script figures out WHAT distinguishes the dropped rows --
the Offers schema carries a `status` block (startDate / endDate /
inactive.indicator) which is the prime suspect for a server-side filter.

Checks:
  1. Are our known-missing SKUs present in /api/Offers?
  2. What does their `status` block look like vs a control SKU that DOES
     sync fine?
  3. Statistically, what do all 143 dropped offers have in common?
"""
import os
import sys
import json
from collections import Counter

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
CONTROL = "OBB-Mudmasky+SavedByTheScrubsforUnderarms"

vc = get_veracore_client()

offers_raw = vc._request("GET", "/api/Offers")
inv_raw = vc._request("GET", "/api/GetInventory")

offers = offers_raw.get("Offers") or offers_raw.get("offers") or []
inv = inv_raw.get("Inventory") or inv_raw.get("inventory") or []

offers_by_id = {}
for o in offers:
    oid = o.get("Id") or o.get("id")
    if oid:
        offers_by_id[str(oid).strip().upper()] = o

inv_ids = {str((r.get("Id") or r.get("id"))).strip().upper() for r in inv if (r.get("Id") or r.get("id"))}

print(f"offers={len(offers_by_id)}  getinventory={len(inv_ids)}")

dropped = set(offers_by_id) - inv_ids
extra = inv_ids - set(offers_by_id)
print(f"offers NOT returned by GetInventory: {len(dropped)}")
print(f"in GetInventory but NOT an offer   : {len(extra)}")

print("\n" + "=" * 70)
print("KNOWN-MISSING SKUS - are they offers?")
print("=" * 70)
for sku in MISSING + [CONTROL]:
    k = sku.strip().upper()
    tag = "CONTROL(syncs ok)" if sku == CONTROL else "missing"
    o = offers_by_id.get(k)
    print(f"\n  [{tag}] {sku!r}")
    print(f"     in /api/Offers      : {o is not None}")
    print(f"     in /api/GetInventory: {k in inv_ids}")
    if o:
        print(f"     status  : {json.dumps(o.get('Status') or o.get('status'))}")
        print(f"     BOMType : {(o.get('BillOfMaterialsType') or o.get('billOfMaterialsType'))!r}")
        print(f"     UOM     : {(o.get('UnitOfMeasure') or o.get('unitOfMeasure'))!r}")
        print(f"     created : {(o.get('OfferCreatedServerDateTime') or o.get('offerCreatedServerDateTime'))!r}")

print("\n" + "=" * 70)
print("WHAT DO THE DROPPED OFFERS HAVE IN COMMON?")
print("=" * 70)


def status_signature(o):
    st = o.get("Status") or o.get("status") or {}
    inact = st.get("Inactive") or st.get("inactive") or {}
    return (
        f"inactive.indicator={inact.get('Indicator', inact.get('indicator'))}"
        f" startDate={'Y' if (st.get('StartDate') or st.get('startDate')) else 'N'}"
        f" endDate={'Y' if (st.get('EndDate') or st.get('endDate')) else 'N'}"
    )


sig_dropped = Counter(status_signature(offers_by_id[k]) for k in dropped)
sig_kept = Counter(status_signature(offers_by_id[k]) for k in (set(offers_by_id) & inv_ids))

print("\n  DROPPED (in Offers, absent from GetInventory):")
for s, c in sig_dropped.most_common(10):
    print(f"     {c:5d}  {s}")
print("\n  KEPT (present in both):")
for s, c in sig_kept.most_common(10):
    print(f"     {c:5d}  {s}")

bom_dropped = Counter(str(offers_by_id[k].get("BillOfMaterialsType") or offers_by_id[k].get("billOfMaterialsType")) for k in dropped)
bom_kept = Counter(str(offers_by_id[k].get("BillOfMaterialsType") or offers_by_id[k].get("billOfMaterialsType")) for k in (set(offers_by_id) & inv_ids))
print("\n  BillOfMaterialsType - DROPPED:", dict(bom_dropped.most_common(6)))
print("  BillOfMaterialsType - KEPT   :", dict(bom_kept.most_common(6)))

print("\n  sample dropped ids:")
for s in sorted(dropped)[:20]:
    print(f"     {s}")

print("\n" + "=" * 70)
print("CAN WE FETCH A DROPPED ONE BY EXPLICIT offerIds FILTER?")
print("=" * 70)
for sku in MISSING[:3]:
    try:
        r = vc._request("GET", "/api/GetInventory", params={"offerIds": sku})
        rows = r.get("Inventory") or r.get("inventory") or []
        print(f"  {sku!r} -> {len(rows)} row(s) err={r.get('Error') or r.get('error')!r}")
        if rows:
            print(f"      {json.dumps(rows[0])}")
    except Exception as e:
        print(f"  {sku!r} -> FAILED {type(e).__name__}: {str(e)[:200]}")
