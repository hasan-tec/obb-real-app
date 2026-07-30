"""
diag_impact_and_alt_endpoint.py — one-off diagnostic (READ ONLY, no writes).

Two questions left after diag_offer_status_gap.py proved GetInventory
silently drops every offer with Inactive.Indicator=1 (143 of 1135):

  Q1. Does /api/GetInventoryDetails apply the SAME inactive filter, or is
      it a viable alternative that returns inactive offers too?
  Q2. BUSINESS IMPACT: how many rows in our items/kits tables are pointed
      at a VeraCore id that GetInventory will never return -- i.e. how
      many have silently frozen stock numbers?
"""
import os
import sys
import json

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_supabase, get_veracore_client  # noqa: E402

vc = get_veracore_client()
db = get_supabase()

inv = vc._request("GET", "/api/GetInventory")
det = vc._request("GET", "/api/GetInventoryDetails")
offers_raw = vc._request("GET", "/api/Offers")

inv_rows = inv.get("Inventory") or inv.get("inventory") or []
det_rows = det.get("Inventory") or det.get("inventory") or []
off_rows = offers_raw.get("Offers") or offers_raw.get("offers") or []

inv_ids = {str(r.get("Id") or r.get("id")).strip().upper() for r in inv_rows if (r.get("Id") or r.get("id"))}
det_ids = {str(r.get("Id") or r.get("id")).strip().upper() for r in det_rows if (r.get("Id") or r.get("id"))}
off_ids = {str(o.get("Id") or o.get("id")).strip().upper() for o in off_rows if (o.get("Id") or o.get("id"))}

print("=" * 70)
print("Q1. ENDPOINT COMPARISON")
print("=" * 70)
print(f"  /api/Offers             : {len(off_ids)}")
print(f"  /api/GetInventory       : {len(inv_ids)}")
print(f"  /api/GetInventoryDetails: {len(det_ids)}")
print(f"  GetInventoryDetails adds over GetInventory: {len(det_ids - inv_ids)}")
print(f"  still missing vs Offers (details)         : {len(off_ids - det_ids)}")
if det_rows:
    print(f"  sample details row: {json.dumps(det_rows[0])[:400]}")

print("\n" + "=" * 70)
print("Q2. BUSINESS IMPACT ON OUR TABLES")
print("=" * 70)

items = db.table("items").select("id, sku, name, veracore_sku, quantity_available, inventory_synced_at").execute().data or []
kits = db.table("kits").select("id, sku, name, veracore_sku, quantity_available").execute().data or []


def effective(row):
    """Same resolution order veracore_sync uses: veracore_sku first, then sku."""
    v = (row.get("veracore_sku") or "").strip()
    s = (row.get("sku") or "").strip()
    return (v or s).upper()


def bucket(rows, label):
    syncable, frozen, no_id = [], [], []
    for r in rows:
        key = effective(r)
        if not key:
            no_id.append(r)
        elif key in inv_ids:
            syncable.append(r)
        else:
            frozen.append(r)
    print(f"\n  {label}: total={len(rows)}")
    print(f"     syncable now (id present in GetInventory) : {len(syncable)}")
    print(f"     FROZEN (id absent -> stock never updates) : {len(frozen)}")
    print(f"     no sku/veracore_sku at all                : {len(no_id)}")
    return frozen


frozen_items = bucket(items, "ITEMS")
frozen_kits = bucket(kits, "KITS")

print("\n  Frozen ITEMS broken down by cause:")
inactive_offer, not_an_offer = [], []
for r in frozen_items:
    key = effective(r)
    (inactive_offer if key in off_ids else not_an_offer).append(r)
print(f"     offer exists but INACTIVE (VeraCore data fix) : {len(inactive_offer)}")
print(f"     no offer record at all (needs Offer created)  : {len(not_an_offer)}")

print("\n  --- frozen because offer is INACTIVE ---")
for r in inactive_offer[:30]:
    print(f"     qty={str(r.get('quantity_available')):>6}  {r['name'][:44]:<44} {effective(r)}")

print("\n  --- frozen because NO OFFER EXISTS ---")
for r in not_an_offer[:30]:
    print(f"     qty={str(r.get('quantity_available')):>6}  {r['name'][:44]:<44} {effective(r)}")

if frozen_kits:
    print("\n  --- frozen KITS ---")
    for r in frozen_kits[:20]:
        print(f"     qty={str(r.get('quantity_available')):>6}  {str(r.get('name'))[:44]:<44} {effective(r)}")
