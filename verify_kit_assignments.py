"""Final verification: Kit assignment recommendations in the report are correct."""
import os, logging
os.environ['SUPABASE_URL'] = 'https://tkcvvjxmzfjaesdhyfiy.supabase.co'
os.environ['SUPABASE_SERVICE_ROLE_KEY'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRrY3Z2anhtemZqYWVzZGh5Zml5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDUzOTg1MSwiZXhwIjoyMDkwMTE1ODUxfQ.3rktaVHiFesZ0RU7BUcULZ9bBfO0r6wOGJqMK60a-7Q'
logging.disable(logging.CRITICAL)

from supabase import create_client
from datetime import date, timedelta
from collections import defaultdict

db = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_ROLE_KEY'])
RUN_ID = "bb99952c-d527-4fc8-9b3d-4a91515e24c2"

def paginate(query, page_size=1000):
    results = []
    offset = 0
    while True:
        batch = query.range(offset, offset + page_size - 1).execute()
        results.extend(batch.data or [])
        if len(batch.data or []) < page_size:
            break
        offset += page_size
    return results

# Load stored report customers
stored = paginate(db.table("curation_run_customers").select("*").eq("run_id", RUN_ID))

# Check kit recommendations
recommended = [s for s in stored if s.get("recommended_kit_sku")]
no_rec = [s for s in stored if not s.get("recommended_kit_sku")]
needs_new = [s for s in stored if s.get("needs_new_curation")]
covered = [s for s in stored if not s.get("needs_new_curation")]

print("=" * 80)
print("  KIT ASSIGNMENT VERIFICATION")
print("=" * 80)
print(f"  Total customers: {len(stored)}")
print(f"  With kit recommendation: {len(recommended)}")
print(f"  Without recommendation (needs new): {len(no_rec)}")
print(f"  Covered: {len(covered)}, Needs new: {len(needs_new)}")
print()

# Group recommendations by trimester
from collections import Counter
rec_by_tri = defaultdict(Counter)
for s in recommended:
    rec_by_tri[s["projected_trimester"]][s["recommended_kit_sku"]] += 1

for tri in [2, 3, 4]:
    print(f"  T{tri} Kit Recommendations:")
    for sku, count in rec_by_tri[tri].most_common():
        print(f"    {sku:30} → {count} customers")
    print()

# Verify FIFO: oldest kit should be recommended first
kits_raw = db.table("kits").select("*").eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()
kits = kits_raw.data or []

print("  Available kits by trimester (FIFO order):")
for tri in [2, 3, 4]:
    tri_kits = [k for k in kits if k["trimester"] == tri]
    print(f"  T{tri}:")
    for k in tri_kits:
        print(f"    age={k['age_rank']:3} {k['sku']:30} stock={k['quantity_available']:>3} univ={k.get('is_universal')}")
    print()

# Verify: For T2, the oldest FIFO kit should be recommended to most customers
print("  FIFO VERIFICATION:")
for tri in [2, 3, 4]:
    tri_kits = sorted([k for k in kits if k["trimester"] == tri], key=lambda x: x["age_rank"])
    if not rec_by_tri[tri]:
        print(f"  T{tri}: No recommendations")
        continue
    top_rec = rec_by_tri[tri].most_common(1)[0][0]
    oldest_sku = tri_kits[0]["sku"] if tri_kits else "?"
    match = "✅" if top_rec == oldest_sku else "⚠️"
    print(f"  {match} T{tri}: Most recommended = {top_rec}, Oldest available = {oldest_sku}")

# Check welcome kit recommendations
wk_custs = [s for s in stored if s.get("recommended_kit_sku") and "WK" in s["recommended_kit_sku"]]
print(f"\n  Welcome kit assignments: {len(wk_custs)} (should be 0 for renewals)")

# Verify no duplicates (no customer recommended a kit they already received)
print("\n  DUPLICATE CHECK (no customer should get a kit they already received):")
all_ships = paginate(db.table("shipments").select("customer_id,kit_sku"))
received_by_cust = defaultdict(set)
for s in all_ships:
    if s.get("kit_sku"):
        received_by_cust[s["customer_id"]].add(s["kit_sku"])

dupes = 0
for s in recommended:
    if s["recommended_kit_sku"] in received_by_cust.get(s["customer_id"], set()):
        dupes += 1
        cust = db.table("customers").select("email").eq("id", s["customer_id"]).execute()
        email = cust.data[0]["email"] if cust.data else "?"
        print(f"    ❌ DUPLICATE: {email} recommended {s['recommended_kit_sku']} but already received it!")

if dupes == 0:
    print(f"  ✅ No duplicates found — all {len(recommended)} recommendations are kits the customer hasn't received")

# Verify no item overlap for covered customers
print("\n  ITEM OVERLAP CHECK (spot-check 20 covered customers):")
all_ki = paginate(db.table("kit_items").select("*"))
ki_by_kit_id = defaultdict(set)
for ki in all_ki:
    ki_by_kit_id[ki["kit_id"]].add(ki["item_id"])

kit_id_by_sku = {k["sku"]: k["id"] for k in kits}

all_si = paginate(db.table("shipment_items").select("*"))
si_by_ship = defaultdict(set)
for si in all_si:
    si_by_ship[si["shipment_id"]].add(si["item_id"])

ship_by_id = {s["id"]: s for s in paginate(db.table("shipments").select("*"))}
history_by_cust = defaultdict(set)
for sid, items in si_by_ship.items():
    s = ship_by_id.get(sid)
    if s:
        history_by_cust[s["customer_id"]].update(items)

overlap_issues = 0
checked = 0
for s in covered[:20]:
    kit_id = kit_id_by_sku.get(s["recommended_kit_sku"])
    if not kit_id:
        continue
    kit_items = ki_by_kit_id.get(kit_id, set())
    cust_items = history_by_cust.get(s["customer_id"], set())
    overlap = kit_items & cust_items
    if overlap:
        overlap_issues += 1
        print(f"    ⚠️ Customer {s['customer_id'][:8]}... has {len(overlap)} item overlaps with {s['recommended_kit_sku']}")
    checked += 1

if overlap_issues == 0:
    print(f"  ✅ Spot-checked {checked} covered customers — zero item overlaps")
else:
    print(f"  ⚠️ {overlap_issues}/{checked} had item overlaps (engine allows this when ALL kits overlap)")

# Build quantity check 
print("\n  BUILD QUANTITY VERIFICATION:")
stored_run = db.table("curation_runs").select("*").eq("id", RUN_ID).execute()
if stored_run.data:
    run = stored_run.data[0]
    report_data = run.get("report_json", {}) or {}
    exec_data = report_data.get("executive", {})
    for tri in [2, 3, 4]:
        tri_key = f"t{tri}"
        tri_data = exec_data.get(tri_key, {})
        if tri_data:
            print(f"  T{tri}: projected={tri_data.get('projected')}, covered={tri_data.get('covered')}, need_new={tri_data.get('need_new')}, build_qty={tri_data.get('build_qty')}")

print("\n" + "=" * 80)
print("  ALL VERIFICATIONS COMPLETE")
print("=" * 80)
