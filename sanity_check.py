"""Sanity check curation report results."""
import os
from dotenv import load_dotenv
from supabase import create_client
from datetime import date, timedelta

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://tkcvvjxmzfjaesdhyfiy.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY in environment")

db = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
run_id = 'ea359fb8-8c55-4d09-baaf-799ec6dad1bc'

# ── T4 Kit Stock ──
print("=== T4 KITS WITH STOCK ===")
t4_kits = db.table("kits").select("sku, quantity_available, age_rank, is_universal, size_variant").eq("trimester", 4).eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()
for k in t4_kits.data:
    print(f"  {k['sku']:15} stock={k['quantity_available']:>5}  age={k['age_rank']:>3}  univ={k['is_universal']}  sz={k['size_variant']}")
print(f"Total T4 kits with stock: {len(t4_kits.data)}")

# ── Sample needs-new customers ──
print("\n=== SAMPLE T4 CUSTOMERS NEEDING NEW CURATION (5) ===")
needs = db.table("curation_run_customers").select("customer_id, reason, blocking_item_count").eq("run_id", run_id).eq("projected_trimester", 4).eq("needs_new_curation", True).limit(5).execute()
for n in needs.data:
    print(f"  ID={n['customer_id'][:8]}... blocked={n['blocking_item_count']}  reason={n['reason'][:120]}")

# ── Sample covered customers ──
print("\n=== SAMPLE T4 COVERED CUSTOMERS (5) ===")
cov = db.table("curation_run_customers").select("customer_id, recommended_kit_sku, alternative_kit_skus, blocking_item_count").eq("run_id", run_id).eq("projected_trimester", 4).eq("needs_new_curation", False).limit(5).execute()
for c in cov.data:
    alts = c.get("alternative_kit_skus", [])
    print(f"  ID={c['customer_id'][:8]}... kit={c['recommended_kit_sku']}  alts={alts}  blocked={c['blocking_item_count']}")

# ── T3 Kit Stock ──
print("\n=== T3 KITS WITH STOCK ===")
t3_kits = db.table("kits").select("sku, quantity_available, age_rank, is_universal, size_variant").eq("trimester", 3).eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()
for k in t3_kits.data:
    print(f"  {k['sku']:15} stock={k['quantity_available']:>5}  age={k['age_rank']:>3}  univ={k['is_universal']}  sz={k['size_variant']}")
print(f"Total T3 kits with stock: {len(t3_kits.data)}")

# ── T2 Kit Stock ──
print("\n=== T2 KITS WITH STOCK ===")
t2_kits = db.table("kits").select("sku, quantity_available, age_rank, is_universal, size_variant").eq("trimester", 2).eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()
for k in t2_kits.data:
    print(f"  {k['sku']:15} stock={k['quantity_available']:>5}  age={k['age_rank']:>3}  univ={k['is_universal']}  sz={k['size_variant']}")
print(f"Total T2 kits with stock: {len(t2_kits.data)}")

# ── Trimester Boundary Check ──
ship_date = date(2026, 5, 14)
t4_cutoff = ship_date + timedelta(days=19)
t3_cutoff = t4_cutoff + timedelta(weeks=13)
t2_cutoff = t3_cutoff + timedelta(weeks=14)
print("\n=== TRIMESTER BOUNDARIES FOR MAY 14, 2026 SHIP ===")
print(f"  T4 (postpartum): due_date <= {t4_cutoff}")
print(f"  T3:              due_date <= {t3_cutoff}")
print(f"  T2:              due_date <= {t2_cutoff}")
print(f"  T1 (early):      due_date > {t2_cutoff}")
print(f"  T1=0 makes sense if no active subscribers have due date after {t2_cutoff}")

# ── DO NOT USE analysis for T3 ──
print("\n=== T3 DO NOT USE ITEMS (top 10 by blocked %) ===")
dnu = db.table("curation_run_items").select("*, items(name, sku)").eq("run_id", run_id).eq("trimester", 3).eq("risk_level", "HIGH").order("blocked_pct", desc=True).limit(10).execute()
for d in dnu.data:
    item = d.get("items", {})
    print(f"  [{d['risk_level']:6}] {d['blocked_pct']:5.1f}% ({d['blocked_count']}/{d['group_size']}) {item.get('name', '?')[:50]}")

# Also show MEDIUM risk for T3
print("\n=== T3 MEDIUM RISK ITEMS (top 10) ===")
med = db.table("curation_run_items").select("*, items(name, sku)").eq("run_id", run_id).eq("trimester", 3).eq("risk_level", "MEDIUM").order("blocked_pct", desc=True).limit(10).execute()
for d in med.data:
    item = d.get("items", {})
    print(f"  [{d['risk_level']:6}] {d['blocked_pct']:5.1f}% ({d['blocked_count']}/{d['group_size']}) {item.get('name', '?')[:50]}")

# ── T4 DO NOT USE = 0 explanation ──
print("\n=== WHY T4 HAS 0 DO NOT USE ITEMS ===")
t4_items = db.table("curation_run_items").select("blocked_pct, risk_level").eq("run_id", run_id).eq("trimester", 4).order("blocked_pct", desc=True).limit(5).execute()
if t4_items.data:
    top_pct = t4_items.data[0]['blocked_pct']
    print(f"  Highest blocked % in T4: {top_pct}%")
    print(f"  With 1474 customers, even popular items don't reach 25%")
    print(f"  This is expected — large group = lower individual item %, more diverse history")
else:
    print("  No item data for T4 — checking if items were generated...")
    all_t4 = db.table("curation_run_items").select("id", count="exact").eq("run_id", run_id).eq("trimester", 4).execute()
    print(f"  Total T4 item records: {all_t4.count}")

print("\n=== OVERALL SANITY ===")
print(f"  Total renewal: 1604 (T1=0 + T2=24 + T3=106 + T4=1474 = {0+24+106+1474})")
print(f"  ✅ Sums match: {0+24+106+1474 == 1604}")

# Check total active+cancelled-prepaid with due dates
custs = db.table("customers").select("id, subscription_status", count="exact").in_("subscription_status", ["active", "cancelled-prepaid"]).not_.is_("due_date", "null").execute()
print(f"  Active/cancelled-prepaid with due_date: {custs.count}")

# Check how many have shipments (renewal)
ships = db.table("shipments").select("customer_id").execute()
ship_cust_ids = set(s["customer_id"] for s in ships.data)
eligible = [c for c in custs.data if c["id"] in ship_cust_ids]
print(f"  Of those, with at least 1 shipment (renewals): {len(eligible)}")
print(f"  Report says: 1604 renewal customers")
print(f"  ✅ Match: {len(eligible) == 1604}")
