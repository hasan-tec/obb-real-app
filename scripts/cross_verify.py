"""
Cross-verification script: Is 1604 renewals correct?
Checking DB counts, statuses, and whether the logic is right.
"""
import os
from dotenv import load_dotenv
from supabase import create_client
from collections import Counter, defaultdict
from datetime import date, timedelta

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://tkcvvjxmzfjaesdhyfiy.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY in environment")

db = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ═══════════════════════════════════════════════════════
# 1. TOTAL CUSTOMERS IN DB
# ═══════════════════════════════════════════════════════
print("=" * 70)
print("  1. TOTAL CUSTOMERS IN DATABASE")
print("=" * 70)

all_custs = []
offset = 0
while True:
    batch = db.table("customers").select("id, email, subscription_status, due_date, platform").range(offset, offset + 999).execute()
    all_custs.extend(batch.data or [])
    if len(batch.data or []) < 1000:
        break
    offset += 1000

print(f"  Total customers in DB: {len(all_custs)}")

# Status breakdown
status_counts = Counter(c["subscription_status"] for c in all_custs)
print(f"\n  Status breakdown:")
for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
    print(f"    {status or 'NULL'}: {count}")

# Due date presence
has_due = sum(1 for c in all_custs if c.get("due_date"))
no_due = sum(1 for c in all_custs if not c.get("due_date"))
print(f"\n  With due_date: {has_due}")
print(f"  Without due_date: {no_due}")

# Platform breakdown
platform_counts = Counter(c.get("platform", "unknown") for c in all_custs)
print(f"\n  Platform breakdown:")
for plat, count in sorted(platform_counts.items(), key=lambda x: -x[1]):
    print(f"    {plat}: {count}")

# ═══════════════════════════════════════════════════════
# 2. WHAT THE REPORT COUNTS AS "RENEWAL"
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  2. REPORT RENEWAL POOL LOGIC")
print("=" * 70)

# The report does:
# 1. status IN (active, cancelled-prepaid)  [paused excluded by default]
# 2. due_date IS NOT NULL
# 3. has at least 1 shipment in DB

eligible_statuses = ["active", "cancelled-prepaid"]
step1 = [c for c in all_custs if c["subscription_status"] in eligible_statuses]
print(f"  Step 1 - Status in {eligible_statuses}: {len(step1)}")

step2 = [c for c in step1 if c.get("due_date")]
print(f"  Step 2 - Has due_date: {len(step2)}")

# Get ALL customer IDs that have shipments
all_ships = []
offset = 0
while True:
    batch = db.table("shipments").select("customer_id").range(offset, offset + 999).execute()
    all_ships.extend(batch.data or [])
    if len(batch.data or []) < 1000:
        break
    offset += 1000

ship_cust_ids = set(s["customer_id"] for s in all_ships)
print(f"\n  Total shipment records: {len(all_ships)}")
print(f"  Unique customers with shipments: {len(ship_cust_ids)}")

step3 = [c for c in step2 if c["id"] in ship_cust_ids]
print(f"  Step 3 - Has at least 1 shipment (RENEWAL): {len(step3)}")

new_custs = [c for c in step2 if c["id"] not in ship_cust_ids]
print(f"  No shipments yet (NEW): {len(new_custs)}")

print(f"\n  >>> REPORT SAYS: 1604 renewals, 63 new")
print(f"  >>> WE GET:      {len(step3)} renewals, {len(new_custs)} new")

# ═══════════════════════════════════════════════════════
# 3. THE PROBLEM: CANCELLED CUSTOMERS IN THE COUNT
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  3. DEEP DIVE: STATUS OF THE 1604/1667")
print("=" * 70)

# How many are active vs cancelled-prepaid?
renewal_statuses = Counter(c["subscription_status"] for c in step3)
print(f"  Renewal pool by status:")
for status, count in sorted(renewal_statuses.items(), key=lambda x: -x[1]):
    print(f"    {status}: {count}")

new_statuses = Counter(c["subscription_status"] for c in new_custs)
print(f"\n  New customers by status:")
for status, count in sorted(new_statuses.items(), key=lambda x: -x[1]):
    print(f"    {status}: {count}")

# ═══════════════════════════════════════════════════════
# 4. THE REAL QUESTION: Should cancelled-expired be excluded?
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  4. WHAT ABOUT OTHER STATUSES?")
print("=" * 70)

# How many customers with status other than active/cancelled-prepaid
# have due_date + shipments?
other_statuses = [c for c in all_custs 
                  if c["subscription_status"] not in eligible_statuses 
                  and c.get("due_date") 
                  and c["id"] in ship_cust_ids]
print(f"  Customers with shipments + due_date BUT excluded from report:")
other_status_breakdown = Counter(c["subscription_status"] for c in other_statuses)
for status, count in sorted(other_status_breakdown.items(), key=lambda x: -x[1]):
    print(f"    {status}: {count}")
print(f"  Total excluded: {len(other_statuses)}")

# ═══════════════════════════════════════════════════════
# 5. ACTIVE-ONLY COUNT (what Ting probably expects)
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  5. ACTIVE-ONLY COUNT (what Ting might expect)")
print("=" * 70)

active_only = [c for c in all_custs if c["subscription_status"] == "active" and c.get("due_date") and c["id"] in ship_cust_ids]
print(f"  Active + due_date + has shipments: {len(active_only)}")

cancelled_prepaid = [c for c in all_custs if c["subscription_status"] == "cancelled-prepaid" and c.get("due_date") and c["id"] in ship_cust_ids]
print(f"  Cancelled-prepaid + due_date + has shipments: {len(cancelled_prepaid)}")

# ═══════════════════════════════════════════════════════
# 6. SHIPMENT COUNT PER CUSTOMER FOR THE RENEWAL POOL
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  6. SHIPMENT DISTRIBUTION IN RENEWAL POOL")
print("=" * 70)

ship_counts = Counter(s["customer_id"] for s in all_ships)
renewal_ids = set(c["id"] for c in step3)
renewal_ship_counts = [ship_counts.get(cid, 0) for cid in renewal_ids]

from collections import Counter as C2
dist = Counter(renewal_ship_counts)
print(f"  Shipments per customer distribution:")
for count in sorted(dist.keys()):
    print(f"    {count} shipments: {dist[count]} customers")

# Customers with only 1 shipment — are these really renewals? 
# 1 shipment could mean they got a welcome kit only
one_ship = [c for c in step3 if ship_counts.get(c["id"], 0) == 1]
print(f"\n  Customers with exactly 1 shipment: {len(one_ship)}")
print(f"  (These may be welcome-kit-only customers who look like renewals)")

# Check what kit_sku their single shipment has
print(f"\n  Sampling 10 one-shipment customers to check if they're actually welcome kits:")
for c in one_ship[:10]:
    ships = db.table("shipments").select("kit_sku, ship_date").eq("customer_id", c["id"]).execute()
    for s in ships.data:
        is_wk = "WK" in (s.get("kit_sku") or "")
        print(f"    {c['email'][:30]:30} kit={s.get('kit_sku', 'none'):20} date={s.get('ship_date')} {'<-- WELCOME KIT' if is_wk else ''}")

# ═══════════════════════════════════════════════════════
# 7. ITEM QUANTITY TRACKING CHECK
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  7. ITEM vs KIT QUANTITY TRACKING")
print("=" * 70)

# Check items table for quantity columns
sample_items = db.table("items").select("*").limit(3).execute()
if sample_items.data:
    cols = list(sample_items.data[0].keys())
    print(f"  Items table columns: {cols}")
    has_qty = any("quantity" in c.lower() or "stock" in c.lower() for c in cols)
    print(f"  Has quantity/stock column: {has_qty}")

# Check kits table for quantity
sample_kits = db.table("kits").select("*").limit(3).execute()
if sample_kits.data:
    cols = list(sample_kits.data[0].keys())
    print(f"\n  Kits table columns: {cols}")
    has_qty = any("quantity" in c.lower() or "stock" in c.lower() for c in cols)
    print(f"  Has quantity/stock column: {has_qty}")
    
    # Show a few kits with their quantities
    print(f"\n  Sample kits with stock:")
    for k in sample_kits.data:
        print(f"    {k.get('sku', '?'):20} qty_available={k.get('quantity_available', 'N/A')}")

# ═══════════════════════════════════════════════════════
# 8. KIT STOCK VERIFICATION
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  8. KIT STOCK vs REPORT NUMBERS")
print("=" * 70)

# T3 kits with stock
t3_kits = db.table("kits").select("sku, quantity_available, age_rank, is_universal, size_variant, trimester").eq("trimester", 3).eq("is_welcome_kit", False).order("age_rank").execute()
print(f"  ALL T3 kits (not just stock > 0):")
for k in t3_kits.data:
    stock = k.get("quantity_available", 0)
    marker = " ★ IN REPORT" if stock and stock > 0 else " (no stock)"
    print(f"    {k['sku']:20} stock={stock:>5}  age={k['age_rank']:>3}  univ={k['is_universal']}  sz={k['size_variant']}{marker}")

# T4 kits with stock
t4_kits = db.table("kits").select("sku, quantity_available, age_rank, is_universal, size_variant").eq("trimester", 4).eq("is_welcome_kit", False).order("age_rank").execute()
print(f"\n  ALL T4 kits (not just stock > 0):")
for k in t4_kits.data:
    stock = k.get("quantity_available", 0)
    marker = " ★ IN REPORT" if stock and stock > 0 else " (no stock)"
    print(f"    {k['sku']:20} stock={stock:>5}  age={k['age_rank']:>3}  univ={k['is_universal']}  sz={k['size_variant']}{marker}")

print("\n" + "=" * 70)
print("  DONE — REVIEW ABOVE FOR ACCURACY")
print("=" * 70)
