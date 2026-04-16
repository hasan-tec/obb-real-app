"""Quick check: when was each customer's last shipment? Are they really 'active'?"""
import os
os.environ['SUPABASE_URL'] = 'https://tkcvvjxmzfjaesdhyfiy.supabase.co'
os.environ['SUPABASE_SERVICE_ROLE_KEY'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRrY3Z2anhtemZqYWVzZGh5Zml5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDUzOTg1MSwiZXhwIjoyMDkwMTE1ODUxfQ.3rktaVHiFesZ0RU7BUcULZ9bBfO0r6wOGJqMK60a-7Q'
from supabase import create_client
from collections import Counter
from datetime import date

db = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_ROLE_KEY'])

# Get all shipments with ship_date
all_ships = []
offset = 0
while True:
    batch = db.table("shipments").select("customer_id, ship_date").range(offset, offset + 999).execute()
    all_ships.extend(batch.data or [])
    if len(batch.data or []) < 1000:
        break
    offset += 1000

# Find LATEST shipment per customer
latest = {}
for s in all_ships:
    cid = s["customer_id"]
    sd = s.get("ship_date", "")
    if sd and (cid not in latest or sd > latest[cid]):
        latest[cid] = sd

print(f"Total customers with shipments: {len(latest)}")

# Group by month of last shipment
month_counts = Counter()
for cid, sd in latest.items():
    month_counts[sd[:7]] += 1  # YYYY-MM

print(f"\nCustomers by LAST shipment month:")
print(f"{'Month':12} {'Count':>6}  {'Cumulative from newest':>20}")
cumulative = 0
for month in sorted(month_counts.keys(), reverse=True):
    cumulative += month_counts[month]
    print(f"  {month:10} {month_counts[month]:>6}  {cumulative:>20}")

# So: customers whose latest shipment was MAR 2026 (latest month) = truly active
# Customers whose latest shipment was 6+ months ago = likely cancelled
print(f"\n--- APPROXIMATE ACTIVITY ---")
recent = sum(month_counts[m] for m in month_counts if m >= "2026-01")
recent_3mo = sum(month_counts[m] for m in month_counts if m >= "2025-10")
old = sum(month_counts[m] for m in month_counts if m < "2025-10")
print(f"  Shipped in 2026 (Jan-Mar):         {recent}")
print(f"  Shipped in last 6mo (Oct25-Mar26): {recent_3mo}")
print(f"  Last shipped before Oct 2025:      {old}")
print(f"\n  Ting says ~500 active per month. Most recent month count: {month_counts.get('2026-03', 0)}")
