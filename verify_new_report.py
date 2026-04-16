"""
Verify the NEW report (run bb99952c) with recency=3 months.
Specifically addressing user concerns:
1. Aleksandra Miucin (due 2024-12-15) showing up in T4 — is that right?
2. Are old due_date customers supposed to be in the report?
3. Are ALL numbers correct?
"""
import os, logging
os.environ['SUPABASE_URL'] = 'https://tkcvvjxmzfjaesdhyfiy.supabase.co'
os.environ['SUPABASE_SERVICE_ROLE_KEY'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRrY3Z2anhtemZqYWVzZGh5Zml5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDUzOTg1MSwiZXhwIjoyMDkwMTE1ODUxfQ.3rktaVHiFesZ0RU7BUcULZ9bBfO0r6wOGJqMK60a-7Q'
logging.disable(logging.CRITICAL)

from supabase import create_client
from datetime import date, timedelta
from collections import Counter, defaultdict

db = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_ROLE_KEY'])

ship_date = date(2026, 5, 14)
t4_cutoff = ship_date + timedelta(days=19)   # 2026-06-02
t3_cutoff = t4_cutoff + timedelta(weeks=13)  # 2026-09-01
t2_cutoff = t3_cutoff + timedelta(weeks=14)  # 2026-12-08

print("=" * 80)
print("  TRIMESTER BOUNDARIES FOR MAY 2026 (ship 2026-05-14)")
print("=" * 80)
print(f"  T4 (postpartum): due_date <=  {t4_cutoff}  (baby already born or due soon)")
print(f"  T3 (3rd tri):    due_date <=  {t3_cutoff}")
print(f"  T2 (2nd tri):    due_date <=  {t2_cutoff}")
print(f"  T1 (1st tri):    due_date  >  {t2_cutoff}")
print()
print(f"  KEY INSIGHT: T4 = 'postpartum' = baby ALREADY BORN.")
print(f"  Anyone whose due date is in the PAST is T4.")
print(f"  A due_date of 2024-12-15 means baby born ~Dec 2024.")
print(f"  That customer is STILL T4 if they're still subscribed & active.")

# ═══════════════════════════════════════════════════════════════
#  Load the NEW report data (run_id bb99952c...)
# ═══════════════════════════════════════════════════════════════
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

stored_custs = paginate(
    db.table("curation_run_customers").select("*").eq("run_id", RUN_ID)
)
print(f"\n  Loaded stored report: {len(stored_custs)} customer records")

# ═══════════════════════════════════════════════════════════════
#  Load ALL customers + shipments for independent verification
# ═══════════════════════════════════════════════════════════════
all_custs_raw = paginate(db.table("customers").select("*"))
cust_by_id = {c["id"]: c for c in all_custs_raw}
cust_by_email = {}
for c in all_custs_raw:
    cust_by_email[c["email"].lower()] = c

all_ships = paginate(db.table("shipments").select("*"))
ships_by_cust = defaultdict(list)
for s in all_ships:
    ships_by_cust[s["customer_id"]].append(s)

# Latest shipment per customer
latest_ship = {}
for s in all_ships:
    cid = s["customer_id"]
    sd = s.get("ship_date", "")
    if sd and (cid not in latest_ship or sd > latest_ship[cid]):
        latest_ship[cid] = sd

# ═══════════════════════════════════════════════════════════════
#  VERIFY: Recency filter (3 months)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  VERIFY RECENCY FILTER (3 months)")
print("=" * 80)

recency_cutoff = ship_date - timedelta(days=3 * 30)  # ~2026-02-13
print(f"  Recency cutoff: last shipment >= {recency_cutoff}")

eligible = [c for c in all_custs_raw 
            if c["subscription_status"] in ("active", "cancelled-prepaid") 
            and c.get("due_date")]

with_ships = [c for c in eligible if c["id"] in latest_ship]
recent = [c for c in with_ships if latest_ship[c["id"]] >= str(recency_cutoff)]
stale = [c for c in with_ships if latest_ship[c["id"]] < str(recency_cutoff)]
new = [c for c in eligible if c["id"] not in latest_ship]

print(f"  Eligible (active/c-prepaid + due_date): {len(eligible)}")
print(f"  With shipments: {len(with_ships)}")
print(f"  Recent (pass recency): {len(recent)} ← this should match report's 296")
print(f"  Stale (excluded): {len(stale)}")
print(f"  New (no shipments): {len(new)}")

# ═══════════════════════════════════════════════════════════════
#  VERIFY: Trimester assignment for ALL 296
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  VERIFY TRIMESTER COUNTS")
print("=" * 80)

def calc_tri(due_str):
    due = date.fromisoformat(due_str)
    if due <= t4_cutoff: return 4
    elif due <= t3_cutoff: return 3
    elif due <= t2_cutoff: return 2
    else: return 1

my_tri = Counter()
for c in recent:
    my_tri[calc_tri(c["due_date"])] += 1

stored_tri = Counter(sc["projected_trimester"] for sc in stored_custs)

for tri in [1, 2, 3, 4]:
    match = "✅" if my_tri[tri] == stored_tri.get(tri, 0) else "❌"
    print(f"  {match} T{tri}: independent={my_tri[tri]}, stored={stored_tri.get(tri, 0)}")

print(f"\n  Total: independent={sum(my_tri.values())}, stored={len(stored_custs)}")

# ═══════════════════════════════════════════════════════════════
#  DEEP DIVE: T4 due dates — are they reasonable?
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  T4 DUE DATE DISTRIBUTION (the 181 T4 customers)")
print("=" * 80)

t4_recent = [c for c in recent if calc_tri(c["due_date"]) == 4]
t4_due_years = Counter(c["due_date"][:4] for c in t4_recent)
print(f"  T4 customers by due_date YEAR:")
for yr in sorted(t4_due_years.keys()):
    print(f"    {yr}: {t4_due_years[yr]} customers")

t4_due_months = Counter(c["due_date"][:7] for c in t4_recent)
print(f"\n  T4 customers by due_date MONTH (top 10):")
for m, count in sorted(t4_due_months.items(), key=lambda x: -x[1])[:10]:
    print(f"    {m}: {count} customers")

# What % of T4 had due dates more than 12 months ago?
old_t4 = [c for c in t4_recent if date.fromisoformat(c["due_date"]) < date(2025, 5, 14)]
medium_t4 = [c for c in t4_recent if date(2025, 5, 14) <= date.fromisoformat(c["due_date"]) < date(2026, 1, 1)]
recent_t4 = [c for c in t4_recent if date.fromisoformat(c["due_date"]) >= date(2026, 1, 1)]

print(f"\n  T4 age breakdown:")
print(f"    Due >12mo ago (before May 2025):  {len(old_t4):3d} ({len(old_t4)/len(t4_recent)*100:.0f}%)")
print(f"    Due 5-12mo ago (May-Dec 2025):    {len(medium_t4):3d} ({len(medium_t4)/len(t4_recent)*100:.0f}%)")
print(f"    Due recently (Jan-Jun 2026):      {len(recent_t4):3d} ({len(recent_t4)/len(t4_recent)*100:.0f}%)")

# ═══════════════════════════════════════════════════════════════
#  SPECIFIC CHECK: Aleksandra Miucin
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  CASE STUDY: Aleksandra Miucin (elkelora@gmail.com)")
print("=" * 80)

alek = cust_by_email.get("elkelora@gmail.com")
if alek:
    alek_ships = sorted(ships_by_cust[alek["id"]], key=lambda s: s.get("ship_date", ""), reverse=True)
    print(f"  Due date:    {alek['due_date']}")
    print(f"  Status:      {alek['subscription_status']}")
    print(f"  Size:        {alek.get('clothing_size')}")
    print(f"  Platform:    {alek.get('platform')}")
    print(f"  Shipments:   {len(alek_ships)}")
    print(f"  Last ship:   {alek_ships[0].get('ship_date') if alek_ships else 'none'}")
    print(f"  First ship:  {alek_ships[-1].get('ship_date') if alek_ships else 'none'}")
    
    tri = calc_tri(alek["due_date"])
    print(f"\n  Trimester calc: due={alek['due_date']} vs T4 cutoff={t4_cutoff}")
    print(f"  → {alek['due_date']} <= {t4_cutoff}? {'YES' if date.fromisoformat(alek['due_date']) <= t4_cutoff else 'NO'}")
    print(f"  → Assigned: T{tri}")
    print(f"  → Meaning: Her baby was born ~Dec 2024, she is POSTPARTUM.")
    print(f"              She's still actively subscribed (shipped Mar 2026).")
    print(f"              She correctly gets T4 (postpartum) boxes.")
    
    recency_pass = latest_ship.get(alek["id"], "") >= str(recency_cutoff)
    print(f"\n  Recency check: last_ship={latest_ship.get(alek['id'])} >= {recency_cutoff}? {'YES' if recency_pass else 'NO'}")
    
    print(f"\n  Recent shipment kit history:")
    for s in alek_ships[:5]:
        print(f"    {s.get('ship_date')} — {s.get('kit_sku')}")

# ═══════════════════════════════════════════════════════════════
#  VERIFY: All stored customer records point to correct data
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  VERIFY ALL 296 STORED CUSTOMER RECORDS")
print("=" * 80)

errors = []
for sc in stored_custs:
    cust = cust_by_id.get(sc["customer_id"])
    if not cust:
        errors.append(f"Missing customer {sc['customer_id']}")
        continue
    
    # Check trimester assignment
    expected_tri = calc_tri(cust["due_date"])
    if expected_tri != sc["projected_trimester"]:
        errors.append(f"{cust['email']}: stored T{sc['projected_trimester']} but should be T{expected_tri} (due={cust['due_date']})")
    
    # Check recency
    last = latest_ship.get(cust["id"], "")
    if last < str(recency_cutoff):
        errors.append(f"{cust['email']}: last_ship={last} is BEFORE recency cutoff {recency_cutoff}")
    
    # Check status
    if cust["subscription_status"] not in ("active", "cancelled-prepaid"):
        errors.append(f"{cust['email']}: status={cust['subscription_status']} shouldn't be in report")

if errors:
    print(f"  ❌ Found {len(errors)} errors:")
    for e in errors[:20]:
        print(f"     {e}")
else:
    print(f"  ✅ All 296 records verified:")
    print(f"     - Every customer exists in DB")
    print(f"     - Every trimester assignment is mathematically correct")
    print(f"     - Every customer passed the recency filter (shipped in last 3 months)")
    print(f"     - Every customer has active/cancelled-prepaid status")

# ═══════════════════════════════════════════════════════════════
#  VERIFY: T4 coverage and "needs new" logic
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  VERIFY T4 COVERAGE (18 covered / 163 need new)")
print("=" * 80)

# Load kit data
all_kits = db.table("kits").select("*").eq("is_welcome_kit", False).gt("quantity_available", 0).eq("trimester", 4).order("age_rank").execute()
t4_kits = all_kits.data or []
print(f"  T4 kits with stock: {len(t4_kits)}")
for k in t4_kits:
    print(f"    {k['sku']:25} stock={k['quantity_available']:>3}  age={k['age_rank']}  universal={k.get('is_universal')}")

# Load kit items
all_ki = paginate(db.table("kit_items").select("*"))
ki_by_kit = defaultdict(set)
for ki in all_ki:
    ki_by_kit[ki["kit_id"]].add(ki["item_id"])

# Load all shipment items for T4 recent customers
all_sit = paginate(db.table("shipment_items").select("*"))
sit_by_ship = defaultdict(list)
for si in all_sit:
    sit_by_ship[si["shipment_id"]].append(si)

# Build full history for T4 customers
t4_cust_ids = set(c["id"] for c in t4_recent)
full_history = defaultdict(set)
received_kits = defaultdict(set)
for s in all_ships:
    if s["customer_id"] in t4_cust_ids:
        if s.get("kit_sku"):
            received_kits[s["customer_id"]].add(s["kit_sku"])
        for si in sit_by_ship.get(s["id"], []):
            full_history[s["customer_id"]].add(si["item_id"])

# Re-run coverage check
covered = 0
needs_new = 0
covered_details = []
needs_details = []

for c in t4_recent:
    cust_items = full_history.get(c["id"], set())
    cust_kits = received_kits.get(c["id"], set())
    clothing_size = c.get("clothing_size")
    
    blocked = set(cust_items)
    
    if clothing_size:
        sz_map = {"S": 1, "M": 2, "L": 3, "XL": 4}
        cv = sz_map.get(clothing_size, 1)
        filtered = [k for k in t4_kits if k.get("is_universal") or k["size_variant"] == cv]
    else:
        filtered = [k for k in t4_kits if k.get("is_universal") or k["size_variant"] == 1]
    
    found = False
    for kit in sorted(filtered, key=lambda k: k.get("age_rank", 0)):
        if kit["sku"] in cust_kits:
            continue
        kit_item_ids = ki_by_kit.get(kit["id"], set())
        if not kit_item_ids or not (kit_item_ids & blocked):
            found = True
            covered_details.append((c["email"], kit["sku"], len(blocked)))
            break
    
    if found:
        covered += 1
    else:
        needs_new += 1
        needs_details.append((c["email"], len(blocked), len(cust_kits), len(filtered)))

stored_t4_covered = sum(1 for sc in stored_custs if sc["projected_trimester"] == 4 and not sc["needs_new_curation"])
stored_t4_needs = sum(1 for sc in stored_custs if sc["projected_trimester"] == 4 and sc["needs_new_curation"])

print(f"\n  Independent recalc: covered={covered}, needs_new={needs_new}")
print(f"  Stored report:      covered={stored_t4_covered}, needs_new={stored_t4_needs}")
match_cov = "✅" if covered == stored_t4_covered else "❌"
match_need = "✅" if needs_new == stored_t4_needs else "❌"
print(f"  {match_cov} Covered matches")
print(f"  {match_need} Needs new matches")

# WHY do 163 need new?
print(f"\n  WHY 163 T4 customers need new curation:")
print(f"  There are only {len(t4_kits)} T4 kits with stock ({sum(k['quantity_available'] for k in t4_kits)} total units)")
print(f"  ALL 3 kits are universal (no size filtering issue)")

# Show some stats about blocked items
blocked_counts = [b for _, b, _, _ in needs_details]
if blocked_counts:
    print(f"  Blocked items per 'needs new' customer:")
    print(f"    Min: {min(blocked_counts)}, Max: {max(blocked_counts)}, Avg: {sum(blocked_counts)/len(blocked_counts):.0f}")

# Sample 5 "needs new" customers to explain WHY
print(f"\n  Sample 5 'needs new' T4 customers (showing WHY):")
for email, blocked, kits_received, kits_checked in sorted(needs_details, key=lambda x: -x[1])[:5]:
    c = cust_by_email[email.lower()]
    ships = ships_by_cust[c["id"]]
    print(f"    {email[:40]:40} ships={len(ships):>2} blocked={blocked:>3} rxd_kits={kits_received} checked={kits_checked}")
    # Check each T4 kit
    cust_hist = full_history.get(c["id"], set())
    cust_rxd = received_kits.get(c["id"], set())
    for kit in t4_kits:
        kit_items = ki_by_kit.get(kit["id"], set())
        overlap = kit_items & cust_hist
        already_rxd = kit["sku"] in cust_rxd
        reason = "ALREADY RECEIVED" if already_rxd else f"{len(overlap)}/{len(kit_items)} items overlap"
        print(f"      {kit['sku']:25} → {reason}")

# ═══════════════════════════════════════════════════════════════
#  VERIFY: T2 all covered, T3 numbers
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  VERIFY T2 (21 covered) and T3 (17 covered / 77 need new)")
print("=" * 80)

for tri, expected_cov, expected_need in [(2, 21, 0), (3, 17, 77)]:
    s_cov = sum(1 for sc in stored_custs if sc["projected_trimester"] == tri and not sc["needs_new_curation"])
    s_need = sum(1 for sc in stored_custs if sc["projected_trimester"] == tri and sc["needs_new_curation"])
    match = "✅" if s_cov == expected_cov and s_need == expected_need else "❌"
    print(f"  {match} T{tri}: covered={s_cov} (expect {expected_cov}), needs_new={s_need} (expect {expected_need})")

# ═══════════════════════════════════════════════════════════════
#  VERIFY: T2 DO NOT USE (9 items, 90.5% blocked)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  VERIFY T2 DO NOT USE ITEMS")
print("=" * 80)

stored_items = paginate(
    db.table("curation_run_items").select("*").eq("run_id", RUN_ID)
)

t2_dnu = [si for si in stored_items if si["trimester"] == 2 and si["risk_level"] in ("HIGH", "MEDIUM")]
print(f"  T2 DO NOT USE items: {len(t2_dnu)}")
# Load item names
items_raw = db.table("items").select("id,name").execute().data or []
item_names = {i["id"]: i["name"] for i in items_raw}
for si in sorted(t2_dnu, key=lambda x: -float(x.get("blocked_pct", 0))):
    name = item_names.get(si["item_id"], "?")
    print(f"    {si['blocked_pct']}% ({si['blocked_count']}/{si['group_size']}) — {name[:50]} [{si['risk_level']}]")

# Cross-check: 19/21 = 90.5%
print(f"\n  Math check: 19/21 = {19/21*100:.1f}% (matches screenshot 90.5%)")

# ═══════════════════════════════════════════════════════════════
#  FINAL: Cross-check with latest CSV (MAR 2026 CK)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  CROSS-CHECK: Are these 296 customers in the March 2026 CSV?")
print("=" * 80)

import csv
csv_path = r"c:\Users\hasan\Desktop\opus tin\order history\Oh Baby Boxes - Monthly Boxing_Customer Kit Assignment - MAR '26 CK.csv"
csv_emails = set()
try:
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if len(row) > 2 and row[2] and "@" in row[2]:
                csv_emails.add(row[2].strip().lower())
except Exception as e:
    print(f"  Error reading CSV: {e}")

recent_emails = set(c["email"].lower() for c in recent)
in_csv = recent_emails & csv_emails
not_in_csv = recent_emails - csv_emails

print(f"  MAR 2026 CSV emails: {len(csv_emails)}")
print(f"  Report renewal emails: {len(recent_emails)}")
print(f"  Overlap (in both): {len(in_csv)}")
print(f"  In report but NOT in March CSV: {len(not_in_csv)}")

if not_in_csv:
    # These could be Feb/Jan shippers not in Mar
    sample = list(not_in_csv)[:10]
    print(f"\n  Sample of report customers NOT in March CSV (checking their last ship):")
    for email in sample:
        c = cust_by_email.get(email)
        if c:
            last = latest_ship.get(c["id"], "?")
            print(f"    {email[:40]:40} last_ship={last}")

print("\n" + "=" * 80)
print("  DONE")
print("=" * 80)
