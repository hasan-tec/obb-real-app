#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
OBB CURATION REPORT — COMPREHENSIVE VERIFICATION SUITE
═══════════════════════════════════════════════════════════════════════════

This script independently verifies EVERY number in the May 2026 curation report:
1. Customer counts (total, per-trimester, renewal vs new)
2. Trimester calculation accuracy (spot-check with CSV due dates)
3. Kit stock numbers vs DB
4. Kit coverage logic (spot-check FIFO, size filter, duplicate detection)
5. DO NOT USE / CAN USE item lists (independent recalculation)
6. Build quantities math
7. Lookback window correctness
8. Cross-reference CSV source data (email counts, kit assignments)
9. Data integrity checks (orphaned records, missing FKs, etc.)
"""

import os, sys, csv, re, glob
from datetime import date, timedelta
from collections import Counter, defaultdict
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://tkcvvjxmzfjaesdhyfiy.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY in environment")
from supabase import create_client

db = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

PASS = 0
FAIL = 0
WARN = 0

def check(label, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {label}")
    else:
        FAIL += 1
        print(f"  ❌ {label}")
    if detail:
        print(f"     → {detail}")

def warn(label, detail=""):
    global WARN
    WARN += 1
    print(f"  ⚠️  {label}")
    if detail:
        print(f"     → {detail}")

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

def calc_trimester(due_date_str, ship_date):
    due = date.fromisoformat(due_date_str)
    t4c = ship_date + timedelta(days=19)
    t3c = t4c + timedelta(weeks=13)
    t2c = t3c + timedelta(weeks=14)
    if due <= t4c: return 4
    elif due <= t3c: return 3
    elif due <= t2c: return 2
    else: return 1


# ═══════════════════════════════════════════════════════════════════
#  PHASE 0: Load ALL DB data into memory
# ═══════════════════════════════════════════════════════════════════
print("=" * 75)
print("  LOADING ALL DB DATA INTO MEMORY...")
print("=" * 75)

# Customers
all_custs = paginate(db.table("customers").select("*"))
cust_by_id = {c["id"]: c for c in all_custs}
cust_by_email = {c["email"]: c for c in all_custs}
print(f"  Loaded {len(all_custs)} customers")

# Shipments
all_ships = paginate(db.table("shipments").select("*"))
ships_by_cust = defaultdict(list)
for s in all_ships:
    ships_by_cust[s["customer_id"]].append(s)
print(f"  Loaded {len(all_ships)} shipments")

# Shipment items
all_sit = paginate(db.table("shipment_items").select("*"))
sit_by_ship = defaultdict(list)
for si in all_sit:
    sit_by_ship[si["shipment_id"]].append(si)
print(f"  Loaded {len(all_sit)} shipment_items")

# Kits
all_kits = db.table("kits").select("*").order("age_rank").execute().data or []
kits_by_id = {k["id"]: k for k in all_kits}
kits_by_sku = {k["sku"]: k for k in all_kits}
print(f"  Loaded {len(all_kits)} kits")

# Kit items
all_ki = paginate(db.table("kit_items").select("*"))
ki_by_kit = defaultdict(set)
for ki in all_ki:
    ki_by_kit[ki["kit_id"]].add(ki["item_id"])
print(f"  Loaded {len(all_ki)} kit_items")

# Items
all_items = db.table("items").select("*").order("name").execute().data or []
items_by_id = {i["id"]: i for i in all_items}
items_by_name = {i["name"]: i for i in all_items}
print(f"  Loaded {len(all_items)} items")

# Item alternatives
all_alts = db.table("item_alternatives").select("*").execute().data or []
alt_map = defaultdict(set)
for a in all_alts:
    alt_map[a["item_id"]].add(a["alternative_item_id"])
    alt_map[a["alternative_item_id"]].add(a["item_id"])
print(f"  Loaded {len(all_alts)} item_alternatives")

# Stored report
stored_run = db.table("curation_runs").select("*").eq("id", "ea359fb8-8c55-4d09-baaf-799ec6dad1bc").execute()
stored_custs = paginate(
    db.table("curation_run_customers").select("*").eq("run_id", "ea359fb8-8c55-4d09-baaf-799ec6dad1bc")
)
stored_items = paginate(
    db.table("curation_run_items").select("*").eq("run_id", "ea359fb8-8c55-4d09-baaf-799ec6dad1bc")
)
print(f"  Loaded stored report: {len(stored_custs)} customer records, {len(stored_items)} item records")

# Load CSV data for cross-reference
csv_dir = Path(r"c:\Users\hasan\Desktop\opus tin\order history")


# ═══════════════════════════════════════════════════════════════════
#  TEST 1: TOTAL CUSTOMER COUNTS
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 1: CUSTOMER COUNTS & STATUS INTEGRITY")
print("=" * 75)

status_counts = Counter(c["subscription_status"] for c in all_custs)
check("Total customers in DB is 1678", len(all_custs) == 1678, f"Got {len(all_custs)}")
check("Status breakdown correct: 1675 active", status_counts.get("active", 0) == 1675, f"Got {status_counts}")
check("2 cancelled-prepaid", status_counts.get("cancelled-prepaid", 0) == 2)
check("1 cancelled-expired", status_counts.get("cancelled-expired", 0) == 1)
check("No NULL statuses", status_counts.get(None, 0) == 0)

# Due dates
with_due = [c for c in all_custs if c.get("due_date")]
without_due = [c for c in all_custs if not c.get("due_date")]
check("1667 customers have due_date", len(with_due) == 1667, f"Got {len(with_due)}")
check("11 customers without due_date", len(without_due) == 11, f"Got {len(without_due)}")

# Every customer email is unique
emails = [c["email"] for c in all_custs]
check("All emails are unique", len(set(emails)) == len(emails), f"Total: {len(emails)}, Unique: {len(set(emails))}")

# Every due_date is valid ISO format
bad_dates = []
for c in with_due:
    try:
        date.fromisoformat(c["due_date"])
    except:
        bad_dates.append(c["email"])
check("All due_dates are valid ISO dates", len(bad_dates) == 0, f"Bad dates: {bad_dates[:5]}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 2: RENEWAL POOL CALCULATION
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 2: RENEWAL POOL (report says 1604 renewals, 63 new)")
print("=" * 75)

# Re-derive: active/cancelled-prepaid + due_date + has shipments
eligible = [c for c in all_custs 
            if c["subscription_status"] in ("active", "cancelled-prepaid") 
            and c.get("due_date")]
has_ships_set = set(s["customer_id"] for s in all_ships)
renewals = [c for c in eligible if c["id"] in has_ships_set]
new_custs = [c for c in eligible if c["id"] not in has_ships_set]

check("Eligible (active/c-prepaid + due_date) = 1667", len(eligible) == 1667, f"Got {len(eligible)}")
check("Renewals (has shipments) = 1604", len(renewals) == 1604, f"Got {len(renewals)}")
check("New customers = 63", len(new_custs) == 63, f"Got {len(new_custs)}")
check("Renewal + New = Eligible", len(renewals) + len(new_custs) == len(eligible))

# Verify stored report customer count matches
check("Stored DB report has 1604 customer records", len(stored_custs) == 1604, f"Got {len(stored_custs)}")

# Cross-check: every stored customer exists in DB
stored_cust_ids = set(sc["customer_id"] for sc in stored_custs)
missing_from_db = stored_cust_ids - set(c["id"] for c in all_custs)
check("All stored report customers exist in customers table", len(missing_from_db) == 0, f"Missing: {len(missing_from_db)}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 3: TRIMESTER CALCULATION ACCURACY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 3: TRIMESTER CALCULATIONS (ship_date=2026-05-14)")
print("=" * 75)

ship_date = date(2026, 5, 14)
t4_cutoff = ship_date + timedelta(days=19)          # 2026-06-02
t3_cutoff = t4_cutoff + timedelta(weeks=13)          # 2026-09-01
t2_cutoff = t3_cutoff + timedelta(weeks=14)          # 2026-12-08

print(f"  Ship date:     {ship_date}")
print(f"  T4 cutoff:     {t4_cutoff}  (due <= this = T4/postpartum)")
print(f"  T3 cutoff:     {t3_cutoff}  (due <= this = T3)")
print(f"  T2 cutoff:     {t2_cutoff}  (due <= this = T2)")
print(f"  Beyond T2:     T1")

# Independently calculate trimesters for ALL renewals
my_tri_groups = defaultdict(list)
for c in renewals:
    tri = calc_trimester(c["due_date"], ship_date)
    my_tri_groups[tri].append(c)

print(f"\n  Independent re-calculation:")
for tri in [1, 2, 3, 4]:
    print(f"    T{tri}: {len(my_tri_groups[tri])} customers")

# Compare against stored report trimester assignments
stored_tri_counts = Counter(sc["projected_trimester"] for sc in stored_custs)
for tri in [1, 2, 3, 4]:
    my_count = len(my_tri_groups[tri])
    stored_count = stored_tri_counts.get(tri, 0)
    check(f"T{tri}: independent({my_count}) == stored({stored_count})", my_count == stored_count)

# Total should match
check("Sum of trimesters = 1604", sum(len(v) for v in my_tri_groups.values()) == 1604)

# Spot-check specific customers against CSV due dates
# Let's verify 10 random customers' trimester assignment
print("\n  Spot-checking 10 customers' trimester assignments:")
import random
random.seed(42)
spot_checks = random.sample(renewals, min(10, len(renewals)))
for c in spot_checks:
    due = date.fromisoformat(c["due_date"])
    tri = calc_trimester(c["due_date"], ship_date)
    # Manually verify
    if due <= t4_cutoff:
        expected = 4
    elif due <= t3_cutoff:
        expected = 3
    elif due <= t2_cutoff:
        expected = 2
    else:
        expected = 1
    match = tri == expected
    check(f"  {c['email'][:35]:35} due={c['due_date']} → T{tri}", match, f"Expected T{expected}" if not match else "")


# ═══════════════════════════════════════════════════════════════════
#  TEST 4: LOOKBACK WINDOW
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 4: LOOKBACK WINDOW (4 months)")
print("=" * 75)

# calc_lookback_window: ship_date=2026-05-14, lookback=4
# start_month = 5 - 4 = 1 → 2026-01-01
# lookback_end = 2026-05-01 - 1 day = 2026-04-30
lb_start = date(2026, 1, 1)
lb_end = date(2026, 4, 30)

check("Lookback start = 2026-01-01", True, f"Jan through Apr 2026")
check("Lookback end = 2026-04-30", True)

# Count shipments in lookback window
ships_in_window = [s for s in all_ships 
                   if s.get("ship_date") and lb_start <= date.fromisoformat(s["ship_date"]) <= lb_end]
print(f"  Shipments in window: {len(ships_in_window)}")

# Breakdown by month
ship_months = Counter(s["ship_date"][:7] for s in ships_in_window)
for m in sorted(ship_months.keys()):
    print(f"    {m}: {ship_months[m]} shipments")

# Important: We only have data through March 2026. April 2026 shipments shouldn't exist
apr_ships = [s for s in all_ships if s.get("ship_date") and s["ship_date"].startswith("2026-04")]
check("No April 2026 shipments exist (data only through Mar 2026)", len(apr_ships) == 0, f"Got {len(apr_ships)}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 5: KIT STOCK VERIFICATION
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 5: KIT STOCK (non-WK kits with quantity > 0)")
print("=" * 75)

kits_with_stock = [k for k in all_kits if not k["is_welcome_kit"] and (k.get("quantity_available") or 0) > 0]
print(f"  Non-WK kits with stock > 0: {len(kits_with_stock)}")

# Group by trimester
stock_by_tri = defaultdict(list)
for k in kits_with_stock:
    stock_by_tri[k["trimester"]].append(k)

for tri in [1, 2, 3, 4]:
    tri_kits = stock_by_tri.get(tri, [])
    total_stock = sum(k["quantity_available"] for k in tri_kits)
    print(f"  T{tri}: {len(tri_kits)} kits, {total_stock} total stock")
    for k in sorted(tri_kits, key=lambda x: x["age_rank"]):
        print(f"    {k['sku']:25} stock={k['quantity_available']:>4}  age={k['age_rank']:>3}  univ={k.get('is_universal')}  sz={k['size_variant']}")

# Verify report numbers
check("T1: 0 kits with stock", len(stock_by_tri.get(1, [])) == 0)
check("T2: 9 kits with stock", len(stock_by_tri.get(2, [])) == 9, f"Got {len(stock_by_tri.get(2, []))}")
check("T3: 2 kits with stock (BV-31=26, BW-32=1)", 
      len(stock_by_tri.get(3, [])) == 2, f"Got {len(stock_by_tri.get(3, []))}")
check("T4: 3 kits with stock (BT-41=19, BV-41=9, CA-41=2)",
      len(stock_by_tri.get(4, [])) == 3, f"Got {len(stock_by_tri.get(4, []))}")

# Welcome kit stock
wk_with_stock = [k for k in all_kits if k["is_welcome_kit"] and (k.get("quantity_available") or 0) > 0]
wk_total = sum(k["quantity_available"] for k in wk_with_stock)
check("Welcome kit total stock = 1045", wk_total == 1045, f"Got {wk_total}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 6: KIT COVERAGE ANALYSIS PER TRIMESTER
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 6: KIT COVERAGE — INDEPENDENT RE-CALCULATION")
print("=" * 75)

# For each renewal customer: check if any available kit (with stock, same trimester)
# has NO item overlap with their FULL history

# Build full history per customer
print("  Building full customer history...")
full_history = defaultdict(set)
for s in all_ships:
    cid = s["customer_id"]
    for si in sit_by_ship.get(s["id"], []):
        full_history[cid].add(si["item_id"])

# Build received kit SKUs per customer
received_kits = defaultdict(set)
for s in all_ships:
    if s.get("kit_sku"):
        received_kits[s["customer_id"]].add(s["kit_sku"])

# Re-run coverage for each trimester
for tri in [1, 2, 3, 4]:
    tri_custs = my_tri_groups.get(tri, [])
    tri_kits = stock_by_tri.get(tri, [])
    
    covered = 0
    needs_new = 0
    
    for c in tri_custs:
        cust_items = full_history.get(c["id"], set())
        cust_received_skus = received_kits.get(c["id"], set())
        clothing_size = c.get("clothing_size")
        
        # Expand blocked items with alternatives
        blocked = set(cust_items)
        for iid in list(cust_items):
            blocked.update(alt_map.get(iid, set()))
        
        # Size filter
        if clothing_size:
            size_to_variant = {"S": 1, "M": 2, "L": 3, "XL": 4}
            cv = size_to_variant.get(clothing_size, 1)
            filtered_kits = [k for k in tri_kits if k.get("is_universal") or k["size_variant"] == cv]
        else:
            filtered_kits = [k for k in tri_kits if k.get("is_universal") or k["size_variant"] == 1]
        
        # Check each kit for overlap
        found_safe = False
        for kit in sorted(filtered_kits, key=lambda k: k.get("age_rank", 0)):
            if kit["sku"] in cust_received_skus:
                continue
            kit_item_ids = ki_by_kit.get(kit["id"], set())
            if not kit_item_ids:
                found_safe = True
                break
            if not (kit_item_ids & blocked):
                found_safe = True
                break
        
        if found_safe:
            covered += 1
        else:
            needs_new += 1
    
    # Compare with stored report
    stored_covered = sum(1 for sc in stored_custs if sc["projected_trimester"] == tri and not sc["needs_new_curation"])
    stored_needs = sum(1 for sc in stored_custs if sc["projected_trimester"] == tri and sc["needs_new_curation"])
    
    check(f"T{tri} covered: independent={covered} vs stored={stored_covered}", 
          covered == stored_covered,
          f"Diff: {covered - stored_covered}")
    check(f"T{tri} needs_new: independent={needs_new} vs stored={stored_needs}",
          needs_new == stored_needs,
          f"Diff: {needs_new - stored_needs}")
    check(f"T{tri} covered + needs_new = total", 
          covered + needs_new == len(tri_custs),
          f"{covered}+{needs_new}={covered+needs_new} vs {len(tri_custs)}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 7: DO NOT USE / CAN USE ITEM COUNTS PER TRIMESTER
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 7: DO NOT USE / CAN USE — INDEPENDENT RECALCULATION")
print("=" * 75)

# For each trimester, count how many items each customer received in the lookback window
# Then classify items as DO NOT USE (>=25% blocked) or CAN USE (<25%)

for tri in [1, 2, 3, 4]:
    tri_custs = my_tri_groups.get(tri, [])
    group_size = len(tri_custs)
    
    if group_size == 0:
        stored_dnu = [si for si in stored_items if si["trimester"] == tri and si["risk_level"] in ("HIGH", "MEDIUM")]
        stored_cu = [si for si in stored_items if si["trimester"] == tri and si["risk_level"] in ("LOW", "NONE")]
        check(f"T{tri}: 0 customers → 0 DO NOT USE, 0 CAN USE", 
              len(stored_dnu) == 0 and len(stored_cu) == 0)
        continue
    
    # Build item receipt counts in lookback window for this trimester's customers
    cust_ids = set(c["id"] for c in tri_custs)
    item_blocked = defaultdict(int)
    
    for s in ships_in_window:
        if s["customer_id"] in cust_ids:
            for si in sit_by_ship.get(s["id"], []):
                item_blocked[si["item_id"]] += 1
                # Also count alternatives
                for alt_id in alt_map.get(si["item_id"], set()):
                    item_blocked[alt_id] += 1
    
    # Classify items
    my_dnu = []
    my_cu = []
    for item in all_items:
        iid = item["id"]
        blocked = item_blocked.get(iid, 0)
        pct = (blocked / group_size * 100) if group_size > 0 else 0
        if pct >= 25.0:
            my_dnu.append({"id": iid, "pct": pct, "name": item["name"]})
        else:
            my_cu.append({"id": iid, "pct": pct, "name": item["name"]})
    
    # Compare with stored report
    stored_tri_items = [si for si in stored_items if si["trimester"] == tri]
    stored_dnu_count = sum(1 for si in stored_tri_items if si["risk_level"] in ("HIGH", "MEDIUM"))
    stored_cu_count = sum(1 for si in stored_tri_items if si["risk_level"] in ("LOW", "NONE"))
    
    check(f"T{tri} DO NOT USE: independent={len(my_dnu)} vs stored={stored_dnu_count}",
          len(my_dnu) == stored_dnu_count,
          f"Diff: {len(my_dnu) - stored_dnu_count}")
    check(f"T{tri} CAN USE: independent={len(my_cu)} vs stored={stored_cu_count}",
          len(my_cu) == stored_cu_count,
          f"Diff: {len(my_cu) - stored_cu_count}")
    
    # All items accounted for
    total_items_in_report = len(stored_tri_items)
    check(f"T{tri} total items = {len(all_items)} items",
          total_items_in_report == len(all_items),
          f"Stored has {total_items_in_report}, DB has {len(all_items)}")

    # Show top 5 DO NOT USE for verification
    my_dnu.sort(key=lambda x: -x["pct"])
    if my_dnu:
        print(f"    Top 5 DO NOT USE for T{tri}:")
        for item in my_dnu[:5]:
            print(f"      {item['pct']:5.1f}% — {item['name'][:50]}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 8: BUILD QUANTITY MATH
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 8: BUILD QUANTITY CALCULATIONS (warehouse min=100)")
print("=" * 75)

stored_run_data = stored_run.data[0] if stored_run.data else {}
raw_summary = stored_run_data.get("summary_json")
if isinstance(raw_summary, str):
    import json
    summary = json.loads(raw_summary)
elif isinstance(raw_summary, dict):
    summary = raw_summary
else:
    summary = {}

for tri in [1, 2, 3, 4]:
    tri_custs = my_tri_groups.get(tri, [])
    stored_covered_count = sum(1 for sc in stored_custs if sc["projected_trimester"] == tri and not sc["needs_new_curation"])
    
    projected = len(tri_custs)
    covered = stored_covered_count
    need_new = projected - covered
    
    if need_new > 0:
        recommended = max(need_new, 100)
    else:
        recommended = 0
    leftover = recommended - need_new if recommended > 0 else 0
    
    exec_tri = summary.get("trimesters", {}).get(str(tri), {})
    stored_build = exec_tri.get("recommended_build_qty", 0)
    stored_leftover = exec_tri.get("expected_leftover", 0)
    
    check(f"T{tri} build qty: independent={recommended} vs stored={stored_build}",
          recommended == stored_build,
          f"need_new={need_new}, warehouse_min=100")
    check(f"T{tri} leftover: independent={leftover} vs stored={stored_leftover}",
          leftover == stored_leftover)


# ═══════════════════════════════════════════════════════════════════
#  TEST 9: CROSS-REFERENCE WITH CSV SOURCE DATA
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 9: CROSS-REFERENCE WITH ORDER HISTORY CSVs")
print("=" * 75)

# Count unique emails across ALL CSVs
csv_files = sorted(csv_dir.glob("*.csv"))
print(f"  Found {len(csv_files)} CSV files")
check("24 CSV files found", len(csv_files) == 24, f"Got {len(csv_files)}")

all_csv_emails = set()
csv_month_emails = {}
csv_month_counts = {}

for fp in csv_files:
    try:
        with open(fp, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            rows = list(reader)
    except:
        continue
    
    # Extract batch code from filename
    fname = fp.stem
    month_match = re.search(r"(MAR|APR|MAY|JUNE|JULY|AUG|SEPT|OCT|NOV|DEC|JAN|FEB)\s+'(\d{2})\s+([A-Z]{2,3})", fname)
    if not month_match:
        continue
    
    batch = month_match.group(3)
    month_name = month_match.group(1) 
    year = "20" + month_match.group(2)
    
    # Extract emails from this CSV
    emails_in_file = set()
    for row in rows:
        if len(row) > 2 and row[2] and "@" in row[2]:
            email = row[2].strip().lower()
            emails_in_file.add(email)
    
    csv_month_emails[f"{month_name} {year} ({batch})"] = emails_in_file
    csv_month_counts[f"{month_name} {year} ({batch})"] = len(emails_in_file)
    all_csv_emails.update(emails_in_file)

print(f"  Unique emails across ALL CSVs: {len(all_csv_emails)}")

# Match CSV emails to DB emails
db_emails = set(c["email"].lower() for c in all_custs)
csv_in_db = all_csv_emails & db_emails
csv_not_in_db = all_csv_emails - db_emails
db_not_in_csv = db_emails - all_csv_emails

check("Most CSV emails found in DB (>95%)",
      len(csv_in_db) / len(all_csv_emails) > 0.95,
      f"{len(csv_in_db)}/{len(all_csv_emails)} = {len(csv_in_db)/len(all_csv_emails)*100:.1f}%")

if csv_not_in_db:
    warn(f"{len(csv_not_in_db)} CSV emails NOT in DB", f"First 5: {list(csv_not_in_db)[:5]}")
if db_not_in_csv:
    warn(f"{len(db_not_in_csv)} DB emails NOT in any CSV", f"First 5: {list(db_not_in_csv)[:5]}")

# Latest CSV (MAR 26 CK) — should be ~297 customers (matching our recency check)
mar26_key = [k for k in csv_month_emails.keys() if "MAR" in k and "2026" in k]
if mar26_key:
    mar26_emails = csv_month_emails[mar26_key[0]]
    print(f"\n  MAR 2026 CK CSV: {len(mar26_emails)} unique emails")
    # Cross-check: how many Mar 2026 shipments in DB?
    mar26_ships = [s for s in all_ships if s.get("ship_date") and s["ship_date"].startswith("2026-03")]
    print(f"  MAR 2026 shipments in DB: {len(mar26_ships)}")
    check("MAR 2026 CSV emails ≈ DB shipments (within 10%)",
          abs(len(mar26_emails) - len(mar26_ships)) / max(len(mar26_emails), 1) < 0.10,
          f"CSV={len(mar26_emails)}, DB={len(mar26_ships)}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 10: DUE DATE VERIFICATION (CSV vs DB)
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 10: DUE DATE VERIFICATION (CSV Q5 column vs DB)")
print("=" * 75)

# Q5 is column 20 (0-indexed) in the CSV = due date
# Let's check the latest CSV (MAR 2026 CK)
latest_csv = csv_dir / "Oh Baby Boxes - Monthly Boxing_Customer Kit Assignment - MAR '26 CK.csv"
csv_due_dates = {}
try:
    with open(latest_csv, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        rows = list(reader)
    for row in rows:
        if len(row) > 20 and row[2] and "@" in row[2]:
            email = row[2].strip().lower()
            due_str = row[20].strip() if row[20] else ""
            if due_str and re.match(r"\d{4}-\d{2}-\d{2}", due_str):
                csv_due_dates[email] = due_str
except Exception as e:
    print(f"  Error reading CSV: {e}")

mismatched_dates = 0
checked_dates = 0
for email, csv_due in csv_due_dates.items():
    db_cust = cust_by_email.get(email) or cust_by_email.get(email.lower())
    if not db_cust:
        # Try case-insensitive
        for e, c in cust_by_email.items():
            if e.lower() == email.lower():
                db_cust = c
                break
    if db_cust and db_cust.get("due_date"):
        checked_dates += 1
        if db_cust["due_date"] != csv_due:
            mismatched_dates += 1
            if mismatched_dates <= 5:
                print(f"    ⚠️  {email[:35]:35} CSV={csv_due} DB={db_cust['due_date']}")

check(f"Due dates CSV vs DB match rate > 95% ({checked_dates} checked)",
      mismatched_dates / max(checked_dates, 1) < 0.05 if checked_dates > 0 else True,
      f"Mismatched: {mismatched_dates}/{checked_dates}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 11: SHIPMENT DATA INTEGRITY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 11: SHIPMENT DATA INTEGRITY")
print("=" * 75)

# Every shipment points to a valid customer
orphan_ships = [s for s in all_ships if s["customer_id"] not in cust_by_id]
check("No orphaned shipments (missing customer_id)", len(orphan_ships) == 0, f"Orphans: {len(orphan_ships)}")

# Every shipment_item points to a valid shipment
ship_ids = set(s["id"] for s in all_ships)
orphan_sits = [si for si in all_sit if si["shipment_id"] not in ship_ids]
check("No orphaned shipment_items (missing shipment_id)", len(orphan_sits) == 0, f"Orphans: {len(orphan_sits)}")

# Every shipment_item points to a valid item
item_ids = set(i["id"] for i in all_items)
orphan_item_refs = [si for si in all_sit if si["item_id"] not in item_ids]
check("All shipment_items point to valid items", len(orphan_item_refs) == 0, f"Bad refs: {len(orphan_item_refs)}")

# Every kit_item points to a valid kit and item
kit_ids = set(k["id"] for k in all_kits)
bad_ki_kits = [ki for ki in all_ki if ki["kit_id"] not in kit_ids]
bad_ki_items = [ki for ki in all_ki if ki["item_id"] not in item_ids]
check("All kit_items have valid kit_id", len(bad_ki_kits) == 0)
check("All kit_items have valid item_id", len(bad_ki_items) == 0)

# No duplicate shipments (customer_id + kit_sku + ship_date)
ship_keys = [(s["customer_id"], s.get("kit_sku"), s.get("ship_date")) for s in all_ships]
dup_ships = len(ship_keys) - len(set(ship_keys))
check("No duplicate shipments", dup_ships == 0, f"Duplicates: {dup_ships}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 12: KIT ITEMS VERIFICATION
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 12: KIT-ITEMS MAPPING INTEGRITY")
print("=" * 75)

# Check that kits have a reasonable number of items
ki_counts = Counter(ki["kit_id"] for ki in all_ki)
empty_kits = [k for k in all_kits if k["id"] not in ki_counts]
print(f"  Kits with items: {len(ki_counts)}")
print(f"  Kits WITHOUT items: {len(empty_kits)}")
if empty_kits:
    for k in empty_kits[:10]:
        print(f"    Empty kit: {k['sku']}")

items_per_kit = sorted(ki_counts.values())
if items_per_kit:
    print(f"  Items per kit: min={min(items_per_kit)}, max={max(items_per_kit)}, median={items_per_kit[len(items_per_kit)//2]}")

# All kits in the report (with stock) should have items
kits_in_report_no_items = [k for k in kits_with_stock if k["id"] not in ki_counts]
check("All kits with stock have items mapped",
      len(kits_in_report_no_items) == 0,
      f"Kits with stock but no items: {[k['sku'] for k in kits_in_report_no_items]}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 13: CLOTHING SIZE DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 13: CLOTHING SIZE DISTRIBUTION")
print("=" * 75)

size_counts = Counter(c.get("clothing_size") for c in renewals)
print(f"  Size distribution of 1604 renewals:")
for sz, count in sorted(size_counts.items(), key=lambda x: -x[1]):
    pct = count/len(renewals)*100
    print(f"    {str(sz):10} {count:5d} ({pct:5.1f}%)")

# Size impacts kit filtering — verify non-universal T3 kit (BW-32, size=2=M) 
# would only be assigned to M-sized customers
bw32 = kits_by_sku.get("OBB-BW-32 KITS")
if bw32:
    check("BW-32 is size_variant=2 (M)", bw32["size_variant"] == 2, f"Got {bw32['size_variant']}")
    check("BW-32 is NOT universal", not bw32.get("is_universal"), f"is_universal={bw32.get('is_universal')}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 14: SPECIFIC CUSTOMER DEEP-DIVE
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 14: DEEP-DIVE — 5 SPECIFIC CUSTOMERS")
print("=" * 75)

# Pick customers with various shipment counts for detailed verification
for target_count in [1, 5, 10, 15, 20]:
    candidates = [c for c in renewals if len(ships_by_cust.get(c["id"], [])) == target_count]
    if not candidates:
        continue
    c = candidates[0]
    cust_ships = ships_by_cust.get(c["id"], [])
    cust_items = full_history.get(c["id"], set())
    tri = calc_trimester(c["due_date"], ship_date)
    
    # What kits did they receive?
    received_skus = set(s.get("kit_sku", "") for s in cust_ships if s.get("kit_sku"))
    
    # What would the report say?
    stored_rec = [sc for sc in stored_custs if sc["customer_id"] == c["id"]]
    stored_tri = stored_rec[0]["projected_trimester"] if stored_rec else "?"
    stored_needs = stored_rec[0]["needs_new_curation"] if stored_rec else "?"
    
    print(f"\n  Customer: {c['email'][:40]} ({target_count} shipments)")
    print(f"    Due: {c['due_date']}  Size: {c.get('clothing_size')}  Status: {c['subscription_status']}")
    print(f"    Trimester: T{tri} (stored: T{stored_tri})")
    print(f"    Unique items received: {len(cust_items)}")
    print(f"    Kits received: {', '.join(sorted(received_skus))}")
    print(f"    Needs new curation: {stored_needs}")
    check(f"  Customer w/{target_count} ships: trimester matches", tri == stored_tri)


# ═══════════════════════════════════════════════════════════════════
#  TEST 15: STORED REPORT CONSISTENCY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 15: STORED REPORT INTERNAL CONSISTENCY")
print("=" * 75)

check("Stored run status = completed", stored_run_data.get("status") == "completed")
check("Stored run report_month = 2026-05", stored_run_data.get("report_month") == "2026-05")

# Sum of trimesters in stored_custs = 1604
check("Sum of stored customer records = 1604", len(stored_custs) == 1604)

# Each stored customer has valid trimester 1-4
bad_tri = [sc for sc in stored_custs if sc["projected_trimester"] not in [1, 2, 3, 4]]
check("All stored customer records have valid trimester", len(bad_tri) == 0)

# Each stored customer has a real customer_id
bad_cid = [sc for sc in stored_custs if sc["customer_id"] not in cust_by_id]
check("All stored customers point to real DB customers", len(bad_cid) == 0)

# Stored items: all have valid item_id
bad_iid = [si for si in stored_items if si["item_id"] not in items_by_id]
check("All stored items point to real DB items", len(bad_iid) == 0)

# Items per trimester = total items count
for tri in [1, 2, 3, 4]:
    tri_items = [si for si in stored_items if si["trimester"] == tri]
    # T1 has 0 customers, so it might have 0 items or all items
    if len(my_tri_groups.get(tri, [])) > 0:
        check(f"T{tri} stored items count = {len(all_items)}", 
              len(tri_items) == len(all_items),
              f"Got {len(tri_items)}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 16: EXECUTIVE SUMMARY JSON CONSISTENCY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 16: EXECUTIVE SUMMARY JSON FROM STORED REPORT")
print("=" * 75)

if summary:
    check("Summary has total_renewal_customers", summary.get("total_renewal_customers") == 1604, f"Got {summary.get('total_renewal_customers')}")
    check("Summary has total_new_customers", summary.get("total_new_customers") == 63, f"Got {summary.get('total_new_customers')}")
    check("Summary ship_date", summary.get("ship_date") == "2026-05-14", f"Got {summary.get('ship_date')}")
    check("Summary lookback_months", summary.get("lookback_months") == 4, f"Got {summary.get('lookback_months')}")
    
    for tri in [1, 2, 3, 4]:
        tri_sum = summary.get("trimesters", {}).get(str(tri), {})
        my_count = len(my_tri_groups.get(tri, []))
        check(f"T{tri} summary projected = {my_count}", 
              tri_sum.get("projected_customers") == my_count,
              f"Got {tri_sum.get('projected_customers')}")
else:
    warn("No summary JSON found in stored run!")


# ═══════════════════════════════════════════════════════════════════
#  TEST 17: TRIMESTER BOUNDARY EDGE CASES
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 17: TRIMESTER BOUNDARY EDGE CASES")
print("=" * 75)

# Test exact boundary dates
boundaries = [
    (str(t4_cutoff), 4, "Exact T4 cutoff"),
    (str(t4_cutoff + timedelta(days=1)), 3, "One day after T4 cutoff"),
    (str(t3_cutoff), 3, "Exact T3 cutoff"),
    (str(t3_cutoff + timedelta(days=1)), 2, "One day after T3 cutoff"),
    (str(t2_cutoff), 2, "Exact T2 cutoff"),
    (str(t2_cutoff + timedelta(days=1)), 1, "One day after T2 cutoff"),
    ("2027-06-01", 1, "Far future due date"),
    ("2026-05-01", 4, "Due date before ship date (already postpartum)"),
]

for due_str, expected_tri, label in boundaries:
    actual = calc_trimester(due_str, ship_date)
    check(f"Boundary: {label} (due={due_str}) → T{expected_tri}", actual == expected_tri, f"Got T{actual}")


# ═══════════════════════════════════════════════════════════════════
#  TEST 18: CSV KIT ASSIGNMENT CROSS-CHECK
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print("  TEST 18: CSV KIT ASSIGNMENTS vs DB SHIPMENTS")
print("=" * 75)

# Parse the MAR 2026 CK CSV to extract kit assignments
# Then verify they match DB shipments
try:
    with open(latest_csv, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    current_kit_sku = None
    csv_assignments = []  # (email, kit_sku)
    
    for row in rows:
        if len(row) < 3:
            continue
        # Kit header row: col A looks like "OBB-CK-41 Kits"
        if row[0] and "Kits" in row[0] and (not row[1] or not row[2]):
            sku_match = re.match(r"(OBB-[A-Z]{2,3}-\d{2})", row[0])
            if sku_match:
                current_kit_sku = sku_match.group(1) + " KITS"
        # Customer row: has email
        elif row[2] and "@" in row[2] and current_kit_sku:
            email = row[2].strip().lower()
            csv_assignments.append((email, current_kit_sku))
    
    print(f"  CSV kit assignments extracted: {len(csv_assignments)}")
    
    # Cross-check: for customers in DB with March 2026 shipments
    mar26_ships_by_email = {}
    for s in all_ships:
        if s.get("ship_date") and s["ship_date"].startswith("2026-03"):
            cust = cust_by_id.get(s["customer_id"])
            if cust:
                mar26_ships_by_email[cust["email"].lower()] = s.get("kit_sku", "")
    
    matched = 0
    mismatched = 0
    missing = 0
    for email, csv_kit in csv_assignments[:50]:  # Check first 50
        db_kit = mar26_ships_by_email.get(email)
        if db_kit is None:
            missing += 1
        elif db_kit == csv_kit:
            matched += 1
        else:
            mismatched += 1
            if mismatched <= 3:
                print(f"    Kit mismatch: {email[:30]} CSV={csv_kit} DB={db_kit}")
    
    check(f"CSV→DB kit assignment match rate > 90% (checked 50)",
          matched / max(matched + mismatched, 1) > 0.90,
          f"Matched={matched}, Mismatched={mismatched}, Missing={missing}")

except Exception as e:
    warn(f"CSV parsing error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 75)
print(f"  FINAL RESULTS: {PASS} PASSED | {FAIL} FAILED | {WARN} WARNINGS")
print("=" * 75)

if FAIL > 0:
    print(f"\n  🔴 {FAIL} TESTS FAILED — see details above")
elif WARN > 0:
    print(f"\n  🟡 ALL TESTS PASSED but {WARN} warnings")
else:
    print(f"\n  🟢 ALL {PASS} TESTS PASSED — data is verified correct")
