"""
OBB Phase 2 Testing Script — Comprehensive Page + Curation Report Testing
Run: python test_phase2.py
"""
import httpx
import os
import json
from collections import Counter
from datetime import date, timedelta

os.environ['SUPABASE_URL'] = 'https://tkcvvjxmzfjaesdhyfiy.supabase.co'
os.environ['SUPABASE_SERVICE_ROLE_KEY'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRrY3Z2anhtemZqYWVzZGh5Zml5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDUzOTg1MSwiZXhwIjoyMDkwMTE1ODUxfQ.3rktaVHiFesZ0RU7BUcULZ9bBfO0r6wOGJqMK60a-7Q'

from supabase import create_client

db = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_ROLE_KEY'])
BASE = 'http://localhost:8000'
client = httpx.Client(timeout=60.0)

def check(html, term, label):
    if term in html:
        print(f"    ✅ {label}")
        return True
    else:
        print(f"    ❌ {label} MISSING")
        return False

print("=" * 70)
print("  OBB PHASE 2 COMPREHENSIVE TEST — PRETENDING TO BE TING")
print("=" * 70)

# ═══ TEST 1: Dashboard ═══
print("\n🏠 TEST 1: Dashboard")
r = client.get(f"{BASE}/")
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    check(r.text, "Total Customers", "Total Customers stat card")
    check(r.text, "/curation-report", "Curation Report nav link")
    check(r.text, "/item-alternatives", "Alternatives nav link")
    check(r.text, "/customers", "Customers nav link")
    check(r.text, "/kits", "Kits nav link")
    check(r.text, "/items", "Items nav link")

# ═══ TEST 2: Customers List ═══
print("\n👥 TEST 2: Customers List")
r = client.get(f"{BASE}/customers")
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    tr_count = r.text.count("<tr")
    print(f"    ✅ Table rows: ~{tr_count}")

# ═══ TEST 3: Customer Profile (the most-shipped customer) ═══
print("\n👤 TEST 3: Customer Profile — Ting checks the longest subscriber")
ships = db.table("shipments").select("customer_id").execute()
counts = Counter(s["customer_id"] for s in ships.data)
top_cid, top_count = counts.most_common(1)[0]
cust = db.table("customers").select("*").eq("id", top_cid).single().execute()
c = cust.data
print(f"  Most-shipped customer: {c['first_name']} {c['last_name']} ({c['email']})")
print(f"  {top_count} shipments | platform={c['platform']} | status={c['subscription_status']}")
print(f"  due_date={c['due_date']} | size={c['clothing_size']}")

r = client.get(f"{BASE}/customers/{top_cid}")
print(f"  Profile page: {r.status_code}")
if r.status_code == 200:
    check(r.text, c["first_name"], "Customer name displayed")
    check(r.text, c["email"], "Email displayed")
    check(r.text, "hipment", "Shipment section")

# Calculate their projected trimester for May 2026
ship_may = date(2026, 5, 14)
if c["due_date"]:
    due = date.fromisoformat(c["due_date"])
    t4c = ship_may + timedelta(days=19)
    t3c = t4c + timedelta(weeks=13)
    t2c = t3c + timedelta(weeks=14)
    if due <= t4c: tri = 4
    elif due <= t3c: tri = 3
    elif due <= t2c: tri = 2
    else: tri = 1
    print(f"  Projected trimester for May 2026: T{tri}")

# ═══ TEST 4: Kits Page ═══
print("\n📦 TEST 4: Kits Page")
r = client.get(f"{BASE}/kits")
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    check(r.text, "quantity", "Quantity column")
    tr_count = r.text.count("<tr")
    print(f"    ✅ Kit rows: ~{tr_count}")

# ═══ TEST 5: Items Page ═══
print("\n🏷️ TEST 5: Items Page")
r = client.get(f"{BASE}/items")
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    tr_count = r.text.count("<tr")
    print(f"    ✅ Item rows: ~{tr_count}")

# ═══ TEST 6: Item Alternatives Page ═══
print("\n🔗 TEST 6: Item Alternatives Page")
r = client.get(f"{BASE}/item-alternatives")
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    check(r.text, "alternatives", "Alternatives explanation")
    check(r.text, "<select", "Item dropdown selects")

# ═══ TEST 7: Curation Report Page (generate form) ═══
print("\n📊 TEST 7: Curation Report Page — Main form")
r = client.get(f"{BASE}/curation-report")
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    check(r.text, "Generate Monthly Curation Report", "Title present")
    check(r.text, "report_month", "Month input field")
    check(r.text, "ship_day", "Ship day input")
    check(r.text, "warehouse_min", "Warehouse minimum input")
    check(r.text, "lookback_months", "Lookback months input")
    check(r.text, "include_paused", "Include paused checkbox")
    check(r.text, "Generate Report", "Submit button")
else:
    print(f"  ❌ ERROR: {r.text[:500]}")

# ═══ TEST 8: Generate the actual report! ═══
print("\n" + "=" * 70)
print("  🚀 TEST 8: GENERATING ACTUAL CURATION REPORT — May 2026")
print("=" * 70)
print("  This is the big one — simulating what Ting runs on May 1st...")
print("  Parameters: month=2026-05, ship_day=14, warehouse_min=100, lookback=4")

r = client.post(
    f"{BASE}/curation-report/generate",
    data={
        "report_month": "2026-05",
        "ship_day": "14",
        "warehouse_min": "100",
        "lookback_months": "4",
    },
    follow_redirects=False,
)
print(f"  POST status: {r.status_code}")
print(f"  Location header: {r.headers.get('location', 'none')}")

if r.status_code in (303, 302):
    redirect_url = r.headers.get("location", "")
    print(f"  Redirected to: {redirect_url}")

    # Follow the redirect
    r2 = client.get(f"{BASE}{redirect_url}")
    print(f"  Report page status: {r2.status_code}")

    if r2.status_code == 200:
        html = r2.text

        # Check executive overview
        print("\n  === Executive Overview ===")
        check(html, "2026-05", "Report month displayed")
        check(html, "Monthly Curation Report", "Report title")
        check(html, "Total Renewal", "Total renewal stat card")
        check(html, "New Customers", "New customers stat card")
        check(html, "Warehouse Min", "Warehouse min stat card")

        # Check trimester tabs
        check(html, "tri-panel-1", "T1 panel")
        check(html, "tri-panel-2", "T2 panel")
        check(html, "tri-panel-3", "T3 panel")
        check(html, "tri-panel-4", "T4 panel")

        # Check DO NOT USE / CAN USE
        check(html, "DO NOT USE", "DO NOT USE section")
        check(html, "CAN USE", "CAN USE section")
        check(html, "Build Quantity", "Build Quantity section")
        check(html, "Customer Flags", "Customer Flags section")
        check(html, "Welcome Kit Watchlist", "Welcome Kit Watchlist")

        # Check for risk badges
        check(html, "badge-red", "Red risk badges")
        check(html, "badge-green", "Green safety badges")

        # Count approximate data
        covered_count = html.count("badge-green")
        needs_count = html.count("badge-yellow")
        print(f"\n  Approximate badge counts: green={covered_count}, yellow={needs_count}")

        # Check the report has actual customer data
        if "Covered" in html and "Need New" in html:
            print("  ✅ Build quantity cards present with Covered/Need New labels")

        print("\n  === Report page length ===")
        print(f"  HTML length: {len(html):,} characters")
    else:
        print(f"  ❌ Report page error: {r2.text[:500]}")
elif r.status_code == 200:
    # Might have returned inline error
    if "error" in r.text.lower() or "Error" in r.text:
        print(f"  ❌ Error in response: {r.text[:500]}")
    else:
        print(f"  ⚠ Got 200 instead of redirect")
else:
    print(f"  ❌ Unexpected status: {r.text[:500]}")

# ═══ TEST 9: Verify report exists in database ═══
print("\n📋 TEST 9: Verify report stored in database")
runs = db.table("curation_runs").select("*").order("generated_at", desc=True).limit(1).execute()
if runs.data:
    run = runs.data[0]
    print(f"  Run ID: {run['id']}")
    print(f"  Report month: {run['report_month']}")
    print(f"  Ship date: {run['ship_date']}")
    print(f"  Status: {run['status']}")
    print(f"  Generated at: {run['generated_at']}")

    summary = run.get("summary_json", {})
    if isinstance(summary, str):
        summary = json.loads(summary)

    if summary:
        print(f"\n  === Executive Summary from DB ===")
        print(f"  Total renewal customers: {summary.get('total_renewal_customers')}")
        print(f"  Total new customers: {summary.get('total_new_customers')}")
        trimesters = summary.get("trimesters", {})
        for tri in ["1", "2", "3", "4"]:
            t = trimesters.get(tri, {})
            if t:
                print(f"  T{tri}: {t.get('projected_customers', 0)} customers | "
                      f"{t.get('covered_by_existing', 0)} covered | "
                      f"{t.get('needs_new_curation', 0)} need new | "
                      f"build {t.get('recommended_build_qty', 0)} | "
                      f"DO NOT USE: {t.get('do_not_use_count', 0)} | "
                      f"CAN USE: {t.get('can_use_count', 0)}")

    # Check customer records
    cust_count = db.table("curation_run_customers").select("id", count="exact").eq("run_id", run["id"]).execute()
    print(f"\n  Customer records in DB: {cust_count.count}")

    # Check item records
    item_count = db.table("curation_run_items").select("id", count="exact").eq("run_id", run["id"]).execute()
    print(f"  Item records in DB: {item_count.count}")

    # Run ID for the view test
    run_id = run["id"]

    # ═══ TEST 10: View the stored report ═══
    print(f"\n📊 TEST 10: View stored report via GET /curation-report/{run_id}")
    r = client.get(f"{BASE}/curation-report/{run_id}")
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        check(r.text, "2026-05", "Correct report month")
        check(r.text, "DO NOT USE", "DO NOT USE section")
        check(r.text, "CAN USE", "CAN USE section")
        print("  ✅ Stored report view loads correctly")
else:
    print("  ❌ No curation runs found in database")

# ═══ SCENARIO SUMMARY ═══
print("\n" + "=" * 70)
print("  📋 TING'S SCENARIO — MAY 2026 CURATION")
print("=" * 70)
if runs.data and summary:
    total = summary.get("total_renewal_customers", 0)
    new = summary.get("total_new_customers", 0)
    print(f"""
  It's May 1st, 2026. Ting opens the Curation Engine.
  She clicks "Curation Report" in the sidebar and sees the generate form.
  She sets: Month=May 2026, Ship Day=14, Warehouse Min=100, Lookback=4 months.
  She clicks "Generate Report" and waits...

  📊 RESULTS:
  Total renewal customers shipping: {total}
  New customers (welcome kit track): {new}
  """)
    for tri in ["1", "2", "3", "4"]:
        t = trimesters.get(tri, {})
        if t:
            cov = t.get("covered_by_existing", 0)
            need = t.get("needs_new_curation", 0)
            proj = t.get("projected_customers", 0)
            build = t.get("recommended_build_qty", 0)
            dnu = t.get("do_not_use_count", 0)
            cu = t.get("can_use_count", 0)
            pct_covered = (cov/proj*100) if proj > 0 else 0
            print(f"  T{tri}: {proj} customers")
            print(f"    → {cov} covered by existing kits ({pct_covered:.0f}%)")
            print(f"    → {need} need newly curated kits")
            print(f"    → Recommended build: {build} units")
            print(f"    → {dnu} items blocked (DO NOT USE), {cu} items safe (CAN USE)")
            print()

print("=" * 70)
print("  ✅ ALL TESTS COMPLETE")
print("=" * 70)
