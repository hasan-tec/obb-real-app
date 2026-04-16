"""Test forward planner — generate a 3-month projection from May 2026."""
import os, logging, time
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://tkcvvjxmzfjaesdhyfiy.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY in environment")

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

from supabase import create_client
from projection_engine import project_forward

db = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

print("=" * 80)
print("  FORWARD PLANNER TEST — 3 Month Projection from May 2026")
print("=" * 80)

start = time.time()
result = project_forward(
    db=db,
    base_month="2026-05",
    ship_day=14,
    horizon_months=3,
    warehouse_minimum=100,
    recency_months=3,
)
elapsed = time.time() - start

print(f"\n  Completed in {elapsed:.1f} seconds")
print(f"\n  Base month: {result['base_month']}")
print(f"  Horizon: {result['horizon']} months")
print(f"  Warnings: {len(result['warnings'])}")
print(f"  Generated: {result['generated_at']}")

print("\n" + "=" * 80)
print("  PER-MONTH OVERVIEW")
print("=" * 80)

for month_str, mdata in result["months"].items():
    print(f"\n  📅 {month_str} (ship {mdata['ship_date']})")
    print(f"     Lookback: {mdata['lookback_window']}")
    print(f"     Renewals: {mdata['total_renewal']}, New: {mdata['total_new']}")
    print(f"     Committed items: {'Yes' if mdata['has_committed_items'] else 'No'}")
    print(f"     {'Tri':>3} {'Custs':>6} {'Cover':>6} {'New':>6} {'Build':>6} {'Stock':>6} {'DNU':>5} {'Safe':>5}")
    print(f"     {'-'*48}")
    for tri in [1, 2, 3, 4]:
        td = mdata["trimesters"].get(tri, {})
        if td:
            print(f"     T{tri}  {td['projected_customers']:>5} {td['covered_by_existing']:>5} {td['needs_new_curation']:>5} {td['recommended_build_qty']:>5} {td['kit_stock_remaining']:>5} {td['do_not_use_count']:>5} {td['can_use_count']:>5}")

print("\n" + "=" * 80)
print("  WARNINGS")
print("=" * 80)
if result["warnings"]:
    for w in result["warnings"]:
        icon = "🚨" if w["severity"] == "critical" else "⚠️"
        print(f"  {icon} [{w['month']}] T{w['trimester']} — {w['detail']}")
else:
    print("  No warnings")

# Verify month-to-month consistency
print("\n" + "=" * 80)
print("  CONSISTENCY CHECKS")
print("=" * 80)

months = list(result["months"].keys())
for i, m in enumerate(months):
    md = result["months"][m]
    total = sum(md["trimesters"].get(t, {}).get("projected_customers", 0) for t in [1,2,3,4])
    print(f"  {m}: Total T1-T4 = {total} (renewal pool = {md['total_renewal']})")
    if total != md['total_renewal']:
        print(f"    ❌ MISMATCH: T1-T4 sum != renewal pool!")
    else:
        print(f"    ✅ Match")

# Check trimester shifts
if len(months) >= 2:
    print(f"\n  Trimester shifts from {months[0]} → {months[1]}:")
    m1 = result["months"][months[0]]["trimesters"]
    m2 = result["months"][months[1]]["trimesters"]
    for tri in [1, 2, 3, 4]:
        c1 = m1.get(tri, {}).get("projected_customers", 0)
        c2 = m2.get(tri, {}).get("projected_customers", 0)
        diff = c2 - c1
        arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
        print(f"    T{tri}: {c1} → {c2} ({arrow}{abs(diff)})")

# Check kit stock depletion
print(f"\n  Kit stock depletion across months:")
for tri in [2, 3, 4]:
    stocks = [result["months"][m]["trimesters"].get(tri, {}).get("kit_stock_remaining", 0) for m in months]
    print(f"    T{tri}: {' → '.join(str(s) for s in stocks)}")
    if all(s >= stocks[0] for s in stocks[1:]) and stocks[0] > 0:
        print(f"      ⚠️ Stock not depleting — check depletion logic")
    else:
        print(f"      ✅ Stock depleting as expected")

print("\n" + "=" * 80)
print("  DONE")
print("=" * 80)
