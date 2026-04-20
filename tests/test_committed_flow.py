"""Test committed items flow — commit items, verify they affect projection."""
import os, time
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
from projection_engine import project_forward

db = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

# Step 1: Clean up any committed items for 2026-05
print("=" * 60)
print("  COMMITTED ITEMS FLOW TEST")
print("=" * 60)

db.table("curation_committed_items").delete().eq("report_month", "2026-05").execute()
db.table("curation_committed_items").delete().eq("report_month", "2026-06").execute()
print("\n✅ Cleaned up existing committed items")

# Step 2: Run baseline projection WITHOUT committed items
print("\n[1] Running baseline projection (no committed items)...")
start = time.time()
baseline = project_forward(db, base_month="2026-05", ship_day=14, horizon_months=2,
                           warehouse_minimum=100, include_paused=False, recency_months=3)
t1 = time.time() - start
print(f"   Elapsed: {t1:.1f}s")

for month_str, mdata in baseline["months"].items():
    t3 = mdata["trimesters"][3]
    t4 = mdata["trimesters"][4]
    print(f"   {month_str}: T3 DNU={t3['do_not_use_count']}, CAN USE={t3['can_use_count']}, committed={t3['committed_item_count']}")
    print(f"   {month_str}: T4 DNU={t4['do_not_use_count']}, CAN USE={t4['can_use_count']}, committed={t4['committed_item_count']}")

baseline_t3_may = baseline["months"]["2026-05"]["trimesters"][3]
baseline_t3_jun = baseline["months"]["2026-06"]["trimesters"][3]

# Step 3: Commit some items for T3 May 2026
print("\n[2] Committing 5 items for 2026-05 T3...")
items = db.table("items").select("id, name").limit(5).execute()
item_ids = [i["id"] for i in items.data]
item_names = [i["name"] for i in items.data]
print(f"   Items: {item_names}")

rows = [{"report_month": "2026-05", "trimester": 3, "item_id": iid} for iid in item_ids]
db.table("curation_committed_items").upsert(rows, on_conflict="report_month,trimester,item_id").execute()
print("   ✅ Committed 5 items")

# Verify in DB
committed = db.table("curation_committed_items").select("*").eq("report_month", "2026-05").eq("trimester", 3).execute()
assert len(committed.data) == 5, f"Expected 5, got {len(committed.data)}"
print(f"   ✅ Verified {len(committed.data)} items in DB")

# Step 4: Re-run projection WITH committed items
print("\n[3] Running projection WITH committed items...")
start = time.time()
with_committed = project_forward(db, base_month="2026-05", ship_day=14, horizon_months=2,
                                  warehouse_minimum=100, include_paused=False, recency_months=3)
t2 = time.time() - start
print(f"   Elapsed: {t2:.1f}s")

for month_str, mdata in with_committed["months"].items():
    t3 = mdata["trimesters"][3]
    t4 = mdata["trimesters"][4]
    print(f"   {month_str}: T3 DNU={t3['do_not_use_count']}, CAN USE={t3['can_use_count']}, committed={t3['committed_item_count']}")
    print(f"   {month_str}: T4 DNU={t4['do_not_use_count']}, CAN USE={t4['can_use_count']}, committed={t4['committed_item_count']}")

committed_t3_may = with_committed["months"]["2026-05"]["trimesters"][3]
committed_t3_jun = with_committed["months"]["2026-06"]["trimesters"][3]

# Step 5: Verify impact
print("\n[4] VERIFICATION:")
print(f"   May T3 committed count: {baseline_t3_may['committed_item_count']} → {committed_t3_may['committed_item_count']}")
assert committed_t3_may["committed_item_count"] == 5, f"Expected 5, got {committed_t3_may['committed_item_count']}"
print("   ✅ May T3 committed count = 5")

# Committed items should propagate: cumulative_committed_count for June should include May's commitments
print(f"   Jun T3 cumulative committed: {committed_t3_jun['cumulative_committed_count']}")
if committed_t3_jun["cumulative_committed_count"] >= 5:
    print("   ✅ Committed items propagated to June")
else:
    print("   ⚠️ Committed items NOT propagated to June (may need investigation)")

# DNU should increase slightly or stay same (committed items add to blocked)
may_dnu_diff = committed_t3_may["do_not_use_count"] - baseline_t3_may["do_not_use_count"]
may_cu_diff = committed_t3_may["can_use_count"] - baseline_t3_may["can_use_count"]
print(f"   May T3 DNU change: {may_dnu_diff:+d} (committed items added to blocked)")
print(f"   May T3 CAN USE change: {may_cu_diff:+d}")

# Clean up
print("\n[5] Cleaning up committed items...")
db.table("curation_committed_items").delete().eq("report_month", "2026-05").execute()
verify = db.table("curation_committed_items").select("id").eq("report_month", "2026-05").execute()
assert len(verify.data or []) == 0, f"Cleanup failed: {len(verify.data)} remaining"
print("   ✅ Cleaned up")

print("\n" + "=" * 60)
print("  COMMITTED ITEMS FLOW TEST — DONE")
print("=" * 60)
