"""HTTP Route Test — Forward Planner Endpoints"""
import os, time
from dotenv import load_dotenv
load_dotenv()

import httpx

BASE = "http://localhost:8000"
passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        print(f"  ❌ {name} — {detail}")

print("=" * 60)
print("  FORWARD PLANNER HTTP ROUTE TESTS")
print("=" * 60)

client = httpx.Client(timeout=120, follow_redirects=True)

# ── Test 1: GET /forward-planner ──
print("\n[1] GET /forward-planner")
try:
    r = client.get(f"{BASE}/forward-planner")
    check("Status 200", r.status_code == 200, f"got {r.status_code}")
    check("Has form", "base_month" in r.text, "missing form field")
    check("Has horizon selector", "horizon_months" in r.text)
    check("Has generate button", "Generate" in r.text)
except Exception as e:
    check("Request succeeded", False, str(e))

# ── Test 2: POST /forward-planner/generate ──
print("\n[2] POST /forward-planner/generate")
try:
    start = time.time()
    r = client.post(f"{BASE}/forward-planner/generate", data={
        "base_month": "2026-05",
        "horizon_months": 3,
        "ship_day": 14,
        "warehouse_min": 100,
        "recency_months": 3,
        "include_paused": "",
    })
    elapsed = time.time() - start
    check("Status 200", r.status_code == 200, f"got {r.status_code}")
    check("Has projection data", "2026-05" in r.text)
    check("Has trimester info", "T1" in r.text and "T2" in r.text and "T3" in r.text and "T4" in r.text)
    check("Has warnings section", "warning" in r.text.lower() or "Warning" in r.text)
    check("Has month cards", "2026-06" in r.text and "2026-07" in r.text)
    print(f"  ⏱️ Elapsed: {elapsed:.1f}s")
except Exception as e:
    check("Request succeeded", False, str(e))

# ── Test 3: POST /forward-planner/commit-items (valid) ──
print("\n[3] POST /forward-planner/commit-items (valid items)")
try:
    # Get a few real item IDs
    from supabase import create_client
    db = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    items = db.table("items").select("id").limit(3).execute()
    item_ids = [i["id"] for i in items.data]
    ids_str = ",".join(item_ids)

    r = client.post(f"{BASE}/forward-planner/commit-items", data={
        "report_month": "2026-05",
        "trimester": 3,
        "item_ids": ids_str,
    })
    check("Status 200", r.status_code == 200, f"got {r.status_code}")
    check("Success message", "Committed" in r.text or "success" in r.text.lower(), "missing success msg")

    # Verify items in DB
    committed = db.table("curation_committed_items").select("*").eq("report_month", "2026-05").eq("trimester", 3).execute()
    check("Items in DB", len(committed.data or []) == 3, f"expected 3, got {len(committed.data or [])}")
except Exception as e:
    check("Request succeeded", False, str(e))

# ── Test 4: POST /forward-planner/commit-items (invalid) ──
print("\n[4] POST /forward-planner/commit-items (empty)")
try:
    r = client.post(f"{BASE}/forward-planner/commit-items", data={
        "report_month": "2026-05",
        "trimester": 3,
        "item_ids": "",
    })
    check("Status 200", r.status_code == 200, f"got {r.status_code}")
    check("Error message", "No item IDs" in r.text or "error" in r.text.lower())
except Exception as e:
    check("Request succeeded", False, str(e))

# ── Test 5: POST /forward-planner/clear-committed ──
print("\n[5] POST /forward-planner/clear-committed")
try:
    r = client.post(f"{BASE}/forward-planner/clear-committed", data={
        "report_month": "2026-05",
    })
    check("Status 200", r.status_code == 200, f"got {r.status_code}")
    check("Cleared message", "Cleared" in r.text or "success" in r.text.lower())

    # Verify items removed from DB
    committed = db.table("curation_committed_items").select("*").eq("report_month", "2026-05").execute()
    check("DB cleared", len(committed.data or []) == 0, f"expected 0, got {len(committed.data or [])}")
except Exception as e:
    check("Request succeeded", False, str(e))

# ── Test 6: POST /forward-planner/commit-items (invalid UUIDs) ──
print("\n[6] POST /forward-planner/commit-items (fake UUID)")
try:
    r = client.post(f"{BASE}/forward-planner/commit-items", data={
        "report_month": "2026-05",
        "trimester": 2,
        "item_ids": "00000000-0000-0000-0000-000000000000",
    })
    check("Status 200", r.status_code == 200, f"got {r.status_code}")
    check("Error for invalid ID", "Invalid" in r.text or "error" in r.text.lower())
except Exception as e:
    check("Request succeeded", False, str(e))

print("\n" + "=" * 60)
print(f"  RESULTS: {passed} passed, {failed} failed")
print("=" * 60)

client.close()
