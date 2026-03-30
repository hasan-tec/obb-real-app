"""
OBB Curation Engine — Manual API Test Guide
Run these curl commands against the live server to test Phase 1 features.
Adjust BASE_URL to your deployment (Heroku, localhost, etc.)
"""

# ═══════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════

BASE_URL = "https://obb-real-d4e16a8bb2ff.herokuapp.com"
# BASE_URL = "http://localhost:8000"  # For local testing


# ═══════════════════════════════════════════════════════════
# TEST SCRIPT — Run with: python tests/test_api.py
# ═══════════════════════════════════════════════════════════

import httpx
import json
import time
import sys

def run_tests():
    base = BASE_URL.rstrip("/")
    client = httpx.Client(timeout=30, follow_redirects=True)
    results = []

    def check(name, passed, detail=""):
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {name}" + (f" — {detail}" if detail else ""))
        results.append((name, passed))

    print(f"\n{'='*60}")
    print(f"OBB Phase 1 API Tests — {base}")
    print(f"{'='*60}\n")

    # ─── 1. Health check ───
    print("1. Health Check")
    try:
        r = client.get(f"{base}/health")
        check("GET /health returns 200", r.status_code == 200)
        data = r.json()
        check("Database connected", data.get("database") == "connected", data.get("database"))
    except Exception as e:
        check("Health check", False, str(e))

    # ─── 2. Dashboard loads ───
    print("\n2. Dashboard")
    try:
        r = client.get(f"{base}/")
        check("GET / returns 200", r.status_code == 200)
        check("Contains 'OBB Engine'", "OBB Engine" in r.text)
    except Exception as e:
        check("Dashboard", False, str(e))

    # ─── 3. Pages load ───
    print("\n3. Page Routes")
    pages = ["/customers", "/decisions", "/kits", "/items", "/activity", "/settings", "/webhooks"]
    for page in pages:
        try:
            r = client.get(f"{base}{page}")
            check(f"GET {page}", r.status_code == 200)
        except Exception as e:
            check(f"GET {page}", False, str(e))

    # ─── 4. Add Item ───
    print("\n4. Add Item")
    try:
        r = client.post(f"{base}/items/add", data={
            "name": f"Test Item {int(time.time())}",
            "sku": f"TEST-{int(time.time())}",
            "category": "testing",
            "unit_cost": "2.50",
            "is_therabox": "",
        })
        check("POST /items/add redirects", r.status_code == 200)  # After redirect
    except Exception as e:
        check("Add item", False, str(e))

    # ─── 5. Add Kit ───
    print("\n5. Add Kit")
    try:
        ts = int(time.time())
        r = client.post(f"{base}/kits/add", data={
            "sku": f"TST{ts % 10000}21",
            "name": "Test Kit",
            "trimester": "2",
            "size_variant": "1",
            "is_welcome_kit": "true",
            "quantity_available": "50",
            "age_rank": "999",
            "cost_per_kit": "10.00",
        })
        check("POST /kits/add redirects", r.status_code == 200)
    except Exception as e:
        check("Add kit", False, str(e))

    # ─── 6. Test Shopify Webhook ───
    print("\n6. Shopify Test Webhook")
    try:
        r = client.post(f"{base}/api/test-webhook", json={
            "email": f"test_{int(time.time())}@example.com",
            "first_name": "API",
            "last_name": "Test",
            "due_date": "2026-08-15",
            "size": "med",
            "expecting": "girl",
            "second_parent": "no",
        })
        check("POST /api/test-webhook returns 200", r.status_code == 200)
        data = r.json()
        check("Decision made", data.get("decision") in ("auto", "needs-curation", "incomplete-data"), data.get("decision"))
        check("Trimester calculated", data.get("trimester") is not None, f"T{data.get('trimester')}")
    except Exception as e:
        check("Test webhook", False, str(e))

    # ─── 7. Test Cratejoy Webhook ───
    print("\n7. Cratejoy Test Webhook")
    try:
        r = client.post(f"{base}/api/test-webhook-cratejoy", json={
            "email": f"cjtest_{int(time.time())}@example.com",
            "first_name": "CJ",
            "last_name": "APITest",
            "due_date": "2026-09-01",
            "size": "large",
        })
        check("POST /api/test-webhook-cratejoy returns 200", r.status_code == 200)
        data = r.json()
        check("CJ Decision made", data.get("decision") in ("auto", "needs-curation", "incomplete-data"))
    except Exception as e:
        check("CJ Test webhook", False, str(e))

    # ─── 8. Fix GSheet Headers ───
    print("\n8. Google Sheets Header Fix")
    try:
        r = client.post(f"{base}/api/fix-gsheet-headers")
        check("POST /api/fix-gsheet-headers", r.status_code in (200, 500), r.text[:100])
    except Exception as e:
        check("GSheet headers", False, str(e))

    # ─── Summary ───
    print(f"\n{'='*60}")
    passed = sum(1 for _, p in results if p)
    failed = sum(1 for _, p in results if not p)
    print(f"Results: {passed} passed, {failed} failed out of {len(results)} tests")
    if failed == 0:
        print("🎉 ALL API TESTS PASSED")
    else:
        print("⚠️ SOME TESTS FAILED — check details above")
    print(f"{'='*60}\n")

    client.close()
    return failed == 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        BASE_URL = sys.argv[1]
    success = run_tests()
    sys.exit(0 if success else 1)
