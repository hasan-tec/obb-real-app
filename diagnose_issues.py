"""
diagnose_issues.py — one-shot live DB inspection for Sheena/Ting's two issues.
Read-only. Dumps the real state so we stop guessing.
"""
import os, json
from collections import Counter
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
db = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

def hr(t): print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)

# ── 1. Recent decisions: what order_id / veracore state do they carry? ──
hr("1. RECENT 25 DECISIONS — order_id, platform, statuses, veracore fields")
dec = (db.table("decisions")
       .select("id, order_id, platform, status, kit_sku, trimester, "
               "veracore_order_id, veracore_status, veracore_tracking, veracore_last_error, created_at")
       .order("created_at", desc=True).limit(25).execute().data or [])
for d in dec:
    print(f"  {d['created_at'][:19]} | id={d['id'][:8]} | order_id={d.get('order_id')!r:>18} "
          f"| {d.get('platform')!r:>10} | st={d.get('status'):>9} | vc_id={d.get('veracore_order_id')!r} "
          f"| vc_st={d.get('veracore_status')!r} | trk={d.get('veracore_tracking')!r}")
    if d.get("veracore_last_error"):
        print(f"        | vc_err: {d['veracore_last_error'][:160]}")

# ── 2. order_id shape analysis across ALL decisions ──
hr("2. order_id SHAPE across all decisions (numeric Shopify id, #OBB-, or null?)")
all_dec = []
off = 0
while True:
    b = db.table("decisions").select("order_id, platform, status, veracore_order_id").range(off, off+999).execute().data or []
    all_dec.extend(b)
    if len(b) < 1000: break
    off += 1000
def classify(oid):
    if oid is None or oid == "": return "NULL/empty"
    s = str(oid)
    if s.isdigit() and len(s) >= 10: return "numeric Shopify-id (10+ digit)"
    if s.isdigit(): return f"numeric short ({len(s)} digit)"
    if s.startswith("#OBB"): return "#OBB-xxxxx (human name)"
    if s.startswith("OBB-"): return "OBB-xxxx (fallback)"
    return "other"
print("  order_id classification:", dict(Counter(classify(d.get("order_id")) for d in all_dec)))
print("  total decisions:", len(all_dec))
print("  platform breakdown:", dict(Counter(d.get("platform") for d in all_dec)))
print("  status breakdown:", dict(Counter(d.get("status") for d in all_dec)))
print("  veracore_order_id populated:", sum(1 for d in all_dec if d.get("veracore_order_id")), "/", len(all_dec))

# ── 3. veracore_sync_log — what did we actually send + what came back? ──
hr("3. veracore_sync_log — recent 20 (request order_id + response + errors)")
try:
    logs = (db.table("veracore_sync_log").select("*").order("run_at", desc=True).limit(20).execute().data or [])
    for L in logs:
        req = L.get("request") or {}
        resp = L.get("response") or {}
        oid = req.get("order_id") if isinstance(req, dict) else None
        print(f"  {str(L.get('run_at'))[:19]} | {str(L.get('sync_type')):>13} | {str(L.get('status')):>4} "
              f"| req.order_id={oid!r} | resp_keys={list(resp.keys()) if isinstance(resp,dict) else type(resp).__name__}")
        if L.get("error"):
            print(f"        | error: {str(L['error'])[:200]}")
        if isinstance(resp, dict) and resp:
            print(f"        | resp: {json.dumps(resp)[:240]}")
    print("  sync_type counts:", dict(Counter(L.get("sync_type") for L in (db.table('veracore_sync_log').select('sync_type').limit(2000).execute().data or []))))
except Exception as e:
    print("  veracore_sync_log error:", e)

# ── 4. The exact customers from the screenshots ──
hr("4. SCREENSHOT CUSTOMERS — Sydney Jarz, Devon Lee, Tammi Collins, Tiffany Ramos, Christine Rivera")
names = [("Sydney","Jarz"),("Devon","Lee"),("Tammi","Collins"),("Tiffany","Ramos"),
         ("Christine","Rivera"),("Roberta","Adair"),("Norma","Sandlin")]
for fn, ln in names:
    cust = db.table("customers").select("id, email, platform, shopify_customer_id, country").ilike("last_name", ln).ilike("first_name", fn).execute().data or []
    for c in cust:
        print(f"  {fn} {ln}: cust={c['id'][:8]} email={c.get('email')} platform={c.get('platform')} "
              f"shopify_cust_id={c.get('shopify_customer_id')} country={c.get('country')}")
        ds = db.table("decisions").select("id, order_id, status, veracore_order_id, veracore_status, veracore_tracking, kit_sku, created_at").eq("customer_id", c["id"]).order("created_at", desc=True).limit(4).execute().data or []
        for d in ds:
            print(f"      dec {d['id'][:8]} order_id={d.get('order_id')!r} st={d.get('status')} "
                  f"vc_id={d.get('veracore_order_id')!r} vc_st={d.get('veracore_status')!r} trk={d.get('veracore_tracking')!r} kit={d.get('kit_sku')}")
    if not cust:
        print(f"  {fn} {ln}: NOT FOUND in customers")

# ── 5. shipments — what order_id + notes do they carry? ──
hr("5. RECENT 15 SHIPMENTS — order_id, ship_date, notes")
sh = db.table("shipments").select("id, order_id, kit_sku, ship_date, platform, notes, created_at").order("created_at", desc=True).limit(15).execute().data or []
for s in sh:
    print(f"  {s['created_at'][:19]} | order_id={s.get('order_id')!r:>18} | ship_date={s.get('ship_date')} "
          f"| kit={s.get('kit_sku')} | notes={s.get('notes')!r}")

# ── 6. app_settings — VeraCore freight service etc. ──
hr("6. app_settings")
try:
    for s in (db.table("app_settings").select("*").execute().data or []):
        print(f"  {s.get('key')} = {s.get('value')!r}")
except Exception as e:
    print("  app_settings error:", e)

# ── 7. kits veracore_sku mapping sanity ──
hr("7. KITS — veracore_sku mapping (offer id used in VC orders)")
kits = db.table("kits").select("sku, veracore_sku, quantity_available, trimester").order("sku").execute().data or []
print(f"  total kits: {len(kits)} | with veracore_sku: {sum(1 for k in kits if k.get('veracore_sku'))}")
for k in kits[:15]:
    print(f"  sku={str(k.get('sku')):>14} | veracore_sku={k.get('veracore_sku')!r} | qty={k.get('quantity_available')} | T{k.get('trimester')}")

print("\n\nDONE.")
