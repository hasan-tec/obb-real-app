"""
Record boxes staff shipped OUTSIDE the engine (September 2026: Sheena's Shopify T3/T4 sheet,
kits CQ41 / CQ31) as shipments in the engine, so October curation sees their history.

Input: the "Monthly Boxing_Customer Kit Assignment" CSV (same layout as the August sheet, see
import_august_manual_assignments.py). Column A holds two unrelated things: section markers (a lone
row like CQ41 -> OBB-CQ-41 KITS) and that kit's item manifest, one item per row, printed down the
first customer rows of the section. The item text on a customer's row is NOT that customer's data;
it is only used to check the engine's kit has the same items. Order rows start with '#OBB-';
SHIPMENT ID is the Shopify order id (= decisions.order_id).

Per order row:
  - Ship date + tracking come from the order's Shopify fulfillment (read-only). Fallback: --ship-date.
  - Open decision for that order (pending/approved, not at VeraCore) -> kit set to the kit actually
    sent, status 'shipped', tracking stored; one shipment + the kit's items written to history.
  - Other open decisions for the SAME order -> rejected with an 'Auto-rejected:' reason
    (that prefix keeps get_rejected_kit_map from blacklisting the kit).
  - No decision for the order -> shipment history only (like Add Shipment History).
  - Held (not written, listed in the report): an order already shipped in the engine with a
    different kit, a box already at VeraCore, an email with several profiles and no decision.
  - Stops before any write if a section's column-A manifest doesn't match the engine kit's items.
Never: changes stock, pushes to VeraCore, writes to Shopify/Cratejoy, touches Google Sheets.
Idempotent: a shipment whose notes carry this import's tag + order number is never written twice.

Dry run (default):  python scripts/import_manual_shipments.py --csv "<file>"
Apply:              ... --apply            (Hasan only)
"""
import argparse
import csv
import logging
import os
import re
import sys
from collections import Counter
from difflib import SequenceMatcher
from datetime import date, datetime

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.import_manual_shipments")

TAG = "Manual Sept 2026 import"
OPEN = ("pending", "approved")


def norm_item(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def with_retry(fn, tries: int = 4):
    """Run fn(); on a network error wait and run it again. Every row is idempotent, so a retry
    after a half-done row finishes it instead of duplicating it."""
    import time
    import httpx
    from cratejoy_client import CratejoyError
    for n in range(1, tries + 1):
        try:
            return fn()
        except (httpx.TransportError, httpx.RemoteProtocolError, CratejoyError) as e:
            if n == tries:
                raise
            logger.warning("[MANUAL IMPORT] network error (%s), retry %d/%d", e, n, tries - 1)
            time.sleep(3 * n)


def kit_sku_for_code(code: str) -> str | None:
    m = re.fullmatch(r"([A-Z]{2})(\d{2})", (code or "").strip().upper())
    return f"OBB-{m.group(1)}-{m.group(2)} KITS" if m else None


def parse_sheet(path: str) -> list[dict]:
    rows, kit_code = [], None
    with open(path, encoding="utf-8-sig") as f:
        for r in list(csv.reader(f))[1:]:
            if not any(c.strip() for c in r):
                continue
            if not r[1].strip() and r[0].strip():
                kit_code = r[0].strip()
                continue
            if not r[1].strip().startswith("#OBB"):
                continue
            rows.append({
                "kit_code": kit_code, "col_a": r[0].strip(), "order_name": r[1].strip(),
                "email": r[2].strip().lower(), "ship_name": r[4].strip(), "order_id": r[13].strip(),
            })
    return rows


def section_manifests(rows: list[dict]) -> dict[str, list[str]]:
    """kit_code -> the item names printed in column A of that section (normalised)."""
    out: dict[str, list[str]] = {}
    for r in rows:
        if r["col_a"]:
            out.setdefault(r["kit_code"], []).append(norm_item(r["col_a"]))
    return out


def manifest_mismatches(manifest: list[str], kit_item_names: list[str]) -> list[str]:
    """Manifest entries that match none of the kit's item names (punctuation ignored, >=90% similar
    counts as the same item, e.g. '80 HOUR' vs '80 Hourly')."""
    names = [norm_item(n) for n in kit_item_names]
    return [m for m in manifest
            if not any(m == n or m in n or n in m or SequenceMatcher(None, m, n).ratio() >= 0.9 for n in names)]


def history_item_names(db, customer_id: str, before: str, kit_item_ids: list, names: dict) -> list[str]:
    """Kit items this customer already received in a shipment dated before `before` (read-only)."""
    sids = [x["id"] for x in db.table("shipments").select("id").eq("customer_id", customer_id)
            .lt("ship_date", before).execute().data or []]
    seen = set()
    for i in range(0, len(sids), 100):
        seen |= {x["item_id"] for x in db.table("shipment_items").select("item_id")
                 .in_("shipment_id", sids[i:i + 100]).execute().data or []}
    return [names[i] for i in kit_item_ids if i in seen]


def manual_reason(order_name: str, kit_sku: str, engine_kit: str | None, engine_reason: str, dup_items: list) -> str:
    """Decision reason that says plainly: staff shipped this by hand, and what the engine had decided."""
    if not engine_kit:
        why = (engine_reason or "").replace("[Bulk-re-curated] ", "").strip()
        head = f"engine did not assign a kit ({why[:140]})" if why else "engine did not assign a kit"
    elif engine_kit != kit_sku:
        head = f"engine assigned {engine_kit}"
    else:
        head = f"engine assigned {engine_kit} (same kit)"
    dup = f"; {len(dup_items)} item(s) already received before: {', '.join(dup_items)}" if dup_items else ""
    return (f"[Manually processed] Staff shipped {kit_sku} by hand outside the engine, {head}{dup}. "
            f"Recorded from the September sheet ({order_name}).")[:1000]


def shopify_fulfillment(sc, order_id: str) -> dict:
    """{'date': 'YYYY-MM-DD'|None, 'tracking': str|None, 'company': str|None} — read-only."""
    q = ("query($id:ID!){order(id:$id){name displayFulfillmentStatus "
         "fulfillments(first:5){createdAt status trackingInfo{number company}}}}")
    order = sc._graphql(q, {"id": sc._to_order_gid(order_id)}).get("order") or {}
    fuls = [f for f in (order.get("fulfillments") or []) if (f.get("status") or "").upper() != "CANCELLED"]
    if not fuls:
        return {"date": None, "tracking": None, "company": None, "status": order.get("displayFulfillmentStatus")}
    f = sorted(fuls, key=lambda x: x.get("createdAt") or "")[-1]
    ti = (f.get("trackingInfo") or [{}])
    ti = ti[0] if ti else {}
    return {"date": (f.get("createdAt") or "")[:10] or None, "tracking": ti.get("number"),
            "company": ti.get("company"), "status": order.get("displayFulfillmentStatus")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--ship-date", help="fallback YYYY-MM-DD when Shopify has no fulfillment date")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--report", default=f"manual_shipments_report_{date.today().isoformat()}.csv")
    args = ap.parse_args()

    db, sc = app.get_supabase(), app.get_shopify_client()
    rows = parse_sheet(args.csv)
    print(f"\n{'APPLY' if args.apply else 'DRY RUN'}: {len(rows)} order rows from {os.path.basename(args.csv)}")

    kits = {}
    for code in sorted({r["kit_code"] for r in rows}):
        sku = kit_sku_for_code(code)
        k = db.table("kits").select("id, sku, trimester").eq("sku", sku).execute().data if sku else []
        if not k:
            print(f"  STOP: kit code {code!r} -> {sku!r} is not in the Kits page. Staff must add it first.")
            return 2
        ki = db.table("kit_items").select("item_id, items(name)").eq("kit_id", k[0]["id"]).execute().data or []
        items = [x["item_id"] for x in ki]
        if not items:
            print(f"  STOP: kit {sku} has no items. Staff must add its items on the Kits page first.")
            return 2
        manifest = section_manifests(rows).get(code, [])
        bad = manifest_mismatches(manifest, [(x.get("items") or {}).get("name") or "" for x in ki])
        if bad:
            print(f"  STOP: sheet's {code} item list doesn't match engine kit {sku}: {bad}")
            return 2
        kits[code] = {**k[0], "items": items,
                      "names": {x["item_id"]: (x.get("items") or {}).get("name") or x["item_id"] for x in ki}}
        print(f"  kit {code} -> {sku} (T{k[0]['trimester']}, {len(items)} items; sheet manifest "
              f"{len(manifest)} items, all match)")

    report, outcome = [], Counter()

    def process(r):
        kit = kits[r["kit_code"]]
        rec = {**r, "kit_sku": kit["sku"], "decision_id": "", "customer_id": "", "ship_date": "",
               "tracking": "", "dup_items": "", "outcome": "", "note": ""}
        report.append(rec)

        decs = (db.table("decisions")
                .select("id, status, kit_sku, customer_id, trimester, veracore_order_id, created_at, reason")
                .eq("order_id", r["order_id"]).order("created_at").execute().data or [])
        open_decs = [d for d in decs if d["status"] in OPEN]
        shipped = [d for d in decs if d["status"] == "shipped"]

        if decs:
            cust_id = (open_decs or shipped or decs)[-1]["customer_id"]
        else:
            profiles = db.table("customers").select("id").ilike("email", r["email"]).execute().data or []
            if len(profiles) != 1:
                rec.update(outcome="held_profile", note=f"{len(profiles)} profiles for this email and no decision")
                outcome[rec["outcome"]] += 1
                return
            cust_id = profiles[0]["id"]
        rec["customer_id"] = cust_id

        tagged = (db.table("shipments").select("id").eq("customer_id", cust_id)
                  .ilike("notes", f"%{TAG} {r['order_name']}%").execute().data)
        resume_sid = tagged[0]["id"] if tagged else None
        if resume_sid and not open_decs:
            rec.update(outcome="already_imported")
            outcome[rec["outcome"]] += 1
            return
        same_kit_sept = (db.table("shipments").select("id, notes").eq("customer_id", cust_id)
                         .eq("kit_sku", kit["sku"]).gte("ship_date", "2026-09-01").execute().data)
        if shipped and not resume_sid:
            same = [d for d in shipped if d["kit_sku"] == kit["sku"]]
            rec.update(outcome="already_recorded" if same else "held_conflict",
                       note="" if same else f"engine shipped {shipped[-1]['kit_sku']} for this order; sheet says {kit['sku']}")
            outcome[rec["outcome"]] += 1
            return
        # Shipments written by THIS import for another order (a customer with two subscriptions,
        # e.g. two CQ41 boxes) don't count — only pre-existing September history does.
        same_kit_sept = [x for x in same_kit_sept if TAG not in (x.get("notes") or "")]
        if same_kit_sept and not resume_sid:
            rec.update(outcome="already_recorded", note=f"September {kit['sku']} shipment already on the profile")
            outcome[rec["outcome"]] += 1
            return
        if any(d.get("veracore_order_id") for d in open_decs):
            rec.update(outcome="held_veracore", note="an open decision for this order is already at VeraCore")
            outcome[rec["outcome"]] += 1
            return

        try:
            ful = shopify_fulfillment(sc, r["order_id"])
        except Exception as e:  # noqa: BLE001 — report and hold, never guess
            rec.update(outcome="held_shopify_error", note=str(e)[:120])
            outcome[rec["outcome"]] += 1
            return
        ship_date = ful["date"] or args.ship_date
        if not ship_date:
            rec.update(outcome="held_no_date", note=f"Shopify status {ful.get('status')}, no fulfillment; pass --ship-date")
            outcome[rec["outcome"]] += 1
            return
        rec.update(ship_date=ship_date, tracking=ful["tracking"] or "")

        target = open_decs[-1] if open_decs else None
        extras = open_decs[:-1]
        rec["decision_id"] = target["id"] if target else ""
        notes = [f"kit changed {target['kit_sku'] or 'none'} -> {kit['sku']}"] if target and target["kit_sku"] != kit["sku"] else []
        if target and target.get("trimester") and int(target["trimester"]) != int(kit["trimester"]):
            notes.append(f"engine says T{target['trimester']}, kit is T{kit['trimester']}")
        if extras:
            notes.append(f"{len(extras)} duplicate open decision(s) auto-rejected")
        if not target:
            notes.append("no decision: history only")
        dup_items = history_item_names(db, cust_id, "2026-09-01", kit["items"], kit["names"])
        rec["dup_items"] = len(dup_items)
        if dup_items:
            notes.append(f"{len(dup_items)} duplicate item(s)")
        reason = manual_reason(r["order_name"], kit["sku"], (target or {}).get("kit_sku"),
                               (target or {}).get("reason") or "", dup_items)
        rec.update(outcome="write", note="; ".join(notes))
        outcome["write"] += 1
        if not args.apply:
            return

        # ── writes: history first, decision last, so a crash can never leave a shipped decision
        #    without history (a re-run finds the tagged shipment and finishes the row) ──
        sid = resume_sid
        if not sid:
            ship = db.table("shipments").insert({
                "customer_id": cust_id, "kit_id": kit["id"], "kit_sku": kit["sku"], "ship_date": ship_date,
                "trimester_at_ship": kit["trimester"], "platform": "shopify",
                "order_id": r["order_id"],
                "notes": f"{TAG} {r['order_name']}: shipped by hand by staff"
                         + (f" (decision {target['id'][:8]})" if target else "")
                         + (f"; {len(dup_items)} duplicate item(s)" if dup_items else ""),
            }).execute().data
            sid = ship[0]["id"] if ship else None
        if sid and not db.table("shipment_items").select("item_id").eq("shipment_id", sid).limit(1).execute().data:
            db.table("shipment_items").insert([{"shipment_id": sid, "item_id": i} for i in kit["items"]]).execute()
        for d in extras:
            db.table("decisions").update({
                "status": "rejected",
                "reason": f"Auto-rejected: duplicate of {r['order_name']}, shipped by hand ({TAG}).",
            }).eq("id", d["id"]).eq("status", d["status"]).execute()
        if target:
            upd = {"status": "shipped", "kit_id": kit["id"], "kit_sku": kit["sku"], "reason": reason}
            if ful["tracking"]:
                upd.update(tracking_number=ful["tracking"], tracking_pushed_at=datetime.utcnow().isoformat())
            db.table("decisions").update(upd).eq("id", target["id"]).in_("status", list(OPEN)).execute()
        logger.warning("[MANUAL IMPORT] %s %s -> shipment %s decision %s", r["order_name"], kit["sku"], sid,
                       (target or {}).get("id"))

    for r in rows:
        before = (len(report), outcome.copy())

        def attempt(r=r, before=before):
            del report[before[0]:]           # a retried row starts clean in the report/counts
            outcome.clear()
            outcome.update(before[1])
            process(r)
        with_retry(attempt)

    with open(args.report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(report[0].keys()))
        w.writeheader()
        w.writerows(report)
    print("\n  Outcome:", dict(outcome))
    print(f"  Report: {os.path.abspath(args.report)}")
    if args.apply:
        import asyncio
        asyncio.run(app.log_activity("shipment", f"{TAG}: {outcome['write']} boxes recorded",
                                     f"file={os.path.basename(args.csv)} outcome={dict(outcome)}", "success"))
    else:
        print("  (dry run — nothing written; re-run with --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
