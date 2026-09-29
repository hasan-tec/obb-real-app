"""
Record Cratejoy renewal boxes staff shipped OUTSIDE the engine (September 2026: Sheena's
"September CJ Renewals.xlsx") as shipments in the engine. Companion to import_manual_shipments.py
(the Shopify sheet), and it shares that script's helpers and rules.

Workbook layout: one tab per trimester (T1..T4). On each tab, column A's first cell is the kit code
(CQ31 -> OBB-CQ-31 KITS) and the cells below it are that kit's item manifest (not customer data;
used only to check the engine kit has the same items). Customer rows: B name, C email, D due date,
E size. There are NO order / subscription / shipment ids, so rows match on email, then recipient
name when an email has several profiles.

Per row (September cycle = decisions with ship_date 2026-08-25..2026-09-30):
  - Open September decision(s) -> the one tied to the September Cratejoy box (else same kit, else
    newest) is fixed to the kit actually sent and marked shipped, with a "[Manually processed]"
    reason; one shipment + kit items written. Other open September decisions -> Auto-rejected.
  - September box already recorded with this kit -> nothing written, but leftover open September
    decisions are Auto-rejected (they block October re-curate).
  - Held (listed, not written): no engine customer, the name doesn't match any profile, a different
    kit already recorded for September, a box already at VeraCore.
  - No open decision and no September history -> shipment history only.
Ship date + tracking: the September Cratejoy box, read-only (shipped_at / tracking_number).
Fallback --ship-date when that box isn't marked shipped in Cratejoy (reported).
Never: stock, VeraCore, Shopify, Cratejoy writes, Google Sheets.

Dry run (default):  python scripts/import_manual_shipments_cj.py --xlsx "<file>" --ship-date 2026-09-17
Apply:              ... --apply            (Hasan only)
"""
import argparse
import csv
import logging
import os
import sys
from collections import Counter
from datetime import datetime

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_here))
sys.path.insert(0, _here)

import openpyxl  # noqa: E402

import app  # noqa: E402
from import_manual_shipments import (  # noqa: E402
    history_item_names, kit_sku_for_code, manifest_mismatches, manual_reason, norm_item, with_retry,
)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.import_manual_shipments_cj")

TAG = "Manual Sept 2026 CJ import"
OPEN = ("pending", "approved")
CYCLE_FROM, CYCLE_TO = "2026-08-25", "2026-09-30"


def parse_workbook(path: str) -> tuple[list[dict], dict[str, list[str]]]:
    """(customer rows, {kit_code: normalised manifest})."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    rows, manifests = [], {}
    for ws in wb.worksheets:
        cells = [r for r in ws.iter_rows(values_only=True) if r and any(c is not None for c in r)]
        if not cells:
            continue
        code = str(cells[0][0] or "").strip().upper()
        if not kit_sku_for_code(code):
            raise SystemExit(f"STOP: tab {ws.title!r} doesn't start with a kit code (found {code!r})")
        manifests[code] = [norm_item(str(r[0])) for r in cells[1:] if r[0]]
        for i, r in enumerate(cells, start=1):
            if len(r) > 2 and r[2]:
                rows.append({"tab": ws.title, "row": i, "kit_code": code, "name": str(r[1] or "").strip(),
                             "email": str(r[2]).strip().lower()})
    return rows, manifests


def profile_name(p: dict) -> str:
    return p.get("recipient_name") or f"{p.get('first_name') or ''} {p.get('last_name') or ''}".strip()


def pick_profile(profiles: list[dict], name: str, same_email_rows: int) -> tuple[dict | None, str]:
    want = app.norm_name(name)
    hits = [p for p in profiles if app.norm_name(profile_name(p)) == want]
    if len(hits) == 1:
        return hits[0], ""
    if len(profiles) == 1 and same_email_rows == 1:
        return profiles[0], ("" if not want else f"sheet name {name!r} vs profile {profile_name(profiles[0])!r}")
    return None, f"{len(profiles)} profile(s) for this email, none named {name!r}"


def september_box(cj, cj_customer_id: str, decision: dict | None) -> dict | None:
    if decision and decision.get("cratejoy_shipment_id"):
        try:
            return cj.get_shipment(decision["cratejoy_shipment_id"])
        except Exception:  # noqa: BLE001
            pass
    if not cj_customer_id:
        return None
    sept = [s for s in cj.list_customer_shipments(cj_customer_id)
            if str(s.get("adjusted_ordered_at") or "")[:7] == "2026-09"]
    return sept[0] if len(sept) == 1 else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--ship-date", required=True, help="used when the September Cratejoy box isn't marked shipped")
    ap.add_argument("--skip-kits", default="", help="comma list of kit codes to hold untouched, e.g. BO11")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--report", default="manual_shipments_cj_report.csv")
    args = ap.parse_args()

    db, cj = app.get_supabase(), app.get_cratejoy_client()
    rows, manifests = parse_workbook(args.xlsx)
    print(f"\n{'APPLY' if args.apply else 'DRY RUN'}: {len(rows)} customer rows from {os.path.basename(args.xlsx)}")

    skip = {c.strip().upper() for c in args.skip_kits.split(",") if c.strip()}
    kits = {}
    for code in sorted({r["kit_code"] for r in rows} - skip):
        sku = kit_sku_for_code(code)
        k = db.table("kits").select("id, sku, trimester").eq("sku", sku).execute().data
        if not k:
            print(f"  STOP: kit {sku} is not in the Kits page. Staff must add it first.")
            return 2
        ki = db.table("kit_items").select("item_id, items(name)").eq("kit_id", k[0]["id"]).execute().data or []
        if not ki:
            print(f"  STOP: kit {sku} has no items.")
            return 2
        bad = manifest_mismatches(manifests.get(code, []), [(x.get("items") or {}).get("name") or "" for x in ki])
        if bad:
            print(f"  STOP: tab {code} item list doesn't match engine kit {sku}: {bad}")
            return 2
        kits[code] = {**k[0], "items": [x["item_id"] for x in ki],
                      "names": {x["item_id"]: (x.get("items") or {}).get("name") or x["item_id"] for x in ki}}
        print(f"  kit {code} -> {sku} (T{k[0]['trimester']}, {len(ki)} items; manifest "
              f"{len(manifests.get(code, []))} items, all match)")

    per_email = Counter(r["email"] for r in rows)
    report, outcome = [], Counter()

    def reject(ids_status: list[dict], why: str):
        for d in ids_status:
            db.table("decisions").update({"status": "rejected", "reason": f"Auto-rejected: {why} ({TAG})."}) \
              .eq("id", d["id"]).eq("status", d["status"]).execute()

    def process(r):
        if r["kit_code"] in skip:
            report.append({**r, "kit_sku": kit_sku_for_code(r["kit_code"]), "outcome": "held_skipped_kit",
                           "note": "kit tab skipped with --skip-kits (staff to confirm)"})
            outcome["held_skipped_kit"] += 1
            return
        kit = kits[r["kit_code"]]
        rec = {**r, "kit_sku": kit["sku"], "customer_id": "", "decision_id": "", "ship_date": "", "tracking": "",
               "dup_items": "", "closed_leftovers": 0, "outcome": "", "note": ""}
        report.append(rec)

        def done(o, note=""):
            rec.update(outcome=o, note=note)
            outcome[o] += 1

        profiles = db.table("customers").select("id, first_name, last_name, recipient_name, cratejoy_customer_id") \
            .ilike("email", r["email"]).execute().data or []
        if not profiles:
            return done("held_no_customer", "email not in the engine")
        prof, why = pick_profile(profiles, r["name"], per_email[r["email"]])
        if not prof:
            return done("held_profile", why)
        cid = prof["id"]
        rec["customer_id"] = cid
        notes = [why] if why else []

        decs = db.table("decisions").select("id, status, kit_sku, trimester, reason, veracore_order_id, "
                                            "cratejoy_shipment_id, order_id, created_at") \
            .eq("customer_id", cid).gte("ship_date", CYCLE_FROM).lte("ship_date", CYCLE_TO) \
            .order("created_at").execute().data or []
        open_decs = [d for d in decs if d["status"] in OPEN]
        sept = [s for s in db.table("shipments").select("id, kit_sku, ship_date, notes").eq("customer_id", cid)
                .gte("ship_date", "2026-09-01").lte("ship_date", CYCLE_TO).execute().data or []]
        mine = [s for s in sept if TAG in (s.get("notes") or "")]
        others = [s for s in sept if TAG not in (s.get("notes") or "")]

        if mine and not open_decs:
            return done("already_imported")
        if not mine and any(s["kit_sku"] == kit["sku"] for s in others):
            rec["closed_leftovers"] = len(open_decs)
            if args.apply and open_decs:
                reject(open_decs, f"September box already shipped ({kit['sku']}), leftover decision closed")
            return done("already_recorded", f"{len(open_decs)} leftover open September decision(s) closed"
                        if open_decs else "")
        if not mine and others:
            return done("held_conflict", f"engine has September {[s['kit_sku'] for s in others]}; sheet says {kit['sku']}")
        if any(d.get("veracore_order_id") for d in open_decs):
            return done("held_veracore", "an open September decision is already at VeraCore")

        cj_box = september_box(cj, prof.get("cratejoy_customer_id"), next(
            (d for d in reversed(open_decs) if d.get("cratejoy_shipment_id")), None))
        box_shipped = bool(cj_box) and (cj_box.get("status") or "").lower() == "shipped"
        tracking = (cj_box or {}).get("tracking_number") if box_shipped else None
        ship_date = str(cj_box.get("shipped_at"))[:10] if box_shipped and cj_box.get("shipped_at") else args.ship_date
        if not box_shipped:
            notes.append("Cratejoy September box not marked shipped: fallback date, no tracking")
        rec.update(ship_date=ship_date, tracking=tracking or "")

        target = None
        if open_decs:
            box_id = str((cj_box or {}).get("id") or "")
            target = (next((d for d in open_decs if box_id and str(d.get("cratejoy_shipment_id")) == box_id), None)
                      or next((d for d in reversed(open_decs) if d["kit_sku"] == kit["sku"]), None)
                      or open_decs[-1])
        extras = [d for d in open_decs if d is not target]
        rec["decision_id"] = (target or {}).get("id", "")
        rec["closed_leftovers"] = len(extras)
        if target and target["kit_sku"] != kit["sku"]:
            notes.append(f"kit changed {target['kit_sku'] or 'none'} -> {kit['sku']}")
        if extras:
            notes.append(f"{len(extras)} leftover open September decision(s) closed")
        if not target:
            notes.append("no open decision: history only")
        dup_items = history_item_names(db, cid, "2026-09-01", kit["items"], kit["names"])
        rec["dup_items"] = len(dup_items)
        if dup_items:
            notes.append(f"{len(dup_items)} duplicate item(s)")
        reason = manual_reason(f"{r['tab']} row {r['row']}", kit["sku"], (target or {}).get("kit_sku"),
                               (target or {}).get("reason") or "", dup_items).replace(
            "the September sheet", "the September CJ renewals workbook")
        done("write", "; ".join(n for n in notes if n))
        if not args.apply:
            return

        # history first, decision last (see import_manual_shipments.py)
        sid = mine[0]["id"] if mine else None
        if not sid:
            ship = db.table("shipments").insert({
                "customer_id": cid, "kit_id": kit["id"], "kit_sku": kit["sku"], "ship_date": ship_date,
                "trimester_at_ship": kit["trimester"], "platform": "cratejoy",
                "order_id": (target or {}).get("order_id"),
                "notes": f"{TAG} {r['tab']} row {r['row']} ({r['name']}): shipped by hand by staff"
                         + (f" (decision {target['id'][:8]})" if target else "")
                         + (f"; {len(dup_items)} duplicate item(s)" if dup_items else ""),
            }).execute().data
            sid = ship[0]["id"] if ship else None
        if sid and not db.table("shipment_items").select("item_id").eq("shipment_id", sid).limit(1).execute().data:
            db.table("shipment_items").insert([{"shipment_id": sid, "item_id": i} for i in kit["items"]]).execute()
        reject(extras, f"duplicate of the September box shipped by hand ({kit['sku']})")
        if target:
            upd = {"status": "shipped", "kit_id": kit["id"], "kit_sku": kit["sku"], "reason": reason}
            if cj_box and not target.get("cratejoy_shipment_id"):
                upd["cratejoy_shipment_id"] = str(cj_box["id"])
            if tracking:
                upd.update(tracking_number=tracking, tracking_pushed_at=datetime.utcnow().isoformat())
            db.table("decisions").update(upd).eq("id", target["id"]).in_("status", list(OPEN)).execute()
        logger.warning("[MANUAL CJ IMPORT] %s %s -> shipment %s decision %s", r["email"], kit["sku"], sid,
                       (target or {}).get("id"))

    for r in rows:
        before = (len(report), outcome.copy())

        def attempt(r=r, before=before):
            del report[before[0]:]
            outcome.clear()
            outcome.update(before[1])
            process(r)
        with_retry(attempt)

    with open(args.report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=max((list(x.keys()) for x in report), key=len), restval="")
        w.writeheader()
        w.writerows(report)
    print("\n  Outcome:", dict(outcome))
    print(f"  Report: {os.path.abspath(args.report)}")
    if args.apply:
        import asyncio
        asyncio.run(app.log_activity("shipment", f"{TAG}: {outcome['write']} boxes recorded",
                                     f"file={os.path.basename(args.xlsx)} outcome={dict(outcome)}", "success"))
    else:
        print("  (dry run — nothing written; re-run with --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
