"""
Threads 20/21 — D2: split ONE purchaser account that has two (or more) recipients receiving
boxes at the same time into one customer profile per recipient (migration 022 must be applied).

Who needs it (found 2026-09-23): jessicamariehawley@gmail.com, miss_tey20@hotmail.com,
klpeters2007@yahoo.com, lina.weston5555@gmail.com, theresalvaros@gmail.com, leaannkirby@aol.com
(Shopify). bednarfive / reneedkelly (Cratejoy) do NOT need it — their second recipient never got a
decision, so the daily sync creates her profile by itself.
`scripts/backfill_recipient_refs.py` prints the current list ("skipped, multi-recipient").

What it does, per account:
  1. groups the account's decisions by recipient (normalized ship-to name);
  2. the recipient with the OLDEST engine decision keeps the existing profile;
  3. every other recipient gets a new profile (same email) — name, recipient_ref
     ('rc:<Recharge id>' from their newest order, else 'name:<name>'), quiz answers and
     address from that recipient's newest order payload;
  4. moves that recipient's decisions, and the shipments tied to them (same order id, or
     notes "… decision <id8>"), to the new profile. shipment_items follow their shipment;
  5. prints every shipment it could NOT attribute (imported history with no order link) —
     those stay on the kept profile; send the list to Sheena to confirm.
Recipients whose due date / size can't be found are created with them EMPTY and flagged
"NEEDS DATA" — staff fill them in on the profile, then Recurate.

Dry run (default, writes nothing):
    python scripts/split_recipient_profiles.py --email jessicamariehawley@gmail.com
Apply (one account at a time, after reviewing the dry run):
    python scripts/split_recipient_profiles.py --email jessicamariehawley@gmail.com --apply
Staff-confirmed due dates for recipients whose orders carry none:
    python scripts/split_recipient_profiles.py --email theresalvaros@gmail.com         --set-due "Emma Coakley=2026-09-30" --set-due "Isabelle Cosgrove=2026-12-04" [--apply]
"""
import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to cp1252
except Exception:
    pass
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.split_recipient_profiles")


def order_payload(db, order_id: str, near_iso: str):
    """Newest orders/create|orders/updated payload for a Shopify order, searched in a window
    around the decision's creation (JSON filters on webhook_logs time out)."""
    if not order_id or not near_iso:
        return None
    t = datetime.fromisoformat(near_iso.replace("Z", "+00:00"))
    for pad in (timedelta(minutes=15), timedelta(days=3)):
        rows = (db.table("webhook_logs").select("id, created_at, pid:payload->>id")
                .eq("source", "shopify").in_("event_type", ["orders/create", "orders/updated"])
                .gte("created_at", (t - pad).isoformat()).lte("created_at", (t + pad).isoformat())
                .order("created_at", desc=True).limit(1000).execute().data or [])
        hit = next((r for r in rows if str(r.get("pid")) == str(order_id)), None)
        if hit:
            return db.table("webhook_logs").select("payload").eq("id", hit["id"]).limit(1).execute().data[0]["payload"]
    return None


def profile_fields_from_payload(payload: dict) -> dict:
    """Quiz + shipping address of one recipient, from their Shopify order payload."""
    quiz = app.extract_quiz_data(payload.get("note_attributes", []) or [], payload.get("line_items", []) or [])
    ship = payload.get("shipping_address") or {}
    out = {
        "address_line1": ship.get("address1") or None, "address_line2": ship.get("address2") or None,
        "city": ship.get("city") or None, "province": ship.get("province") or None,
        "zip": ship.get("zip") or None, "country": ship.get("country_code") or "US",
    }
    due = app.parse_due_date(quiz.get("due_date_str"))
    if due:
        out["due_date"] = due.isoformat()
        out["trimester"] = app.calculate_trimester(due, date.today())
    if quiz.get("clothing_size"):
        out["clothing_size"] = quiz["clothing_size"]
    if quiz.get("baby_gender"):
        out["baby_gender"] = quiz["baby_gender"]
    out["wants_daddy_item"] = bool(quiz.get("wants_daddy"))
    return {k: v for k, v in out.items() if v is not None}, app.recipient_ref_for_shopify(quiz)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--set-due", action="append", default=[], metavar="'Recipient Name=YYYY-MM-DD'",
                    help="due date for a recipient whose orders carry none (confirmed by staff); repeatable")
    args = ap.parse_args()
    set_due = {}
    for item in args.set_due:
        name, _, iso = item.partition("=")
        set_due[app.norm_name(name)] = date.fromisoformat(iso.strip())
    email = args.email.strip().lower()
    db = app.get_supabase()
    mode = "APPLY" if args.apply else "DRY RUN"

    profiles = app.load_email_profiles(db, email)
    if not profiles:
        print(f"No customer for {email}")
        return 1
    if len(profiles) > 1:
        print(f"{email} already has {len(profiles)} profiles — already split, nothing to do.")
        return 0
    base = profiles[0]
    decisions = (db.table("decisions").select("*").eq("customer_id", base["id"])
                 .order("created_at").execute().data or [])
    shipments = db.table("shipments").select("id, ship_date, kit_sku, order_id, notes") \
        .eq("customer_id", base["id"]).order("ship_date").execute().data or []

    groups = defaultdict(list)
    for d in decisions:
        if d.get("ship_first_name"):
            groups[app.norm_name(f"{d['ship_first_name']} {d.get('ship_last_name') or ''}")].append(d)
    if len(groups) < 2:
        print(f"{email}: only {len(groups)} recipient on its decisions — nothing to split.")
        return 0

    ordered = sorted(groups.items(), key=lambda kv: kv[1][0]["created_at"])  # oldest engine history first
    keep_key = ordered[0][0]
    print(f"\n══════ {mode}: split {email} (profile {base['id']}) into {len(ordered)} recipient profiles ══════")
    plan = []
    for key, ds in ordered:
        latest = ds[-1]
        display = " ".join(f"{latest['ship_first_name']} {latest.get('ship_last_name') or ''}".split())
        fields, ref, order_names = {}, None, set()
        for d in reversed(ds):  # newest first: quiz/address from the newest payload found
            if d.get("platform") == "shopify" and d.get("order_id"):
                payload = order_payload(db, d["order_id"], d["created_at"])
                if payload:
                    if payload.get("name"):  # shipments store the order NAME (#OBB-13855)
                        order_names.add(str(payload["name"]).lstrip("#").upper())
                    if not fields:
                        fields, ref = profile_fields_from_payload(payload)
        if not ref:
            ref = f"name:{key}"
        if key in set_due:  # staff-confirmed due date (orders carried none)
            fields["due_date"] = set_due[key].isoformat()
            fields["trimester"] = app.calculate_trimester(set_due[key], date.today())
        needs = [k for k in ("due_date", "clothing_size") if k not in fields]
        plan.append({"key": key, "name": display, "ref": ref, "fields": fields, "decisions": ds,
                     "keep": key == keep_key, "needs": needs, "order_names": order_names})
        print(f"\n  {'KEEP  ' if key == keep_key else 'NEW   '} {display!r:28} ref={ref}")
        print(f"         quiz/address: {fields or '(none found)'}")
        if needs:
            print(f"         ⚠ NEEDS DATA: {needs} — fill in on the profile after the split, then Recurate")
        for d in ds:
            print(f"         decision {d['id'][:8]} {d['created_at'][:10]} {d['status']:9} order={d.get('order_id')} kit={d.get('kit_sku')}")

    moved_ship_ids = set()
    for p in plan:
        if p["keep"]:
            continue
        ids8 = {d["id"][:8] for d in p["decisions"]}
        oids = {str(d["order_id"]) for d in p["decisions"] if d.get("order_id")}
        p["shipments"] = [s for s in shipments
                          if (s.get("order_id") and str(s["order_id"]) in oids)
                          or (s.get("order_id") and str(s["order_id"]).lstrip("#").upper() in p["order_names"])
                          or any(f"decision {i}" in (s.get("notes") or "") for i in ids8)]
        moved_ship_ids |= {s["id"] for s in p["shipments"]}
        for s in p["shipments"]:
            print(f"         → moves shipment {s['id'][:8]} {s['ship_date']} {s['kit_sku']} ({(s.get('notes') or '')[:40]})")
    unattributed = [s for s in shipments if s["id"] not in moved_ship_ids]
    print(f"\n  Shipments staying on the kept profile ({plan[0]['name']}): {len(unattributed)}")
    for s in unattributed:
        print(f"         {s['ship_date']} {s['kit_sku']} order={s.get('order_id')} {(s.get('notes') or '')[:50]}")
    print("  ⚠ Send this list to Sheena: any of these that were really for another recipient must be moved by hand.")

    if not args.apply:
        print("\n(dry run — nothing written; re-run with --apply)")
        return 0

    for p in plan:
        if p["keep"]:
            upd = {"recipient_ref": p["ref"], "recipient_name": p["name"], **p["fields"]}
            db.table("customers").update(upd).eq("id", base["id"]).execute()
            logger.info("[D2] kept profile %s updated: %s", base["id"], upd)
            continue
        first, _, last = p["name"].partition(" ")
        rec = {
            "email": base["email"], "first_name": first or None, "last_name": last or None,
            "recipient_ref": p["ref"], "recipient_name": p["name"],
            "platform": base.get("platform"), "subscription_status": base.get("subscription_status") or "active",
            "shopify_customer_id": base.get("shopify_customer_id"),
            "cratejoy_customer_id": base.get("cratejoy_customer_id"),
            "history_pending": False, "phone": base.get("phone"), **p["fields"],
        }
        new = db.table("customers").insert(rec).execute().data[0]
        logger.info("[D2] created profile %s for %s (%s)", new["id"], p["name"], p["ref"])
        dec_ids = [d["id"] for d in p["decisions"]]
        db.table("decisions").update({"customer_id": new["id"]}).in_("id", dec_ids).execute()
        ship_ids = [s["id"] for s in p["shipments"]]
        if ship_ids:
            db.table("shipments").update({"customer_id": new["id"]}).in_("id", ship_ids).execute()
        logger.info("[D2] moved %d decision(s) and %d shipment(s) to %s", len(dec_ids), len(ship_ids), new["id"])
    print(f"\nDone. Open the profiles for {email} on the Customers page and check each one.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
