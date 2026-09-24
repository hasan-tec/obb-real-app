"""
Threads 20/21 — create the recipient profile for a SECOND Cratejoy subscription under a
purchaser email NOW, instead of waiting for that subscription's next box to reach the daily sync.

Same fields the daily sync would write (app.process_cratejoy_box, create_recipient path):
name + address from the subscription, due date / size / gender / daddy from THAT subscription's
own survey, recipient_ref 'cj:<subscription id>' so every later box of this subscription routes
here. No decision is created — the daily sync creates decisions when boxes come due.

Used for (2026-09-24): bednarfive@gmail.com → Lana Bain (7093458554),
                       reneedkelly@yahoo.com → Emmalyn Kelly (7086274853).

Dry run (default):  python scripts/add_cratejoy_recipient_profile.py --email bednarfive@gmail.com --subscription 7093458554
Apply:              ... --apply
"""
import argparse
import asyncio
import logging
import os
import sys

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

import app  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("obb.add_cratejoy_recipient_profile")


async def build_profile(email: str, sub_id: str) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"https://api.cratejoy.com/v1/subscriptions/{sub_id}/", headers=app._cj_basic_headers())
        r.raise_for_status()
        sub = r.json()
        cust = sub.get("customer") or {}
        if (cust.get("email") or "").strip().lower() != email:
            raise SystemExit(f"Subscription {sub_id} belongs to {cust.get('email')!r}, not {email!r} — refusing")
        addr = sub.get("address") or {}
        recipient = " ".join(str(addr.get("to") or "").split())
        first, _, last = recipient.partition(" ")
        rec = {
            "email": email,
            "first_name": first or None,
            "last_name": last or None,
            "cratejoy_customer_id": str(cust.get("id") or "") or None,
            "platform": "cratejoy",
            "subscription_status": "active" if (sub.get("status") or "").lower() == "active" else "cancelled-prepaid",
            "history_pending": False,
            "recipient_ref": app.recipient_ref_for_cratejoy(sub_id),
            "recipient_name": recipient or None,
        }
        rec.update(await app._cj_enrich_new_customer(client, sub_id, addr))
        return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True)
    ap.add_argument("--subscription", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    email, sub_id = args.email.strip().lower(), args.subscription.strip()
    db = app.get_supabase()

    profiles = app.ensure_profile_identity(db, app.load_email_profiles(db, email), dry_run=not args.apply)
    ref = app.recipient_ref_for_cratejoy(sub_id)
    existing = [p for p in profiles if p.get("recipient_ref") == ref]
    print(f"\n{'APPLY' if args.apply else 'DRY RUN'}: {email} subscription {sub_id}")
    for p in profiles:
        print(f"  existing profile {p['id'][:8]} recipient={p.get('recipient_name')!r} ref={p.get('recipient_ref')}")
    if existing:
        print(f"  Already has a profile for {ref} ({existing[0]['id']}) — nothing to do.")
        return 0

    rec = asyncio.run(build_profile(email, sub_id))
    print("  NEW profile:", {k: rec.get(k) for k in ("first_name", "last_name", "recipient_name", "recipient_ref",
                                                     "due_date", "trimester", "clothing_size", "baby_gender",
                                                     "address_line1", "city", "province", "zip", "subscription_status")})
    if not args.apply:
        print("  (dry run — nothing written; re-run with --apply)")
        return 0
    row = app._insert_profile(db, rec, email, rec["recipient_ref"], rec.get("recipient_name") or "", "[ADD CJ RECIPIENT]")
    logger.info("[ADD CJ RECIPIENT] created profile %s for %s (%s)", (row or {}).get("id"), rec.get("recipient_name"), ref)
    print(f"  Created profile {(row or {}).get('id')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
