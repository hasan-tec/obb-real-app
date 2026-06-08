"""
veracore_sync.py — scheduled inventory + shipment sync
──────────────────────────────────────────────────────
Phase 3 — Oh Baby Boxes Curation Engine

Runs on a schedule (see app.py _monthly_report_scheduler thread):
  - Daily at 11 PM ET: pull VeraCore inventory → sync into kits table
    + raise low-stock alerts for any kit under LOW_STOCK_THRESHOLD.

All functions here are SYNCHRONOUS and SAFE to call from a background thread.
They never raise — errors are logged + written to veracore_sync_log as 'fail' rows.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

# Matches "EXP 10/2026", "EXP 9/22", "Exp. 04/2022" AND "EXP January 2026", "EXP Jan 2026".
_EXP_RE = re.compile(
    r'EXP\.?\s+(?:(\d{1,2})/(\d{2,4})|([A-Za-z]+)\s+(\d{4}))',
    re.IGNORECASE
)
_MONTH_MAP = {
    'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
    'march': 3,   'mar': 3, 'april': 4,    'apr': 4,
    'may': 5,
    'june': 6,    'jun': 6, 'july': 7,     'jul': 7,
    'august': 8,  'aug': 8, 'september': 9,'sep': 9, 'sept': 9,
    'october': 10,'oct': 10,'november': 11,'nov': 11,
    'december': 12,'dec': 12,
}

logger = logging.getLogger(__name__)


# Kits under this on-hand quantity raise a stock alert.
# Based on Ting's warehouse-minimum of 100 units/build × ~15% buffer before panic.
LOW_STOCK_THRESHOLD = 15


def normalize_sku(raw: str) -> str:
    """Normalize an OBB or VeraCore SKU for matching.
    'OBB-WK-C3 Kits' and 'WKC3' both → 'WKC3'."""
    if not raw:
        return ""
    s = raw.strip().upper()
    if s.startswith("RW-"):   s = s[3:]
    elif s.startswith("RW"):  s = s[2:]
    if s.startswith("OBB-"):  s = s[4:]
    if s.endswith(" KITS"):   s = s[:-5]
    elif s.endswith(" KIT"):  s = s[:-4]
    return s.replace("-", "").replace(" ", "")


def log_sync(db, sync_type: str, decision_id: Optional[str],
             request: Optional[dict], response: Optional[dict],
             status: str, error: Optional[str] = None) -> None:
    """
    Write a row to `veracore_sync_log`.  Never raises — audit trail must not
    itself break the calling flow.
    """
    try:
        db.table("veracore_sync_log").insert({
            "sync_type":   sync_type,
            "decision_id": decision_id,
            "request":     request,
            "response":    response,
            "status":      status,
            "error":       error,
        }).execute()
    except Exception as e:
        # If even the audit log write fails, just log to stdout. Don't throw.
        logger.error("[VERACORE SYNC] Failed to write sync log: %s (orig status=%s, error=%s)",
                     e, status, error)


def run_inventory_sync(db, vc_client) -> dict:
    """
    Pull VeraCore inventory → update kits.quantity_available → raise low-stock alerts.

    Optimised: bulk-fetches all kits + existing alerts upfront, does matching in-memory,
    and only writes rows where quantity actually changed.

    Returns: {synced: int, skipped: int, alerts_raised: int, error: str|None}
    """
    result = {"synced": 0, "skipped": 0, "unmatched": 0, "alerts_raised": 0, "error": None}
    started_at = datetime.utcnow()
    logger.info("[VERACORE SYNC] ═══ Inventory sync started at %s UTC ═══", started_at.isoformat())

    # 1. Bulk-fetch all kits and existing unresolved alerts — 2 DB calls total instead of N.
    try:
        all_kits    = db.table("kits").select("id, sku, quantity_available, veracore_sku").execute().data or []
        alerts_data = db.table("kit_stock_alerts").select("kit_id").eq("resolved", False).execute().data or []
    except Exception as e:
        err = f"bulk DB fetch failed: {e}"
        logger.error("[VERACORE SYNC] %s", err, exc_info=True)
        log_sync(db, "inventory", None, None, None, "fail", err)
        result["error"] = err
        return result

    # Build O(1) lookup dict keyed by normalized SKU — handles OBB-WK-C3 Kits ↔ WKC3 drift.
    kits_by_norm    = {normalize_sku(k.get("veracore_sku") or k.get("sku") or ""): k
                       for k in all_kits if (k.get("veracore_sku") or k.get("sku"))}
    alerted_kit_ids = {r["kit_id"] for r in alerts_data}

    # 2. Pull VeraCore inventory (1 API call).
    try:
        rows = vc_client.get_inventory()
    except Exception as e:
        err = f"get_inventory failed: {e}"
        logger.error("[VERACORE SYNC] %s", err, exc_info=True)
        log_sync(db, "inventory", None, None, None, "fail", err)
        result["error"] = err
        return result

    # 3. Process in-memory; only DB-write rows where qty actually changed.
    for row in rows:
        sku = row.get("sku")
        if not sku:
            result["skipped"] += 1
            continue

        kit = kits_by_norm.get(normalize_sku(sku))
        if not kit:
            logger.warning("[VERACORE SYNC] No matching kit for VeraCore SKU '%s' (norm='%s') — skipping",
                           sku, normalize_sku(sku))
            result["unmatched"] += 1
            continue

        new_qty = max(0, int(row.get("available_balance", 0)))
        old_qty = int(kit.get("quantity_available", 0) or 0)

        if old_qty != new_qty:
            try:
                db.table("kits").update({"quantity_available": new_qty}).eq("id", kit["id"]).execute()
                logger.info("[VERACORE SYNC] Kit %s qty: %d → %d", kit["sku"], old_qty, new_qty)
                kit["quantity_available"] = new_qty  # keep in-memory state current
            except Exception as e:
                logger.warning("[VERACORE SYNC] Update failed for kit %s: %s", kit["sku"], e)
                result["skipped"] += 1
                continue
        result["synced"] += 1

        # Alert check is in-memory — no extra DB call per kit.
        if new_qty < LOW_STOCK_THRESHOLD and kit["id"] not in alerted_kit_ids:
            try:
                db.table("kit_stock_alerts").insert({
                    "kit_id":    kit["id"],
                    "seen_qty":  new_qty,
                    "threshold": LOW_STOCK_THRESHOLD,
                    "note":      f"Auto-raised by inventory sync — VeraCore shows {new_qty} on hand",
                }).execute()
                alerted_kit_ids.add(kit["id"])  # prevent double-insert within same run
                result["alerts_raised"] += 1
                logger.warning("[VERACORE SYNC] LOW STOCK alert raised for %s (qty=%d, threshold=%d)",
                               kit["sku"], new_qty, LOW_STOCK_THRESHOLD)
            except Exception as e:
                logger.warning("[VERACORE SYNC] Failed to raise stock alert for %s: %s", kit["sku"], e)

    log_sync(db, "inventory", None,
             {"pulled_rows": len(rows)},
             {"synced": result["synced"], "skipped": result["skipped"],
              "unmatched": result["unmatched"], "alerts_raised": result["alerts_raised"]},
             "ok")
    logger.info("[VERACORE SYNC] ═══ Inventory sync done: synced=%d skipped=%d unmatched=%d alerts=%d ═══",
                result["synced"], result["skipped"], result["unmatched"], result["alerts_raised"])
    return result


def run_shipment_poll(db, vc_client, since_iso: Optional[str] = None) -> dict:
    """
    Pull shipment/tracking updates from VeraCore → update decisions with
    tracking number + flip status to 'shipped'.

    Matches VC shipments back to OBB decisions by OrderID (our `decisions.order_id`
    OR the fallback OBB-{decision_id[:8]} we set at submit time) OR by
    veracore_order_id (whichever VC echoes in its shipment payload).

    Args:
        db:        Supabase client.
        vc_client: VeraCoreClient instance.
        since_iso: ISO timestamp. Defaults to 24h ago.

    Returns: {matched: int, updated: int, unmatched: int, error: str|None}
    """
    from datetime import timedelta
    result = {"matched": 0, "updated": 0, "unmatched": 0, "error": None}
    if since_iso is None:
        since_iso = (datetime.utcnow() - timedelta(days=1)).isoformat()

    logger.info("[VERACORE POLL] ═══ Shipment poll started since=%s ═══", since_iso)

    try:
        shipments = vc_client.get_shipments(since_iso)
    except Exception as e:
        err = f"get_shipments failed: {e}"
        logger.error("[VERACORE POLL] %s", err, exc_info=True)
        log_sync(db, "shipment_poll", None, {"since": since_iso}, None, "fail", err)
        result["error"] = err
        return result

    for s in shipments:
        order_ref  = s.get("order_id") or ""
        tracking   = s.get("tracking_number") or ""
        carrier    = s.get("carrier") or ""
        shipped_at = s.get("shipped_at") or ""
        if not order_ref or not tracking:
            result["unmatched"] += 1
            continue

        # Try matching decisions by public order_id first, then by veracore_order_id.
        try:
            q = db.table("decisions").select("id, order_id, veracore_order_id, status").eq("order_id", order_ref).execute()
            rows = q.data or []
            if not rows:
                q2 = db.table("decisions").select("id, order_id, veracore_order_id, status").eq("veracore_order_id", order_ref).execute()
                rows = q2.data or []
            if not rows:
                logger.debug("[VERACORE POLL] No decision match for VC order_ref=%s", order_ref)
                result["unmatched"] += 1
                continue

            result["matched"] += len(rows)
            tracking_display = f"{carrier}: {tracking}".strip(": ") if carrier else tracking
            for d in rows:
                patch = {
                    "veracore_tracking": tracking_display,
                    "veracore_status":   "shipped",
                }
                # Only mark the decision itself as shipped if it isn't already in a later state.
                if d.get("status") in ("approved", "pending"):
                    patch["status"] = "shipped"
                db.table("decisions").update(patch).eq("id", d["id"]).execute()
                result["updated"] += 1
                logger.info("[VERACORE POLL] Updated decision %s → tracking=%s shipped_at=%s",
                            d["id"], tracking_display, shipped_at)
        except Exception as e:
            logger.warning("[VERACORE POLL] DB update failed for order_ref=%s: %s", order_ref, e)
            continue

    log_sync(db, "shipment_poll", None,
             {"since": since_iso, "shipments_pulled": len(shipments)},
             {"matched": result["matched"], "updated": result["updated"], "unmatched": result["unmatched"]},
             "ok")
    logger.info("[VERACORE POLL] ═══ Poll done: matched=%d updated=%d unmatched=%d ═══",
                result["matched"], result["updated"], result["unmatched"])
    return result


def run_expiry_sync(db, vc_client) -> dict:
    """
    Pull VeraCore GetInventory → parse expiry dates from offer Titles → update items.expiry_date.

    Optimised: bulk-fetches all items upfront, does matching in-memory,
    and only writes rows where the date actually changed.

    Returns: {updated, skipped_no_match, skipped_no_expiry, error}
    """
    result = {"updated": 0, "skipped_no_match": 0, "skipped_no_expiry": 0, "error": None}
    started_at = datetime.utcnow()
    logger.info("[EXPIRY SYNC] ═══ Expiry sync started at %s UTC ═══", started_at.isoformat())

    # 1. Bulk-fetch all items — 1 DB call instead of 2 per VeraCore row.
    try:
        all_items = db.table("items").select("id, sku, expiry_date, veracore_sku").execute().data or []
    except Exception as e:
        err = f"bulk items fetch failed: {e}"
        logger.error("[EXPIRY SYNC] %s", err, exc_info=True)
        log_sync(db, "expiry_sync", None, None, None, "fail", err)
        result["error"] = err
        return result

    items_by_norm = {normalize_sku(it.get("veracore_sku") or it.get("sku") or ""): it
                     for it in all_items if (it.get("veracore_sku") or it.get("sku"))}

    # 2. Pull VeraCore inventory (1 API call).
    try:
        rows = vc_client.get_inventory()
    except Exception as e:
        err = f"get_inventory failed: {e}"
        logger.error("[EXPIRY SYNC] %s", err, exc_info=True)
        log_sync(db, "expiry_sync", None, None, None, "fail", err)
        result["error"] = err
        return result

    # 3. Process in-memory; only write rows where date actually changed.
    for row in rows:
        sku   = row.get("sku", "")
        title = row.get("title", "")

        if "kits" in sku.lower():
            continue

        m = _EXP_RE.search(title)
        if not m:
            result["skipped_no_expiry"] += 1
            continue

        if m.group(1) is not None:
            month = int(m.group(1))
            year  = int(m.group(2))
            if year < 100:
                year += 2000
        else:
            month = _MONTH_MAP.get(m.group(3).lower())
            if not month:
                logger.warning("[EXPIRY SYNC] Unrecognised month name '%s' in title: %s", m.group(3), title)
                result["skipped_no_expiry"] += 1
                continue
            year = int(m.group(4))
        expiry_date = f"{year:04d}-{month:02d}-01"

        norm_sku = normalize_sku(_EXP_RE.sub("", sku).strip())
        item = items_by_norm.get(norm_sku)

        if not item:
            logger.warning("[EXPIRY SYNC] No matching item for VeraCore Id '%s' (norm='%s') — skipping",
                           sku, norm_sku)
            result["skipped_no_match"] += 1
            continue

        if str(item.get("expiry_date") or "")[:10] == expiry_date:
            continue  # already correct, skip write

        try:
            db.table("items").update({"expiry_date": expiry_date}).eq("id", item["id"]).execute()
            result["updated"] += 1
            logger.info("[EXPIRY SYNC] Item %s expiry: %s → %s",
                        item["sku"], item.get("expiry_date") or "none", expiry_date)
            item["expiry_date"] = expiry_date  # keep in-memory state current
        except Exception as e:
            logger.warning("[EXPIRY SYNC] Update failed for item %s: %s", item["sku"], e)

    log_sync(db, "expiry_sync", None,
             {"scanned_rows": len(rows)},
             {"updated": result["updated"],
              "skipped_no_match": result["skipped_no_match"],
              "skipped_no_expiry": result["skipped_no_expiry"]},
             "ok")
    logger.info("[EXPIRY SYNC] ═══ Done: updated=%d no_match=%d no_expiry=%d ═══",
                result["updated"], result["skipped_no_match"], result["skipped_no_expiry"])
    return result


def run_cancellation_sync(db, vc_client, since_iso: Optional[str] = None) -> dict:
    """Detect VeraCore-cancelled orders → mark OBB decision 'cancelled' +
    delete the matching shipment & shipment_items so duplicate-detection stays clean."""
    from datetime import timedelta
    result = {"matched": 0, "cancelled": 0, "shipments_removed": 0, "unmatched": 0, "error": None}
    if since_iso is None:
        since_iso = (datetime.utcnow() - timedelta(days=2)).isoformat()
    logger.info("[VERACORE CANCEL] ═══ Cancellation sync started since=%s ═══", since_iso)

    try:
        canceled = vc_client.get_canceled_orders(since_iso)
    except Exception as e:
        err = f"get_canceled_orders failed: {e}"
        logger.error("[VERACORE CANCEL] %s", err, exc_info=True)
        log_sync(db, "cancellation", None, {"since": since_iso}, None, "fail", err)
        result["error"] = err
        return result

    for c in canceled:
        order_ref = c.get("order_id") or ""
        if not order_ref:
            result["unmatched"] += 1
            continue

        # 3-way match: first by public order_id, then by veracore_order_id (same pattern as run_shipment_poll).
        try:
            rows = (db.table("decisions")
                    .select("id, customer_id, status, order_id, veracore_order_id")
                    .eq("order_id", order_ref).execute().data or [])
            if not rows:
                rows = (db.table("decisions")
                        .select("id, customer_id, status, order_id, veracore_order_id")
                        .eq("veracore_order_id", order_ref).execute().data or [])
        except Exception as e:
            logger.warning("[VERACORE CANCEL] DB lookup failed for order_ref=%s: %s", order_ref, e)
            result["unmatched"] += 1
            continue

        if not rows:
            logger.debug("[VERACORE CANCEL] No decision match for order_ref=%s", order_ref)
            result["unmatched"] += 1
            continue

        result["matched"] += len(rows)
        for d in rows:
            if d.get("status") == "cancelled":
                continue  # idempotent — already done
            try:
                db.table("decisions").update({
                    "status": "cancelled",
                    "veracore_status": "cancelled",
                }).eq("id", d["id"]).execute()
                result["cancelled"] += 1

                # Delete matching shipment(s) stamped with this decision's id prefix.
                ships = (db.table("shipments").select("id")
                         .eq("customer_id", d["customer_id"])
                         .ilike("notes", f"%decision {d['id'][:8]}%").execute().data or [])
                for s in ships:
                    db.table("shipment_items").delete().eq("shipment_id", s["id"]).execute()
                    db.table("shipments").delete().eq("id", s["id"]).execute()
                    result["shipments_removed"] += 1

                log_sync(db, "cancellation", d["id"], {"order_ref": order_ref},
                         {"removed_shipments": len(ships)}, "ok")
                logger.info("[VERACORE CANCEL] decision %s cancelled, %d shipment(s) removed",
                            d["id"], len(ships))
            except Exception as e:
                logger.warning("[VERACORE CANCEL] update failed for decision %s: %s", d["id"], e)

    log_sync(db, "cancellation", None,
             {"since": since_iso},
             {"matched": result["matched"], "cancelled": result["cancelled"],
              "shipments_removed": result["shipments_removed"], "unmatched": result["unmatched"]},
             "ok" if not result["error"] else "fail", result.get("error"))
    logger.info("[VERACORE CANCEL] ═══ done matched=%d cancelled=%d removed=%d unmatched=%d ═══",
                result["matched"], result["cancelled"], result["shipments_removed"], result["unmatched"])
    return result


def run_offer_sync(db, vc_client) -> dict:
    """Detect VeraCore offers with no matching OBB kit → auto-create kit rows
    with metadata parsed from the SKU. Stock filled from GetInventory."""
    from app import parse_kit_attrs_from_sku  # local import avoids circular at module load
    result = {"created": 0, "skipped_existing": 0, "needs_review": 0, "error": None}
    logger.info("[OFFER SYNC] ═══ Offer sync started ═══")

    try:
        offers = vc_client.get_offers()
        inv_rows = vc_client.get_inventory()
        inv = {normalize_sku(r.get("sku") or ""): int(r.get("available_balance", 0))
               for r in inv_rows if r.get("sku")}
        existing = {
            normalize_sku(k.get("veracore_sku") or k.get("sku") or "")
            for k in (db.table("kits").select("sku, veracore_sku").execute().data or [])
        }
    except Exception as e:
        err = f"offer/inventory fetch failed: {e}"
        logger.error("[OFFER SYNC] %s", err, exc_info=True)
        log_sync(db, "offer_sync", None, None, None, "fail", err)
        result["error"] = err
        return result

    for o in offers:
        raw_id = o.get("id") or ""
        norm = normalize_sku(raw_id)
        if not norm or norm in existing:
            result["skipped_existing"] += 1
            continue
        if o.get("inactive"):
            logger.info("[OFFER SYNC] Skipping inactive offer %s", raw_id)
            result["skipped_existing"] += 1
            continue
        try:
            attrs = parse_kit_attrs_from_sku(raw_id)
            db.table("kits").insert({
                "sku":                norm,
                "name":               o.get("title") or norm,
                "veracore_sku":       raw_id,
                "trimester":          attrs["trimester"],
                "size_variant":       attrs["size_variant"],
                "is_welcome_kit":     attrs["is_welcome_kit"],
                "age_rank":           attrs["age_rank"],
                "quantity_available": inv.get(norm, 0),
                "needs_review":       attrs["needs_review"],
            }).execute()
            result["created"] += 1
            if attrs["needs_review"]:
                result["needs_review"] += 1
            log_sync(db, "offer_sync", None, {"offer_id": raw_id}, attrs, "ok")
            logger.info("[OFFER SYNC] created kit %s (T%s sz%s review=%s)",
                        norm, attrs["trimester"], attrs["size_variant"], attrs["needs_review"])
        except Exception as e:
            logger.warning("[OFFER SYNC] insert failed for offer_id=%s: %s", raw_id, e)

    log_sync(db, "offer_sync", None,
             {"offers_checked": len(offers)},
             {"created": result["created"], "skipped_existing": result["skipped_existing"],
              "needs_review": result["needs_review"]},
             "ok" if not result["error"] else "fail", result.get("error"))
    logger.info("[OFFER SYNC] ═══ done created=%d skipped=%d review=%d ═══",
                result["created"], result["skipped_existing"], result["needs_review"])
    return result
