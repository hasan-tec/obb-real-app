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
    Pull VeraCore inventory → update kits.quantity_available + items.quantity_available
    → raise low-stock alerts for kits under LOW_STOCK_THRESHOLD.

    Optimised: bulk-fetches all kits + items + alerts upfront, does matching in-memory,
    and only writes rows where quantity actually changed.

    Returns: {synced: int, items_synced: int, skipped: int, alerts_raised: int, error: str|None}
    """
    result = {"synced": 0, "items_synced": 0, "skipped": 0, "alerts_raised": 0, "error": None}
    started_at = datetime.utcnow()
    logger.info("[VERACORE SYNC] ═══ Inventory sync started at %s UTC ═══", started_at.isoformat())

    # 1. Bulk-fetch kits, items, and existing unresolved alerts — 3 DB calls total.
    try:
        all_kits    = db.table("kits").select("id, sku, quantity_available, veracore_sku").execute().data or []
        all_items   = db.table("items").select("id, sku, quantity_available, veracore_sku").execute().data or []
        alerts_data = db.table("kit_stock_alerts").select("kit_id").eq("resolved", False).execute().data or []
    except Exception as e:
        err = f"bulk DB fetch failed: {e}"
        logger.error("[VERACORE SYNC] %s", err, exc_info=True)
        log_sync(db, "inventory", None, None, None, "fail", err)
        result["error"] = err
        return result

    # Build O(1) lookup dicts keyed by upper-cased SKU.
    kits_by_vc_sku   = {k["veracore_sku"].upper(): k for k in all_kits if k.get("veracore_sku")}
    kits_by_sku      = {k["sku"].upper(): k for k in all_kits if k.get("sku")}
    items_by_vc_sku  = {it["veracore_sku"].upper(): it for it in all_items if it.get("veracore_sku")}
    items_by_sku     = {it["sku"].upper(): it for it in all_items if it.get("sku")}
    alerted_kit_ids  = {r["kit_id"] for r in alerts_data}

    logger.info("[VERACORE SYNC] Loaded %d kits, %d items from DB", len(all_kits), len(all_items))

    # 2. Pull VeraCore inventory (1 API call).
    try:
        rows = vc_client.get_inventory()
    except Exception as e:
        err = f"get_inventory failed: {e}"
        logger.error("[VERACORE SYNC] %s", err, exc_info=True)
        log_sync(db, "inventory", None, None, None, "fail", err)
        result["error"] = err
        return result

    synced_at_iso = datetime.utcnow().isoformat()

    # 3. Process in-memory; only DB-write rows where qty actually changed.
    for row in rows:
        sku = row.get("sku")
        if not sku:
            result["skipped"] += 1
            continue

        sku_upper = sku.upper()
        new_qty = max(0, int(row.get("available_balance", 0) or 0))

        # ── Kit match ──
        kit = kits_by_vc_sku.get(sku_upper) or kits_by_sku.get(sku_upper)
        if kit:
            old_qty = int(kit.get("quantity_available", 0) or 0)
            if old_qty != new_qty:
                try:
                    db.table("kits").update({"quantity_available": new_qty}).eq("id", kit["id"]).execute()
                    logger.info("[VERACORE SYNC] Kit %s qty: %d → %d", kit["sku"], old_qty, new_qty)
                    kit["quantity_available"] = new_qty
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
                    alerted_kit_ids.add(kit["id"])
                    result["alerts_raised"] += 1
                    logger.warning("[VERACORE SYNC] LOW STOCK alert raised for %s (qty=%d, threshold=%d)",
                                   kit["sku"], new_qty, LOW_STOCK_THRESHOLD)
                except Exception as e:
                    logger.warning("[VERACORE SYNC] Failed to raise stock alert for %s: %s", kit["sku"], e)
            continue

        # ── Item match (item-level VC SKUs contain '+') ──
        item = items_by_vc_sku.get(sku_upper) or items_by_sku.get(sku_upper)
        if item:
            old_qty = int(item.get("quantity_available", 0) or 0)
            patch = {"inventory_synced_at": synced_at_iso}
            if old_qty != new_qty:
                patch["quantity_available"] = new_qty
            try:
                db.table("items").update(patch).eq("id", item["id"]).execute()
                if old_qty != new_qty:
                    logger.info("[VERACORE SYNC] Item %s qty: %d → %d", item["sku"], old_qty, new_qty)
                    item["quantity_available"] = new_qty
            except Exception as e:
                logger.warning("[VERACORE SYNC] Item update failed for %s: %s", item["sku"], e)
                result["skipped"] += 1
                continue
            result["items_synced"] += 1
            continue

        logger.debug("[VERACORE SYNC] No matching kit or item for VeraCore SKU '%s' — skipping", sku)
        result["skipped"] += 1

    log_sync(db, "inventory", None,
             {"pulled_rows": len(rows)},
             {"synced": result["synced"], "items_synced": result["items_synced"],
              "skipped": result["skipped"], "alerts_raised": result["alerts_raised"]},
             "ok")
    logger.info(
        "[VERACORE SYNC] ═══ Inventory sync done: kits=%d items=%d skipped=%d alerts=%d ═══",
        result["synced"], result["items_synced"], result["skipped"], result["alerts_raised"],
    )
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

    items_by_vc_sku = {it["veracore_sku"].upper(): it for it in all_items if it.get("veracore_sku")}
    items_by_sku    = {it["sku"].upper(): it for it in all_items if it.get("sku")}

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

        sku_upper = _EXP_RE.sub("", sku).strip().upper()
        item = items_by_vc_sku.get(sku_upper) or items_by_sku.get(sku_upper)

        if not item:
            logger.debug("[EXPIRY SYNC] No matching item for VeraCore Id '%s' — skipping", sku)
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
