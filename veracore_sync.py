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
from datetime import datetime
from typing import Optional

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
    Pull VeraCore inventory → update kits.quantity_available → raise low-stock alerts.

    Args:
        db:        Supabase client (from get_supabase()).
        vc_client: VeraCoreClient instance.

    Returns: {synced: int, skipped: int, alerts_raised: int, error: str|None}
    """
    result = {"synced": 0, "skipped": 0, "alerts_raised": 0, "error": None}
    started_at = datetime.utcnow()
    logger.info("[VERACORE SYNC] ═══ Inventory sync started at %s UTC ═══", started_at.isoformat())

    try:
        rows = vc_client.get_inventory()
    except Exception as e:
        err = f"get_inventory failed: {e}"
        logger.error("[VERACORE SYNC] %s", err, exc_info=True)
        log_sync(db, "inventory", None, None, None, "fail", err)
        result["error"] = err
        return result

    for row in rows:
        sku = row.get("sku")
        if not sku:
            result["skipped"] += 1
            continue

        # Match by veracore_sku first (explicit mapping), then fall back to internal SKU.
        kit_query = db.table("kits").select("id, sku, quantity_available, veracore_sku")
        try:
            kit_res = kit_query.eq("veracore_sku", sku).execute()
            if not kit_res.data:
                kit_res = db.table("kits").select(
                    "id, sku, quantity_available, veracore_sku"
                ).eq("sku", sku).execute()
        except Exception as e:
            logger.warning("[VERACORE SYNC] DB lookup failed for sku=%s: %s", sku, e)
            result["skipped"] += 1
            continue

        if not kit_res.data:
            logger.debug("[VERACORE SYNC] No matching kit for VeraCore SKU '%s' — skipping", sku)
            result["skipped"] += 1
            continue

        kit = kit_res.data[0]
        new_qty = max(0, int(row.get("available_balance", 0)))
        old_qty = int(kit.get("quantity_available", 0) or 0)

        try:
            db.table("kits").update({"quantity_available": new_qty}).eq("id", kit["id"]).execute()
            result["synced"] += 1
            if old_qty != new_qty:
                logger.info("[VERACORE SYNC] Kit %s qty: %d → %d", kit["sku"], old_qty, new_qty)
        except Exception as e:
            logger.warning("[VERACORE SYNC] Update failed for kit %s: %s", kit["sku"], e)
            result["skipped"] += 1
            continue

        # Raise low-stock alert if needed — only if no unresolved alert already exists.
        if new_qty < LOW_STOCK_THRESHOLD:
            try:
                existing = db.table("kit_stock_alerts").select("id").eq("kit_id", kit["id"]).eq("resolved", False).execute()
                if not existing.data:
                    db.table("kit_stock_alerts").insert({
                        "kit_id":    kit["id"],
                        "seen_qty":  new_qty,
                        "threshold": LOW_STOCK_THRESHOLD,
                        "note":      f"Auto-raised by inventory sync — VeraCore shows {new_qty} on hand",
                    }).execute()
                    result["alerts_raised"] += 1
                    logger.warning("[VERACORE SYNC] 🚨 LOW STOCK alert raised for %s (qty=%d, threshold=%d)",
                                   kit["sku"], new_qty, LOW_STOCK_THRESHOLD)
            except Exception as e:
                logger.warning("[VERACORE SYNC] Failed to raise stock alert for %s: %s", kit["sku"], e)

    log_sync(db, "inventory", None,
             {"pulled_rows": len(rows)},
             {"synced": result["synced"], "skipped": result["skipped"],
              "alerts_raised": result["alerts_raised"]},
             "ok")
    logger.info("[VERACORE SYNC] ═══ Inventory sync done: synced=%d skipped=%d alerts=%d ═══",
                result["synced"], result["skipped"], result["alerts_raised"])
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
