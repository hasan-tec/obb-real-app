"""
cratejoy_client.py — Cratejoy Merchant API client (outbound tracking write-back)

Auth: Basic base64(client_id:client_secret) — same creds as webhook validation.
Endpoint: https://api.cratejoy.com/v1/

Flow per row from Pirate Ship export:
  1. find_unshipped_shipment_by_email(email) — GET /v1/shipments/?customer.email__ilike=…&status=unshipped
  2. add_tracking(shipment_id, …)            — PUT /v1/shipments/{id}/ with status="shipped"

Idempotent: the find step filters status=unshipped, so already-shipped rows are excluded
(counted as 'already'). Re-uploading the same file will NOT re-notify customers.
"""

from __future__ import annotations

import base64
import logging
from typing import Optional

import httpx

logger = logging.getLogger("obb")

_BASE = "https://api.cratejoy.com/v1"


class CratejoyError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class CratejoyClient:
    """
    Minimal Cratejoy Merchant API client for fulfillment write-back.

    Usage:
        cc = CratejoyClient(client_id="...", client_secret="...")
        result = cc.mark_tracking_by_email("jane@example.com", "1Z...", carrier="UPS")
        # result: {status: 'fulfilled'|'already'|'unmatched'|'failed', ...}
    """

    def __init__(self, client_id: str, client_secret: str, timeout: float = 30.0):
        if not (client_id and client_secret):
            raise CratejoyError("CratejoyClient: client_id and client_secret are required")
        raw = f"{client_id}:{client_secret}"
        self._auth_header = "Basic " + base64.b64encode(raw.encode()).decode()
        self._http = httpx.Client(timeout=timeout)
        logger.info("[CRATEJOY] client init")

    def _headers(self) -> dict:
        return {"Authorization": self._auth_header, "Content-Type": "application/json"}

    # ─────────────────────────────────────────────────────────
    # Find
    # ─────────────────────────────────────────────────────────

    def find_unshipped_shipment_by_email(self, email: str) -> Optional[dict]:
        """
        Return the unshipped shipment for this email, or None if not found.

        Primary: customer.email__ilike filter on /v1/shipments/ (single call).
        Fallback: customer lookup -> customer_id -> shipments (two calls) in case
                  the relation filter misbehaves.
        If >1 unshipped shipment, pick the most recent by created_at and log a warning.
        """
        email = email.strip().lower()
        logger.info("[CRATEJOY] finding unshipped shipment — email=%s", email)

        # Primary path
        try:
            r = self._http.get(
                f"{_BASE}/shipments/",
                headers=self._headers(),
                params={"customer.email__ilike": email, "status": "unshipped", "limit": 5},
            )
        except httpx.HTTPError as e:
            raise CratejoyError(f"network error on shipment lookup: {e}") from e
        if r.status_code != 200:
            raise CratejoyError(f"shipment lookup failed: {r.status_code}", r.status_code, r.text)

        results = r.json().get("results") or []

        if not results:
            # Fallback: look up customer_id then query shipments
            logger.info("[CRATEJOY] primary filter returned 0 — trying 2-step fallback for email=%s", email)
            results = self._find_by_customer_id_fallback(email)

        if not results:
            logger.info("[CRATEJOY] no unshipped shipment found for email=%s", email)
            return None

        if len(results) > 1:
            logger.warning("[CRATEJOY] %d unshipped shipments for email=%s — picking most recent",
                           len(results), email)
            results = sorted(results, key=lambda s: s.get("created_at") or "", reverse=True)

        return results[0]

    def _find_by_customer_id_fallback(self, email: str) -> list:
        try:
            r = self._http.get(
                f"{_BASE}/customers/",
                headers=self._headers(),
                params={"email__ilike": email, "limit": 1},
            )
            if r.status_code != 200 or not r.json().get("results"):
                return []
            customer_id = r.json()["results"][0]["id"]
            r2 = self._http.get(
                f"{_BASE}/shipments/",
                headers=self._headers(),
                params={"customer_id": customer_id, "status": "unshipped", "limit": 5},
            )
            if r2.status_code != 200:
                return []
            return r2.json().get("results") or []
        except httpx.HTTPError:
            return []

    # ─────────────────────────────────────────────────────────
    # Write (tracking)
    # ─────────────────────────────────────────────────────────

    def add_tracking(
        self,
        shipment_id: int,
        tracking_number: str,
        carrier: Optional[str] = None,
        tracking_url: Optional[str] = None,
        shipped_at: Optional[str] = None,
    ) -> dict:
        """
        PUT /v1/shipments/{id}/ — sets status='shipped' (triggers customer email)
        and attaches tracking. Returns the updated shipment dict or raises CratejoyError.
        """
        payload: dict = {"status": "shipped", "tracking_number": tracking_number}
        if carrier:
            payload["carrier_name"] = carrier
        if tracking_url:
            payload["tracking_link"] = tracking_url
        if shipped_at:
            payload["shipped_at"] = shipped_at

        logger.info("[CRATEJOY] add_tracking shipment=%s tracking=%s carrier=%s",
                    shipment_id, tracking_number, carrier)
        try:
            r = self._http.put(
                f"{_BASE}/shipments/{shipment_id}/",
                headers=self._headers(),
                json=payload,
            )
        except httpx.HTTPError as e:
            raise CratejoyError(f"network error on add_tracking: {e}") from e
        if r.status_code not in (200, 201):
            raise CratejoyError(
                f"add_tracking failed: {r.status_code}", r.status_code, r.text[:300]
            )
        logger.info("[CRATEJOY] tracking added — shipment=%s status=shipped", shipment_id)
        return r.json()

    # ─────────────────────────────────────────────────────────
    # Combined helper (used by upload-tracking route)
    # ─────────────────────────────────────────────────────────

    def mark_tracking_by_email(
        self,
        email: str,
        tracking_number: str,
        carrier: Optional[str] = None,
        tracking_url: Optional[str] = None,
    ) -> dict:
        """
        Find the unshipped Cratejoy shipment for `email` and push tracking.

        Returns a dict with the same shape as ShopifyClient.fulfill_order:
          {status: 'fulfilled'|'already'|'unmatched'|'failed', shipment_id?, error?}
        """
        try:
            shipment = self.find_unshipped_shipment_by_email(email)
        except CratejoyError as e:
            logger.error("[CRATEJOY] find failed for email=%s: %s", email, e)
            return {"status": "failed", "error": str(e)}

        if shipment is None:
            return {"status": "unmatched"}

        # Belt-and-suspenders: shouldn't happen given the filter
        if (shipment.get("status") or "").lower() == "shipped":
            logger.info("[CRATEJOY] shipment %s already shipped — skipping", shipment["id"])
            return {"status": "already", "shipment_id": shipment["id"]}

        try:
            self.add_tracking(
                shipment_id=shipment["id"],
                tracking_number=tracking_number,
                carrier=carrier,
                tracking_url=tracking_url,
            )
            return {"status": "fulfilled", "shipment_id": shipment["id"]}
        except CratejoyError as e:
            logger.error("[CRATEJOY] add_tracking failed for shipment=%s: %s", shipment["id"], e)
            return {"status": "failed", "shipment_id": shipment["id"], "error": str(e)}

    def close(self):
        try:
            self._http.close()
        except Exception:
            pass
