"""
cratejoy_client.py — Cratejoy Merchant API client (outbound tracking write-back)

Auth: Basic base64(client_id:client_secret) — same creds as webhook validation.
Endpoint: https://api.cratejoy.com/v1/

Flow per row from the Pirate Ship tracking export (see app.upload_tracking):
  1. the row is tied to ONE engine decision (OBB Ref / order id / email + recipient name)
  2. list_customer_shipments(cj_customer_id) + app.pick_cratejoy_shipment_for_tracking()
     choose that decision's EXACT shipment (never "most recent by email")
  3. add_tracking(shipment_id, …) — PUT /v1/shipments/{id}/ with status="shipped"

Threads 20/21: the old email-based lookup picked "the most recent unshipped shipment", so a
re-uploaded file marked the customer's NEXT prepaid box shipped (24 future boxes, 21 customers).
It was removed; idempotency now lives in decisions.tracking_number (migration 022).
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
        shipments = cc.list_customer_shipments("7093449651")
        cc.add_tracking(shipment_id=7093457045, tracking_number="9334...", carrier="USPS")
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
    # Read (exact shipments only)
    # ─────────────────────────────────────────────────────────

    def get_shipment(self, shipment_id) -> dict:
        """GET /v1/shipments/{id}/ — the one shipment, or raise CratejoyError."""
        logger.info("[CRATEJOY] get_shipment shipment=%s", shipment_id)
        try:
            r = self._http.get(f"{_BASE}/shipments/{shipment_id}/", headers=self._headers())
        except httpx.HTTPError as e:
            raise CratejoyError(f"network error on get_shipment: {e}") from e
        if r.status_code != 200:
            raise CratejoyError(f"get_shipment failed: {r.status_code}", r.status_code, r.text[:300])
        return r.json()

    def list_customer_shipments(self, cj_customer_id) -> list[dict]:
        """
        GET /v1/shipments/?customer_id={id} — ALL of this Cratejoy customer's shipments
        (any status, every subscription), following `next` pages. Raises CratejoyError.
        """
        logger.info("[CRATEJOY] list_customer_shipments customer=%s", cj_customer_id)
        url: Optional[str] = f"{_BASE}/shipments/"
        params: Optional[dict] = {"customer_id": cj_customer_id, "limit": 100}
        out: list[dict] = []
        pages = 0
        while url and pages < 20:  # hard stop — a customer never has 2,000 shipments
            pages += 1
            try:
                r = self._http.get(url, headers=self._headers(), params=params)
            except httpx.HTTPError as e:
                raise CratejoyError(f"network error on list_customer_shipments: {e}") from e
            if r.status_code != 200:
                raise CratejoyError(f"list_customer_shipments failed: {r.status_code}", r.status_code, r.text[:300])
            data = r.json()
            out.extend(data.get("results") or [])
            nxt = data.get("next")
            params = None  # `next` already carries the query string
            if not nxt:
                url = None
            elif nxt.startswith("http"):
                url = nxt
            elif nxt.startswith("/"):
                url = "https://api.cratejoy.com" + nxt
            else:
                url = f"{_BASE}/shipments/" + nxt
        logger.info("[CRATEJOY] list_customer_shipments customer=%s → %d shipment(s)", cj_customer_id, len(out))
        return out

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

    def close(self):
        try:
            self._http.close()
        except Exception:
            pass
