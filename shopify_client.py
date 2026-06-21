"""
shopify_client.py — Shopify Admin API client (OUTBOUND)
───────────────────────────────────────────────────────
Phase 3 — Oh Baby Boxes Curation Engine

Why this exists:
  The Engine only ever *received* Shopify webhooks; it had no way to write back.
  After the workflow moved to CSV-import in Pirate Ship, Shopify orders stopped
  auto-fulfilling (the old PS<->Shopify native link was bypassed). This client lets
  the Engine push tracking -> Shopify so orders flip to "Fulfilled" + customers get
  their tracking email.

Auth (verified 2026-06):
  OBB's app is a NEW Shopify Dev Dashboard app — there is NO static shpat_ token.
  We exchange client_id + client_secret for a 24h token via the client-credentials
  grant (POST /admin/oauth/access_token), cache it, and refresh before expiry.

Fulfillment (schema introspected live from the OhBaby tenant, 2025-01):
  FulfillmentInput = { trackingInfo{number,company,url}, notifyCustomer,
                       lineItemsByFulfillmentOrder[{fulfillmentOrderId}] }
  Omitting fulfillmentOrderLineItems fulfills ALL remaining items in that
  fulfillment order. Idempotent: an order with no OPEN fulfillment orders is
  already fulfilled -> we skip it (no duplicate fulfillment on retry).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import httpx

logger = logging.getLogger("obb")

DEFAULT_TIMEOUT_SECONDS = 30.0
# Fulfillment orders in these states still have items we can fulfill.
_OPEN_FO_STATES = {"OPEN", "IN_PROGRESS", "SCHEDULED"}


class ShopifyError(Exception):
    """Raised for any Shopify Admin API failure (auth, HTTP, GraphQL, userErrors)."""

    def __init__(self, message: str, status_code: Optional[int] = None,
                 body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class ShopifyClient:
    """
    Minimal Shopify Admin API client for fulfillment write-back.

    Usage:
        sc = ShopifyClient(
            shop_domain="oh-baby-boxes-llc.myshopify.com",
            client_id="...",
            client_secret="shpss_...",
        )
        sc.fulfill_order("7286669279521", tracking_number="1Z...", carrier="UPS")
    """

    def __init__(
        self,
        shop_domain: str,
        client_id: str,
        client_secret: str,
        api_version: str = "2025-01",
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ):
        if not (shop_domain and client_id and client_secret):
            raise ShopifyError("ShopifyClient: shop_domain, client_id, client_secret are all required")
        # Normalize: we want the bare myshopify host (no scheme, no trailing slash).
        self.shop_domain = shop_domain.replace("https://", "").replace("http://", "").strip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = api_version

        self._http = httpx.Client(timeout=timeout)
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._lock = threading.Lock()
        logger.info("[SHOPIFY] client init — shop=%s api_version=%s", self.shop_domain, self.api_version)

    # ─────────────────────────────────────────────────────────
    # Auth — client-credentials grant (24h token, cached + refreshed)
    # ─────────────────────────────────────────────────────────

    def _get_token(self) -> str:
        with self._lock:
            # Refresh 2 min before expiry to avoid mid-request expiry.
            if self._token and time.time() < self._token_expires_at - 120:
                return self._token
            url = f"https://{self.shop_domain}/admin/oauth/access_token"
            logger.info("[SHOPIFY] fetching client_credentials token from %s", url)
            try:
                r = self._http.post(url, data={
                    "grant_type":    "client_credentials",
                    "client_id":     self.client_id,
                    "client_secret": self.client_secret,
                })
            except httpx.HTTPError as e:
                raise ShopifyError(f"token exchange network error: {e}") from e
            if r.status_code != 200:
                logger.error("[SHOPIFY] token exchange failed %s: %s", r.status_code, r.text[:300])
                raise ShopifyError(f"token exchange failed: {r.status_code}", r.status_code, r.text)
            body = r.json()
            self._token = body.get("access_token")
            if not self._token:
                raise ShopifyError("token exchange returned no access_token", body=str(body))
            self._token_expires_at = time.time() + int(body.get("expires_in", 86399))
            logger.info("[SHOPIFY] token acquired — expires_in=%s scope=%s",
                        body.get("expires_in"), body.get("scope"))
            return self._token

    # ─────────────────────────────────────────────────────────
    # Low-level GraphQL
    # ─────────────────────────────────────────────────────────

    def _graphql(self, query: str, variables: Optional[dict] = None) -> dict:
        url = f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"
        headers = {"X-Shopify-Access-Token": self._get_token(), "Content-Type": "application/json"}
        try:
            r = self._http.post(url, headers=headers, json={"query": query, "variables": variables or {}})
        except httpx.HTTPError as e:
            raise ShopifyError(f"GraphQL network error: {e}") from e
        if r.status_code != 200:
            raise ShopifyError(f"GraphQL HTTP {r.status_code}: {r.text[:200]}", r.status_code, r.text)
        data = r.json()
        if data.get("errors"):
            raise ShopifyError(f"GraphQL errors: {data['errors']}", body=str(data["errors"]))
        return data.get("data") or {}

    # ─────────────────────────────────────────────────────────
    # Order resolution — accept numeric id, gid, or order name (#OBB-123)
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _to_order_gid(numeric_or_gid: str) -> str:
        s = str(numeric_or_gid).strip()
        if s.startswith("gid://"):
            return s
        return f"gid://shopify/Order/{s}"

    def find_order_id_by_name(self, name: str) -> Optional[str]:
        """Resolve an order *name* (e.g. '#OBB-13652' or 'OBB-13652') -> numeric id."""
        n = name.strip()
        if not n.startswith("#"):
            n = "#" + n
        q = "query($q:String!){orders(first:5,query:$q){nodes{id name}}}"
        nodes = (self._graphql(q, {"q": f"name:{n}"}).get("orders") or {}).get("nodes") or []
        for node in nodes:
            if (node.get("name") or "").lstrip("#") == n.lstrip("#"):
                return node["id"].rsplit("/", 1)[-1]
        return nodes[0]["id"].rsplit("/", 1)[-1] if nodes else None

    def find_order_id_by_email(self, email: str) -> Optional[str]:
        """Resolve the most recent *unfulfilled* order for an email -> numeric id."""
        q = ("query($q:String!){orders(first:5,query:$q,sortKey:CREATED_AT,reverse:true)"
             "{nodes{id name displayFulfillmentStatus}}}")
        qstr = f"email:{email} fulfillment_status:unfulfilled"
        nodes = (self._graphql(q, {"q": qstr}).get("orders") or {}).get("nodes") or []
        return nodes[0]["id"].rsplit("/", 1)[-1] if nodes else None

    # ─────────────────────────────────────────────────────────
    # Fulfillment
    # ─────────────────────────────────────────────────────────

    def get_open_fulfillment_orders(self, order_numeric_id: str) -> list[dict]:
        """Return the order's fulfillment orders that still have fulfillable items."""
        q = ("query($id:ID!){order(id:$id){id name displayFulfillmentStatus "
             "fulfillmentOrders(first:20){nodes{id status}}}}")
        order = self._graphql(q, {"id": self._to_order_gid(order_numeric_id)}).get("order")
        if not order:
            raise ShopifyError(f"order {order_numeric_id} not found")
        fos = (order.get("fulfillmentOrders") or {}).get("nodes") or []
        return [fo for fo in fos if (fo.get("status") or "").upper() in _OPEN_FO_STATES]

    def fulfill_order(
        self,
        order_numeric_id: str,
        tracking_number: str,
        carrier: Optional[str] = None,
        tracking_url: Optional[str] = None,
        notify_customer: bool = True,
    ) -> dict:
        """
        Create a fulfillment (with tracking) for ALL open fulfillment orders of an order.

        Returns one of:
          {status:'fulfilled', fulfillment_id, order_id}
          {status:'already',   order_id}              # no open FOs -> already fulfilled
          {status:'failed',    order_id, error}
        Never raises — callers get a dict they can tally.
        """
        try:
            open_fos = self.get_open_fulfillment_orders(order_numeric_id)
        except ShopifyError as e:
            logger.warning("[SHOPIFY] fulfill %s — lookup failed: %s", order_numeric_id, e)
            return {"status": "failed", "order_id": order_numeric_id, "error": str(e)}

        if not open_fos:
            logger.info("[SHOPIFY] order %s has no open fulfillment orders — already fulfilled, skipping",
                        order_numeric_id)
            return {"status": "already", "order_id": order_numeric_id}

        tracking_info: dict = {"number": tracking_number}
        if carrier:
            tracking_info["company"] = carrier
        if tracking_url:
            tracking_info["url"] = tracking_url

        mutation = (
            "mutation fulfill($f:FulfillmentInput!){"
            "fulfillmentCreate(fulfillment:$f){"
            "fulfillment{id status trackingInfo{number company url}}"
            "userErrors{field message}}}"
        )
        variables = {"f": {
            "lineItemsByFulfillmentOrder": [{"fulfillmentOrderId": fo["id"]} for fo in open_fos],
            "trackingInfo": tracking_info,
            "notifyCustomer": notify_customer,
        }}
        try:
            res = self._graphql(mutation, variables).get("fulfillmentCreate") or {}
        except ShopifyError as e:
            logger.error("[SHOPIFY] fulfillmentCreate failed for %s: %s", order_numeric_id, e)
            return {"status": "failed", "order_id": order_numeric_id, "error": str(e)}

        user_errors = res.get("userErrors") or []
        if user_errors:
            logger.error("[SHOPIFY] fulfillmentCreate userErrors for %s: %s", order_numeric_id, user_errors)
            return {"status": "failed", "order_id": order_numeric_id, "error": str(user_errors)}

        f = res.get("fulfillment") or {}
        logger.info("[SHOPIFY] fulfilled order %s — fulfillment=%s tracking=%s",
                    order_numeric_id, f.get("id"), tracking_number)
        return {"status": "fulfilled", "order_id": order_numeric_id, "fulfillment_id": f.get("id")}

    def close(self):
        try:
            self._http.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────
# Helpers — used by app.py
# ─────────────────────────────────────────────────────────

def normalize_carrier(raw: Optional[str]) -> Optional[str]:
    """
    Map a free-text carrier/service string (e.g. 'UPS Ground Saver') to a carrier
    name Shopify recognizes so it auto-generates the tracking URL. Returns None when
    blank (Shopify then just shows the bare tracking number).
    """
    if not raw:
        return None
    s = raw.strip().upper()
    if "UPS" in s:
        return "UPS"
    if "USPS" in s or "POSTAL" in s:
        return "USPS"
    if "FEDEX" in s:
        return "FedEx"
    if "DHL" in s:
        return "DHL"
    if "CANADA POST" in s:
        return "Canada Post"
    return raw.strip()  # pass through; Shopify treats unknowns as "Other"
