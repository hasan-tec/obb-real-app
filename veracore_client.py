"""
veracore_client.py — VeraCore Public API HTTP wrapper
─────────────────────────────────────────────────────
Phase 3 — Oh Baby Boxes Curation Engine

Thin, dependency-light wrapper around the VeraCore Public REST API.
Why write this from scratch?
  - The community `veracore-api-client` package is GPL-3.0 → would contaminate
    our project's license. We read its source for reference only.
  - We only need 3 endpoints: GET inventory, POST order, GET shipments.

Design rules (DO NOT break):
  1. All HTTP calls are wrapped in try/except.  Caller decides how to record the
     audit row in `veracore_sync_log` — this module just raises VeraCoreError.
  2. Idempotency belongs to the CALLER.  If a decision already has
     `veracore_order_id` populated, do NOT call `add_order` again.
  3. Retries: ONLY on 5xx + network errors (`httpx.TransportError` / timeouts).
     NEVER retry on 4xx — those are permanent (bad payload, bad auth).
  4. Timeouts: 30s on all requests.
  5. Auth is pluggable: "basic" | "base64_json" | "oauth2".  Ting's VeraCore rep
     tells us which to use (see claude/opus-phase3-plan.md §4.1 #4).

⚠️  FIELD-NAME CAVEAT (read before shipping to prod):
    VeraCore tenants differ in exact JSON field casing — e.g. some use
    `OrderID` (PascalCase per REST docs), others use `orderID` or `Order.ID`.
    The shapes below follow VeraCore's public REST conventions as of 2026-04.
    **When Ting provides her Swagger URL, the junior dev MUST cross-check
    every field name below against the live Swagger UI** and adjust.  The
    HTTP structure, auth, retries, logging — all of that stays the same.
    Only the payload keys may need tweaks.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class VeraCoreError(Exception):
    """Raised for any VeraCore API failure (4xx, 5xx after retries, network)."""

    def __init__(self, message: str, status_code: Optional[int] = None,
                 response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


# Max retry attempts on 5xx/network errors (total tries = MAX_RETRIES + 1).
MAX_RETRIES = 3
# Base backoff seconds (exponential: 1s, 2s, 4s...).
BACKOFF_BASE_SECONDS = 1.0
# Per-request timeout (connect + read).
DEFAULT_TIMEOUT_SECONDS = 30.0


class VeraCoreClient:
    """
    Minimal VeraCore Public API client.

    Usage:
        vc = VeraCoreClient(
            base_url="https://acme.veracore.com/VeraCore/Public.Api",
            user_id="obb_api_user",
            password="...",
            system_id="OBBPROD",
            auth_mode="basic",   # or "base64_json" / "oauth2"
        )
        vc.add_order(order_id="OBB-12345", ship_to={...}, line_items=[{...}], ...)
    """

    def __init__(
        self,
        base_url: str,
        user_id: str,
        password: str,
        system_id: str = "",
        auth_mode: str = "basic",
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        # Endpoint-path overrides.  Defaults follow VeraCore Public API REST docs.
        # Override via env vars if Ting's tenant exposes them at different paths.
        inventory_path: str = "/inventory",
        order_path: str = "/orders",
        shipment_path: str = "/shipments",
        oauth_token_path: str = "/oauth/token",
    ):
        if not base_url:
            raise VeraCoreError("VeraCoreClient: base_url is required")
        if auth_mode not in ("basic", "base64_json", "oauth2", "jwt"):
            raise VeraCoreError(f"VeraCoreClient: unknown auth_mode '{auth_mode}'")

        self.base_url = base_url.rstrip("/")
        self.user_id = user_id
        self.password = password
        self.system_id = system_id
        self.auth_mode = auth_mode

        self.inventory_path = inventory_path
        self.order_path = order_path
        self.shipment_path = shipment_path
        self.oauth_token_path = oauth_token_path

        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

        self._http = httpx.Client(timeout=timeout)
        logger.info(
            "[VERACORE] Client initialized base_url=%s auth_mode=%s system_id=%s",
            self.base_url, self.auth_mode, self.system_id or "(none)",
        )

    # ─────────────────────────────────────────────────────────
    # Auth
    # ─────────────────────────────────────────────────────────

    def _get_jwt_token(self) -> str:
        """Fetch + cache JWT from POST /api/Login. Refresh 5 min before expiry."""
        if self._token and time.time() < self._token_expires_at - 300:
            logger.debug("[VERACORE] Reusing cached JWT (expires in %ds)",
                         int(self._token_expires_at - time.time()))
            return self._token

        url = f"{self.base_url}/api/Login"
        logger.info("[VERACORE] Fetching new JWT from %s", url)
        try:
            r = self._http.post(url, json={
                "userName": self.user_id,
                "password": self.password,
                "systemId": self.system_id,
            })
            r.raise_for_status()
            body = r.json()
            self._token = body.get("Token") or body.get("token") or body.get("access_token")
            if not self._token:
                raise VeraCoreError("JWT login returned no token field")
            exp_str = body.get("UtcExpirationDate") or body.get("expires_at")
            if exp_str:
                try:
                    import datetime as _dt
                    exp_clean = exp_str[:26].rstrip("Z") + "+00:00"
                    self._token_expires_at = _dt.datetime.fromisoformat(exp_clean).timestamp()
                except Exception:
                    self._token_expires_at = time.time() + (89 * 24 * 3600)
            else:
                self._token_expires_at = time.time() + (89 * 24 * 3600)
            logger.info("[VERACORE] JWT acquired, expires %s", exp_str or "unknown")
            return self._token
        except httpx.HTTPStatusError as e:
            logger.error("[VERACORE] JWT login failed %s: %s", e.response.status_code, e.response.text)
            raise VeraCoreError(
                f"JWT login failed: {e.response.status_code}",
                status_code=e.response.status_code,
                response_body=e.response.text,
            ) from e
        except VeraCoreError:
            raise
        except Exception as e:
            logger.error("[VERACORE] JWT login network error: %s", e, exc_info=True)
            raise VeraCoreError(f"JWT login network error: {e}") from e

    def _auth_headers(self) -> dict:
        """Build Authorization header based on auth_mode."""
        if self.auth_mode == "jwt":
            return {"Authorization": f"Bearer {self._get_jwt_token()}"}

        if self.auth_mode == "basic":
            token = base64.b64encode(
                f"{self.user_id}:{self.password}".encode("utf-8")
            ).decode("ascii")
            return {"Authorization": f"Basic {token}"}

        if self.auth_mode == "base64_json":
            # Some VeraCore tenants expect the creds as a base64-encoded JSON blob.
            # Documented format: {"UserId":"...","Password":"...","SystemId":"..."}
            payload = json.dumps({
                "UserId": self.user_id,
                "Password": self.password,
                "SystemId": self.system_id,
            })
            token = base64.b64encode(payload.encode("utf-8")).decode("ascii")
            return {"Authorization": f"Basic {token}"}

        if self.auth_mode == "oauth2":
            return {"Authorization": f"Bearer {self._get_oauth_token()}"}

        raise VeraCoreError(f"Unknown auth_mode: {self.auth_mode}")

    def _get_oauth_token(self) -> str:
        """Fetch + cache OAuth2 client_credentials token.  Refresh 60s before expiry."""
        if self._token and time.time() < self._token_expires_at - 60:
            logger.debug("[VERACORE] Reusing cached OAuth token (expires in %ds)",
                         int(self._token_expires_at - time.time()))
            return self._token

        url = f"{self.base_url}{self.oauth_token_path}"
        logger.info("[VERACORE] Fetching new OAuth token from %s", url)
        try:
            r = self._http.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.user_id,
                    "client_secret": self.password,
                },
            )
            r.raise_for_status()
            body = r.json()
            self._token = body["access_token"]
            self._token_expires_at = time.time() + int(body.get("expires_in", 3600))
            logger.info("[VERACORE] OAuth token acquired, expires in %ss",
                        body.get("expires_in", 3600))
            return self._token
        except httpx.HTTPStatusError as e:
            logger.error("[VERACORE] OAuth token fetch failed %s: %s",
                         e.response.status_code, e.response.text)
            raise VeraCoreError(
                f"OAuth token fetch failed: {e.response.status_code}",
                status_code=e.response.status_code,
                response_body=e.response.text,
            ) from e
        except Exception as e:
            logger.error("[VERACORE] OAuth token fetch network error: %s", e, exc_info=True)
            raise VeraCoreError(f"OAuth token fetch network error: {e}") from e

    # ─────────────────────────────────────────────────────────
    # Low-level request with retry
    # ─────────────────────────────────────────────────────────

    def _request(self, method: str, path: str, *,
                 json_body: Optional[dict] = None,
                 params: Optional[dict] = None) -> dict:
        """
        Issue an HTTP request with retries on 5xx + network errors.
        Never retries on 4xx.  Always returns parsed JSON (or {} if empty).
        """
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        headers.update(self._auth_headers())
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        last_exc: Optional[Exception] = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                logger.info("[VERACORE] %s %s (attempt %d/%d)",
                            method, url, attempt + 1, MAX_RETRIES + 1)
                r = self._http.request(
                    method, url,
                    headers=headers,
                    json=json_body,
                    params=params,
                )
                # 4xx → never retry, fail fast
                if 400 <= r.status_code < 500:
                    logger.error("[VERACORE] %s %s → %d (client error, not retrying): %s",
                                 method, url, r.status_code, r.text[:500])
                    raise VeraCoreError(
                        f"VeraCore {method} {path} → {r.status_code}: {r.text[:200]}",
                        status_code=r.status_code,
                        response_body=r.text,
                    )
                # 5xx → retry
                if r.status_code >= 500:
                    logger.warning("[VERACORE] %s %s → %d (server error, will retry)",
                                   method, url, r.status_code)
                    last_exc = VeraCoreError(
                        f"VeraCore {method} {path} → {r.status_code}",
                        status_code=r.status_code,
                        response_body=r.text,
                    )
                    if attempt < MAX_RETRIES:
                        time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
                        continue
                    raise last_exc

                # 2xx → success
                if not r.text.strip():
                    return {}
                try:
                    return r.json()
                except json.JSONDecodeError:
                    logger.warning("[VERACORE] Non-JSON 2xx response: %s", r.text[:200])
                    return {"_raw": r.text}

            except (httpx.TransportError, httpx.TimeoutException) as e:
                logger.warning("[VERACORE] %s %s network error on attempt %d: %s",
                               method, url, attempt + 1, e)
                last_exc = VeraCoreError(f"Network error on {method} {path}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
                    continue
                raise last_exc from e

        # Should be unreachable, but just in case:
        raise last_exc or VeraCoreError(f"VeraCore {method} {path} failed after {MAX_RETRIES + 1} attempts")

    # ─────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────

    def get_inventory(self) -> list[dict]:
        """
        GET {base}/inventory — read live warehouse balances.

        Returns a normalized list: [{sku, available_balance, on_hand, committed}, ...]

        ⚠️  Adjust field names below against Ting's Swagger.  Common shapes:
              [{"Sku": "OBB-CK21", "AvailableBalance": 45, "OnHand": 50, "Committed": 5}]
            or with different casing.  We normalize to snake_case for our DB.
        """
        raw = self._request("GET", self.inventory_path)
        # Response may be a list or wrapped in {"data": [...]} / {"Products": [...]}
        if isinstance(raw, dict):
            for key in ("data", "Products", "Inventory", "items", "Items"):
                if key in raw and isinstance(raw[key], list):
                    raw = raw[key]
                    break
        if not isinstance(raw, list):
            logger.error("[VERACORE] Unexpected inventory response shape: %s", type(raw))
            return []

        normalized = []
        for row in raw:
            if not isinstance(row, dict):
                continue
            sku = (row.get("Id") or row.get("id")
                   or row.get("Sku") or row.get("sku")
                   or row.get("OfferID") or row.get("offer_id"))
            if not sku:
                continue
            normalized.append({
                "sku": str(sku),
                "available_balance": int(
                    row.get("AvailableBalance")
                    or row.get("available_balance")
                    or row.get("Available")
                    or 0
                ),
                "on_hand": int(
                    row.get("OnHand") or row.get("on_hand") or row.get("Quantity") or 0
                ),
                "committed": int(
                    row.get("Committed") or row.get("committed") or 0
                ),
            })
        logger.info("[VERACORE] Inventory sync pulled %d SKUs", len(normalized))
        return normalized

    def get_offers(self, offer_ids: Optional[list] = None) -> list[dict]:
        """
        GET /api/Offers — list Offers (kit definitions) in the VeraCore account.

        Use this to verify which kits exist in the system and get their Offer IDs.
        NOTE: Does NOT include real-time inventory quantities — use get_inventory() for that.

        Returns: [{id, title, status}, ...]
        """
        params = {}
        if offer_ids:
            params["offerIds"] = ",".join(offer_ids)
        raw = self._request("GET", "/api/Offers", params=params or None)
        offers = raw.get("offers", []) if isinstance(raw, dict) else []
        logger.info("[VERACORE] get_offers returned %d offers", len(offers))
        return [
            {
                "id": o.get("id", ""),
                "title": o.get("title", ""),
                "inactive": (o.get("status") or {}).get("inactive") is not None,
            }
            for o in offers if isinstance(o, dict)
        ]

    def add_order(
        self,
        order_id: str,
        _ship_to: dict,
        _line_items: list[dict],
        _shipping_method: str,
        _comments: str = "",
        _customs: Optional[dict] = None,
    ) -> dict:
        """
        Submit a new customer order to VeraCore.

        ⚠️  IMPLEMENTATION BLOCKED — see note below before calling this.

        CONFIRMED from Swagger (2026-05-07): The VeraCore Public REST API at
        /VeraCore/Public.Api has NO endpoint for creating new OMS orders.
        The only REST order endpoints are GET (query existing orders).

        POST /api/ShippingOrder is for creating WMS pick slips from EXISTING OMS
        orders — it requires an orderId that must already exist in the OMS. It is
        NOT for creating new customer orders. Tested live: returns 400 "Unable to
        locate Order for Order Id '...'" when the order doesn't pre-exist.

        WHAT IS NEEDED: The classic VeraCore "Add Order Web Service" — a SOAP
        endpoint at a different base URL (e.g. /VeraCore/Services.asmx or similar).
        Brian (VeraCore rep) must provide: the SOAP endpoint URL and the exact
        XML request schema. Once that is known, this method should be rewritten to
        call the SOAP endpoint using httpx + raw XML (or the zeep library).

        Until then: this method logs a critical warning and raises VeraCoreError so
        the caller sees a clean 'failed' status rather than a silent wrong call.
        """
        if not order_id:
            raise VeraCoreError("add_order: order_id is required")

        logger.critical(
            "[VERACORE] add_order called but SOAP endpoint not yet implemented. "
            "OrderID=%s will NOT be submitted. Ask Brian for the Add Order SOAP URL.",
            order_id,
        )
        raise VeraCoreError(
            "add_order: VeraCore order creation requires the SOAP AddOrder web service "
            "which is not yet implemented. The Public REST API has no POST /api/orders "
            "endpoint. Obtain the SOAP endpoint URL from Brian and implement it here.",
            status_code=None,
        )

    def get_shipments(self, since_iso: str) -> list[dict]:
        """
        GET {base}/shipments?since=... — poll for shipment/tracking updates.

        Returns normalized: [{order_id, tracking_number, carrier, shipped_at}, ...]
        """
        raw = self._request("GET", self.shipment_path, params={"StartDate": since_iso})
        if isinstance(raw, dict):
            for key in ("data", "ShippingOrders", "Shipments", "shipments", "items"):
                if key in raw and isinstance(raw[key], list):
                    raw = raw[key]
                    break
        if not isinstance(raw, list):
            return []

        normalized = []
        for row in raw:
            if not isinstance(row, dict):
                continue
            normalized.append({
                "order_id": row.get("OrderID") or row.get("order_id") or "",
                "tracking_number": row.get("TrackingNumber") or row.get("tracking_number") or "",
                "carrier": row.get("Carrier") or row.get("carrier") or "",
                "shipped_at": row.get("ShippedAt") or row.get("shipped_at") or "",
            })
        logger.info("[VERACORE] get_shipments since=%s → %d rows", since_iso, len(normalized))
        return normalized

    def close(self):
        """Close the underlying HTTP client.  Safe to call multiple times."""
        try:
            self._http.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────
# Shared helpers — used by app.py + veracore_sync.py
# ─────────────────────────────────────────────────────────

def normalize_country(country: Optional[str]) -> str:
    """
    Normalize a raw country string (US / us / United States / USA) → ISO-2 ('US').
    Ensures the `country == 'US'` check works regardless of how data was imported.
    """
    if not country:
        return "US"
    c = country.strip().upper()
    if c in ("US", "USA", "U.S.", "U.S.A.", "UNITED STATES", "UNITED STATES OF AMERICA"):
        return "US"
    if len(c) == 2:
        return c
    # Minimal extra map — extend as needed.
    return {
        "CANADA": "CA",
        "UNITED KINGDOM": "GB",
        "MEXICO": "MX",
        "AUSTRALIA": "AU",
    }.get(c, c[:2])


def pick_shipping_method(country: str) -> str:
    """
    Default shipping method per destination.
    US → USPS Ground Advantage
    Non-US → USPS Priority Mail International
    """
    return "USPS Ground Advantage" if normalize_country(country) == "US" else "USPS Priority Mail International"


def build_customs(kit: dict) -> dict:
    """
    Build a minimal customs declaration for non-US shipments.
    Defaults tuned for OBB's subscription box contents (baby/pregnancy goods).
    """
    return {
        "description": "Subscription Box - Pregnancy Products",
        "declared_value": float(kit.get("cost_per_kit") or 25.00),
        "country_of_origin": "US",
        # HS code 9503.00.00 = "toys; reduced-size scale models" — generic catch-all.
        # Ting can set kit.hs_code per-kit later.
        "hs_code": kit.get("hs_code") or "9503.00.00",
    }
