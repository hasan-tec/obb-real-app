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
        if auth_mode not in ("basic", "base64_json", "oauth2"):
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

    def _auth_headers(self) -> dict:
        """Build Authorization header based on auth_mode."""
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
            sku = row.get("Sku") or row.get("sku") or row.get("OfferID") or row.get("offer_id")
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

    def add_order(
        self,
        order_id: str,
        ship_to: dict,
        line_items: list[dict],
        shipping_method: str,
        comments: str = "",
        customs: Optional[dict] = None,
    ) -> dict:
        """
        POST {base}/orders — submit an order to VeraCore.

        ship_to      = {name, address1, address2?, city, state, zip, country, phone?}
        line_items   = [{offer_id, quantity}, ...]
        customs      = optional, REQUIRED for non-US: {description, declared_value,
                       country_of_origin, hs_code?}

        Returns: {order_id (OBB), veracore_internal_id (VC), status, raw}
        """
        if not order_id:
            raise VeraCoreError("add_order: order_id is required")
        if not line_items:
            raise VeraCoreError("add_order: at least one line item required")

        # Build VeraCore-style payload (PascalCase, per REST docs).
        # Junior dev: cross-check field names against Ting's Swagger before go-live.
        payload = {
            "OrderID": order_id,
            "SystemID": self.system_id,
            "ShipTo": {
                "Name": ship_to.get("name", ""),
                "Address1": ship_to.get("address1", ""),
                "Address2": ship_to.get("address2", "") or "",
                "City": ship_to.get("city", ""),
                "State": ship_to.get("state", ""),
                "Zip": ship_to.get("zip", ""),
                "Country": ship_to.get("country", "US"),
                "Phone": ship_to.get("phone", "") or "",
            },
            "Items": [
                {"OfferID": li["offer_id"], "Quantity": int(li.get("quantity", 1))}
                for li in line_items
            ],
            "ShipMethod": shipping_method,
            "Comments": comments,
        }
        if customs:
            payload["Customs"] = {
                "Description": customs.get("description", ""),
                "DeclaredValue": float(customs.get("declared_value", 0)),
                "CountryOfOrigin": customs.get("country_of_origin", "US"),
                "HSCode": customs.get("hs_code", ""),
            }

        logger.info("[VERACORE] add_order OrderID=%s, items=%d, method=%s, customs=%s",
                    order_id, len(line_items), shipping_method, bool(customs))
        raw = self._request("POST", self.order_path, json_body=payload)

        # Response shape varies by tenant. Extract whatever identifier VeraCore returns.
        veracore_internal_id = (
            raw.get("VeraCoreOrderID")
            or raw.get("veracore_order_id")
            or raw.get("InternalOrderID")
            or raw.get("OrderNumber")
            or raw.get("order_number")
            or raw.get("OrderID")
            or order_id  # fall back to our ID if VC echoes it
        )
        return {
            "order_id": order_id,
            "veracore_internal_id": str(veracore_internal_id),
            "status": raw.get("Status") or raw.get("status") or "submitted",
            "raw": raw,
        }

    def get_shipments(self, since_iso: str) -> list[dict]:
        """
        GET {base}/shipments?since=... — poll for shipment/tracking updates.

        Returns normalized: [{order_id, tracking_number, carrier, shipped_at}, ...]
        """
        raw = self._request("GET", self.shipment_path, params={"since": since_iso})
        if isinstance(raw, dict):
            for key in ("data", "Shipments", "shipments", "items"):
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
