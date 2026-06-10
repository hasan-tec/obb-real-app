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
import xml.etree.ElementTree as ET
from typing import Optional
from urllib.parse import urlparse
from xml.sax.saxutils import escape as _xml_escape

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

# VeraCore SOAP AddOrder constants.
_SOAP_NS = "http://sma-promail/"
_SOAP_ACTION_ADD_ORDER = "http://sma-promail/AddOrder"
ORDER_ID_MAX_LEN = 20  # VeraCore hard limit

# Carrier/service pairs as configured in VeraCore.
# Source: Brian Alexander email, 2026-05-07.
# Format: "full shipping method string" -> (FreightCarrier.Name, FreightService.Description)
_SHIPPING_MAP: dict[str, tuple[str, str]] = {
    # OBB production carriers
    "OSM USPS First Class":               ("OSM", "USPS First Class"),
    "OSM USPS Priority Mail":             ("OSM", "USPS Priority Mail"),
    "OSM USPS Parcel Select Lightweight": ("OSM", "USPS Parcel Select Lightweight"),
    "OSM USPS Parcel Select":             ("OSM", "USPS Parcel Select"),
    "UPS Ground Prepaid":                 ("UPS", "Ground Prepaid"),
    "UPS Ground Collect":                 ("UPS", "Ground Collect"),
    "UPS Next Day Standard":              ("UPS", "Next Day Standard"),
    "UPS Two Day Air":                    ("UPS", "Two Day Air"),
    "FedEx Ground Collect":               ("FedEx", "Ground Collect"),
    "FEDEX Ground":                       ("FedEx", "Ground"),
    # TSTSHM test carriers — carrier code as Description (trying P03 code lookup)
    "USPS P03":                           ("USPS", "P03"),
    "USPS Priority Mail":                 ("USPS", "Priority Mail"),
    "UPS Ground":                         ("UPS", "Ground"),
    "UPS Next Day Air Letter":            ("UPS", "Next Day Air Letter"),
    "UPS 2nd Day Air":                    ("UPS", "2nd Day Air"),
}


def _split_shipping_method(shipping_method: str) -> tuple[str, str]:
    """Map a shipping method string to (FreightCarrier.Name, FreightService.Description)."""
    if shipping_method in _SHIPPING_MAP:
        return _SHIPPING_MAP[shipping_method]
    # Fallback: split on first space (carrier = first word).
    parts = shipping_method.split(" ", 1)
    return (parts[0], parts[1]) if len(parts) > 1 else (parts[0], parts[0])


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
        # SOAP endpoint for order creation (different base path from REST API).
        # Defaults to https://<same-host>/pmomsws/order.asmx if not supplied.
        soap_url: str = "",
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

        # SOAP endpoint — derive from REST host if not explicitly set.
        parsed = urlparse(self.base_url)
        self.soap_url = soap_url or f"{parsed.scheme}://{parsed.netloc}/pmomsws/order.asmx"

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
        GET {base}/api/GetInventory — read live warehouse offer balances.

        Confirmed field names (OhBaby tenant, live-tested 2026-05):
          Id          — offer SKU  (e.g. "OBB-WK-C3 Kits")
          Available   — available balance (excluding reserved)
          Title       — display name; may embed expiry ("... EXP 10/2026")

        Returns: [{sku, title, available_balance, on_hand, committed}, ...]
        'title' is included so callers can parse expiry dates without a second API call.
        """
        raw = self._request("GET", self.inventory_path)
        # Real response shape: {"Inventory": [...], "Error": null}
        if isinstance(raw, dict):
            for key in ("Inventory", "inventory", "data", "Products", "items", "Items"):
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
                "sku":               str(sku),
                "title":             str(row.get("Title") or row.get("title") or ""),
                # "Available" is the confirmed field name; fallbacks kept for other tenants
                "available_balance": int(row.get("Available")
                                         or row.get("AvailableBalance")
                                         or row.get("available_balance")
                                         or 0),
                "on_hand":           int(row.get("OnHand") or row.get("on_hand") or 0),
                "committed":         int(row.get("Committed") or row.get("committed") or 0),
            })
        logger.info("[VERACORE] GetInventory pulled %d SKUs", len(normalized))
        return normalized

    # ─────────────────────────────────────────────────────────
    # SOAP helpers (AddOrder)
    # ─────────────────────────────────────────────────────────

    def _build_soap_add_order_xml(
        self,
        order_id: str,
        ship_to: dict,
        line_items: list[dict],
        shipping_method: str,
        comments: str,
    ) -> str:
        """
        Return a SOAP 1.1 envelope for AddOrder, matching the actual WSDL schema.

        Field sources confirmed from rhu190.veracore.com/pmomsws/order.asmx?wsdl:
        - OrderedBy / OrderShipTo both extend OrderMailer, which has:
          FirstName, LastName, Address1, Address2, City, State, PostalCode, Country, Phone, Email
        - Offers is a direct child of <order>, NOT nested inside ShipTo
        - Shipping carrier/service goes in <Shipping><FreightCarrier><Name> + <FreightService><Description>
        - Flag=OrderedBy on OrderShipTo means "ship to same address as OrderedBy"
        - AuthenticationHeader and AddOrder use xmlns="http://sma-promail/" default namespace (no prefix)
        """

        def e(v) -> str:
            return _xml_escape(str(v or ""))

        # Split full name → FirstName / LastName (OrderMailer uses separate fields).
        name_parts = (ship_to.get("name") or "").strip().split(" ", 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        # FreightService is optional — OBB uses Pirate Ship so VeraCore doesn't need to ship.
        # Only include Shipping block if shipping_method is provided.
        carrier, service = _split_shipping_method(shipping_method) if shipping_method else ("", "")

        # Build OfferOrdered blocks — Offers is at <order> level, not inside ShipTo.
        # Offer.Header.ID = the kit Offer code in VeraCore (e.g. "CK21").
        # ShipToKey=0 links each OfferOrdered to the single ShipTo record (Key=0).
        # Required by VeraCore SOAP spec — "value of 0 can be used for single-shipment orders".
        offers_xml = ""
        for item in line_items:
            offer_sku = item.get("offer_id") or item.get("sku") or ""
            qty = int(item.get("quantity", 1))
            offers_xml += f"""
        <OfferOrdered>
          <Offer>
            <Header>
              <ID>{e(offer_sku)}</ID>
            </Header>
          </Offer>
          <Quantity>{qty}</Quantity>
          <OrderShipToKey>
            <Key>0</Key>
          </OrderShipToKey>
        </OfferOrdered>"""

        addr2_xml = f"<Address2>{e(ship_to['address2'])}</Address2>" if ship_to.get("address2") else ""
        phone_xml = f"<Phone>{e(ship_to['phone'])}</Phone>" if ship_to.get("phone") else ""
        comments_xml = f"<Comments>{e(comments)}</Comments>" if comments else ""
        freight_service_xml = f"<FreightService><Description>{e(service)}</Description></FreightService>" if service else ""
        shipping_xml = f"""<Shipping>
          <FreightCarrier><Name>{e(carrier)}</Name></FreightCarrier>
          {freight_service_xml}
        </Shipping>""" if carrier else ""

        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <AuthenticationHeader xmlns="http://sma-promail/">
      <Username>{e(self.user_id)}</Username>
      <Password>{e(self.password)}</Password>
    </AuthenticationHeader>
  </soap:Header>
  <soap:Body>
    <AddOrder xmlns="http://sma-promail/">
      <order>
        <Header>
          <ID>{e(order_id)}</ID>
          {comments_xml}
        </Header>
        {shipping_xml}
        <OrderedBy>
          <FirstName>{e(first_name)}</FirstName>
          <LastName>{e(last_name)}</LastName>
          <Address1>{e(ship_to.get("address1"))}</Address1>
          {addr2_xml}
          <City>{e(ship_to.get("city"))}</City>
          <State>{e(ship_to.get("state"))}</State>
          <PostalCode>{e(ship_to.get("zip"))}</PostalCode>
          <Country>{e(ship_to.get("country", "US"))}</Country>
          {phone_xml}
        </OrderedBy>
        <ShipTo>
          <OrderShipTo>
            <Key>0</Key>
            <Flag>OrderedBy</Flag>
          </OrderShipTo>
        </ShipTo>
        <Offers>{offers_xml}
        </Offers>
      </order>
    </AddOrder>
  </soap:Body>
</soap:Envelope>"""

    def _soap_request(self, xml_body: str, soap_action: str) -> str:
        """POST raw SOAP XML to self.soap_url with retry on 5xx/network errors."""
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f'"{soap_action}"',
        }
        last_exc: Optional[Exception] = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                logger.info("[VERACORE-SOAP] POST %s (attempt %d/%d)",
                            self.soap_url, attempt + 1, MAX_RETRIES + 1)
                r = self._http.post(
                    self.soap_url,
                    content=xml_body.encode("utf-8"),
                    headers=headers,
                )
                if 400 <= r.status_code < 500:
                    logger.error("[VERACORE-SOAP] %d (client error): %s",
                                 r.status_code, r.text[:500])
                    raise VeraCoreError(
                        f"SOAP AddOrder → {r.status_code}: {r.text[:200]}",
                        status_code=r.status_code,
                        response_body=r.text,
                    )
                if r.status_code >= 500:
                    # SOAP faults come back as HTTP 500 with an XML body.
                    # If the body is XML, return it and let add_order() parse the fault.
                    # Only retry if it's a non-XML server crash (HTML error page etc.).
                    if r.text.strip().startswith("<"):
                        logger.error("[VERACORE-SOAP] SOAP 500 XML response (likely fault): %s",
                                     r.text[:300])
                        return r.text
                    logger.warning("[VERACORE-SOAP] %d non-XML server error (will retry)", r.status_code)
                    last_exc = VeraCoreError(
                        f"SOAP AddOrder -> {r.status_code}",
                        status_code=r.status_code,
                        response_body=r.text,
                    )
                    if attempt < MAX_RETRIES:
                        time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
                        continue
                    raise last_exc
                return r.text
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                logger.warning("[VERACORE-SOAP] Network error attempt %d: %s", attempt + 1, exc)
                last_exc = VeraCoreError(f"SOAP network error: {exc}")
                if attempt < MAX_RETRIES:
                    time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
                    continue
                raise last_exc from exc
        raise last_exc or VeraCoreError("SOAP request failed after retries")

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
        offers = (raw.get("Offers") or raw.get("offers") or []) if isinstance(raw, dict) else []
        logger.info("[VERACORE] get_offers returned %d offers", len(offers))
        return [
            {
                "id":       o.get("Id")    or o.get("id")    or "",
                "title":    o.get("Title") or o.get("title") or "",
                "bom_type": o.get("BillOfMaterialsType") or o.get("billOfMaterialsType") or "",
                "inactive": (o.get("Status") or o.get("status") or {}).get("Inactive", {}).get("Indicator", 0) == 1,
            }
            for o in offers if isinstance(o, dict)
        ]

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
        Submit a new customer order to VeraCore via SOAP AddOrder web service.

        Endpoint: POST {soap_url}  (default: https://<host>/pmomsws/order.asmx)
        SOAPAction: "http://sma-promail/AddOrder"
        Auth: SOAP AuthenticationHeader (Username + Password) — NOT JWT.

        line_items items must have keys: offer_id (= kit SKU in VeraCore, e.g. "CK21")
        and quantity (int, typically 1).

        Returns: {"order_id": str, "veracore_internal_id": int_or_None}
          - veracore_internal_id = OrderSeqID from VeraCore (their internal PK)
          - order_id = the ID we sent (echoed back)

        NOTE: order_id is hard-limited to 20 characters by VeraCore.
        NOTE: customs is not sent in the SOAP body (no standard AddOrder field for it).
              For international orders, customs paperwork is handled separately in VeraCore.
        """
        if not order_id:
            raise VeraCoreError("add_order: order_id is required")
        if not ship_to:
            raise VeraCoreError("add_order: ship_to is required")
        if not line_items:
            raise VeraCoreError("add_order: line_items cannot be empty")
        if len(order_id) > ORDER_ID_MAX_LEN:
            raise VeraCoreError(
                f"add_order: order_id '{order_id}' exceeds {ORDER_ID_MAX_LEN}-char VeraCore limit"
            )

        logger.info(
            "[VERACORE] add_order SOAP OrderID=%s ship_to=%s items=%d soap_url=%s",
            order_id, ship_to.get("name", "?"), len(line_items), self.soap_url,
        )

        xml_body = self._build_soap_add_order_xml(
            order_id, ship_to, line_items, shipping_method, comments
        )
        response_text = self._soap_request(xml_body, _SOAP_ACTION_ADD_ORDER)
        logger.debug("[VERACORE] add_order raw SOAP response: %s", response_text[:1000])

        # Parse SOAP response XML.
        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as exc:
            raise VeraCoreError(f"SOAP response is not valid XML: {exc}") from exc

        # Check for SOAP Fault.
        fault = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Fault")
        if fault is None:
            fault = root.find(".//faultstring")
        if fault is not None:
            fs = fault.find("faultstring")
            fault_msg = (fs.text if fs is not None else ET.tostring(fault, encoding="unicode"))
            raise VeraCoreError(f"SOAP fault from VeraCore: {fault_msg}")

        # Extract OrderSeqID (their internal PK) and OrderID (our echoed ID).
        # f-prefix required on all searches so _SOAP_NS is interpolated (not literal text).
        seq_elem = (
            root.find(f"{{{_SOAP_NS}}}OrderSeqID")
            or root.find(f".//{{{_SOAP_NS}}}OrderSeqID")
            or root.find(".//OrderSeqID")
        )
        oid_elem = (
            root.find(f"{{{_SOAP_NS}}}OrderID")
            or root.find(f".//{{{_SOAP_NS}}}OrderID")
            or root.find(".//OrderID")
        )
        if seq_elem is None:
            logger.warning("[VERACORE] OrderSeqID not found in SOAP response — raw: %s", response_text[:500])

        seq_id = int(seq_elem.text) if seq_elem is not None and seq_elem.text else None
        returned_id = oid_elem.text if oid_elem is not None and oid_elem.text else order_id

        logger.info("[VERACORE] add_order success OrderID=%s OrderSeqID=%s", returned_id, seq_id)
        return {"order_id": returned_id, "veracore_internal_id": seq_id}

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
    US → OSM USPS Priority Mail (OBB's standard carrier, confirmed by Brian Alexander 2026-05-07)
    Non-US → ask Brian/Ting for the correct international carrier before going live
    """
    return "OSM USPS Priority Mail" if normalize_country(country) == "US" else "OSM USPS Priority Mail"


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
