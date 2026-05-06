#!/usr/bin/env python3
"""
test_soap.py  —  Quick SOAP AddOrder validation against TSTSHM.

Goal: fire a real SOAP call and read the response to determine whether
      the XML format is accepted.

  XML format error  → SOAP fault mentioning schema/deserialize/invalid
  Offer not found   → XML accepted ✅  (TSTSHM won't have OBB kits)
  Auth/permission   → AddOrder permission still not active
  Success           → XML perfect, a test order was created in TSTSHM

Run from the project root:
    cd "c:/Users/hasan/Desktop/opus tin/opus-obb-prototype"
    python test_soap.py
"""

import os
import sys
import logging

# ── Load .env without requiring python-dotenv ──────────────────────────────
def _load_env(path=".env"):
    try:
        with open(path) as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass

_load_env()

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

sys.path.insert(0, os.path.dirname(__file__))
from veracore_client import VeraCoreClient, VeraCoreError

# ── Test payload ───────────────────────────────────────────────────────────
ORDER_ID      = "TOBB-TEST-0507"        # 14 chars — safely under 20-char limit
SHIP_TO       = {
    "name":     "Jane Test Smith",
    "address1": "123 Test Street",
    "address2": "",
    "city":     "Austin",
    "state":    "TX",
    "zip":      "78701",
    "country":  "US",
    "phone":    "512-555-0001",
}
LINE_ITEMS    = [{"offer_id": "CK21", "quantity": 1}]
SHIPPING      = "USPS Ground Advantage"
COMMENTS      = "SOAP format test - do not fulfill"

# ── Build client ───────────────────────────────────────────────────────────
vc = VeraCoreClient(
    base_url       = os.environ["VERACORE_BASE_URL"],
    user_id        = os.environ["VERACORE_USER_ID"],
    password       = os.environ["VERACORE_PASSWORD"],
    system_id      = os.environ.get("VERACORE_SYSTEM_ID", "TSTSHM"),
    auth_mode      = os.environ.get("VERACORE_AUTH_MODE", "jwt"),
    soap_url       = os.environ.get("VERACORE_SOAP_URL", ""),
    inventory_path = os.environ.get("VERACORE_INVENTORY_PATH", "/api/GetInventory"),
    order_path     = os.environ.get("VERACORE_ORDER_PATH", "/api/ShippingOrder"),
    shipment_path  = os.environ.get("VERACORE_SHIPMENT_PATH",
                                    "/api/ShippingOrder/GetCompletedShippingOrderDetails"),
)

# ── Print the XML before sending so we can eyeball it ─────────────────────
xml = vc._build_soap_add_order_xml(ORDER_ID, SHIP_TO, LINE_ITEMS, SHIPPING, COMMENTS)
print("\n" + "=" * 62)
print("XML BEING SENT:")
print("=" * 62)
print(xml)
print("=" * 62 + "\n")

# ── Fire the call ──────────────────────────────────────────────────────────
try:
    result = vc.add_order(
        order_id        = ORDER_ID,
        ship_to         = SHIP_TO,
        line_items      = LINE_ITEMS,
        shipping_method = SHIPPING,
        comments        = COMMENTS,
    )
    print(f"[OK]  SUCCESS: {result}")
    print("\nSOAP AddOrder accepted - XML format is correct.")
    print("A test order was created in TSTSHM (harmless - it's a test system).")

except VeraCoreError as exc:
    print(f"[ERR] VeraCoreError: {exc}")
    if exc.status_code:
        print(f"      HTTP {exc.status_code}")
    body = exc.response_body or ""
    if body:
        print(f"\n--- Raw VeraCore response ---\n{body[:3000]}\n---")

    # Interpret what the error tells us
    b = body.lower()
    print("\n-- Diagnosis ------------------------------------------")
    if any(w in b for w in ("freight service", "freight carrier", "invalid freight")):
        print("  Carrier/service name mismatch - XML format is ACCEPTED [OK]")
        print("  VeraCore doesn't have 'Ground Advantage' configured in this account.")
        print("  Ask Brian for the exact FreightService name in OBB's VeraCore settings.")
    elif any(w in b for w in ("offer", "sku", "product", "item")):
        print("  Offer/SKU not found - XML format is ACCEPTED [OK]")
        print("  Next step: wait for Brian to confirm OBB real System ID + kit Offer IDs.")
    elif any(w in b for w in ("duplicate", "already exist", "order id")):
        print("  Order ID conflict - XML format is ACCEPTED [OK]")
        print("  Change ORDER_ID at the top of this script and re-run.")
    elif any(w in b for w in ("schema", "deserializ", "invalid xml", "parse", "element")):
        print("  XML schema error - format still needs fixing [ERR]")
        print("  Check the raw response above for the exact field name.")
    elif any(w in b for w in ("authentication", "unauthorized", "401", "permission", "access")):
        print("  Auth/permission error - AddOrder may not be active yet [ERR]")
        print("  Confirm with Ting that the Web Services permission was saved.")
    elif "faultstring" in b or "fault" in b:
        print("  SOAP fault returned - check raw response above for faultstring.")
    else:
        print("  Unknown error - read the raw response above.")
    print("-------------------------------------------------------")

except Exception as exc:
    print(f"[!!] Unexpected error: {type(exc).__name__}: {exc}")
