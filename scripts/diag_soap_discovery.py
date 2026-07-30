"""
diag_soap_discovery.py — one-off diagnostic (READ ONLY, no writes).

Explores the VeraCore SOAP surface on Ting's tenant to find an inventory /
product read that does NOT apply the active-offer filter that cripples the
REST /api/GetInventory endpoint.

We already speak SOAP for AddOrder against:
    https://rhu190.veracore.com/pmomsws/order.asmx

This script:
  1. Downloads the WSDL for order.asmx and lists EVERY operation it exposes.
  2. Probes other likely .asmx service names under /pmomsws/ and lists their
     operations too.

Purely GET requests for WSDL documents. No SOAP action is invoked, nothing
is written, no order is created.
"""
import os
import re
import sys
from urllib.parse import urlparse

import httpx

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_veracore_client  # noqa: E402

vc = get_veracore_client()
host = urlparse(vc.base_url).netloc
print(f"tenant host: {host}")
print(f"known soap url: {vc.soap_url}\n")

CANDIDATES = [
    "order", "inventory", "product", "products", "offer", "offers",
    "shipping", "shipment", "customer", "mailer", "receiving",
    "purchaseorder", "report", "reports", "general", "service",
]

client = httpx.Client(timeout=30.0, follow_redirects=True)

OP_RE = re.compile(r'<(?:wsdl:)?operation name="([^"]+)"')


def probe(name):
    url = f"https://{host}/pmomsws/{name}.asmx?wsdl"
    try:
        r = client.get(url)
    except Exception as e:
        return name, None, f"network error: {type(e).__name__}"
    if r.status_code != 200:
        return name, None, f"HTTP {r.status_code}"
    if "wsdl" not in r.text[:2000].lower() and "<definitions" not in r.text[:2000].lower():
        return name, None, f"HTTP 200 but not a WSDL ({len(r.text)} bytes)"
    ops = sorted(set(OP_RE.findall(r.text)))
    return name, ops, f"OK ({len(r.text)} bytes)"


found = {}
for name in CANDIDATES:
    nm, ops, note = probe(name)
    flag = "  <== SERVICE FOUND" if ops else ""
    print(f"  /pmomsws/{nm}.asmx?wsdl -> {note}{flag}")
    if ops:
        found[nm] = ops

print("\n" + "=" * 70)
print("OPERATIONS PER DISCOVERED SERVICE")
print("=" * 70)
for nm, ops in found.items():
    print(f"\n--- {nm}.asmx  ({len(ops)} operations) ---")
    for o in ops:
        star = ""
        low = o.lower()
        if any(k in low for k in ("inventor", "product", "offer", "balance", "stock", "quantity")):
            star = "   <<< INVENTORY/PRODUCT RELATED"
        print(f"    {o}{star}")

print("\n" + "=" * 70)
print("SUMMARY OF CANDIDATE INVENTORY READS")
print("=" * 70)
for nm, ops in found.items():
    for o in ops:
        low = o.lower()
        if any(k in low for k in ("inventor", "product", "offer", "balance", "stock", "quantity")):
            print(f"    {nm}.asmx :: {o}")
