#!/usr/bin/env python3
"""
test_soap.py  —  Quick SOAP AddOrder validation against TSTSHM.

Tries 4 XML variants to determine if the "Ship To Key" error is our
XML or VeraCore admin config:

  Variant A — current: <ShipToKey>0</ShipToKey> in OfferOrdered, Flag=OrderedBy in ShipTo
  Variant B — no ShipToKey in OfferOrdered at all (it's minOccurs=0 per WSDL)
  Variant C — full address in OrderShipTo (no Flag), ShipToKey=1
  Variant D — no ShipTo block at all

If all 4 give the SAME "does not have a Ship To Key" error → admin config issue.
If any gives a different error → XML issue we can fix.

Run from the project root:
    cd "c:/Users/hasan/Desktop/opus tin/opus-obb-prototype"
    python test_soap.py
"""

import os
import sys
import logging
import httpx
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as _xml_escape

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

SOAP_URL  = os.environ.get("VERACORE_SOAP_URL", "https://rhu190.veracore.com/pmomsws/order.asmx")
USERNAME  = os.environ["VERACORE_USER_ID"]
PASSWORD  = os.environ["VERACORE_PASSWORD"]

def e(v):
    return _xml_escape(str(v or ""))

SHIP = {
    "first": "Jane",
    "last": "TestSmith",
    "address1": "123 Test Street",
    "city": "Austin",
    "state": "TX",
    "zip": "78701",
    "country": "US",
    "phone": "512-555-0001",
}

def _envelope(order_id, offers_xml, ship_to_xml, source=None):
    source_xml = f"          <Source>{e(source)}</Source>\n" if source else ""
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <AuthenticationHeader xmlns="http://sma-promail/">
      <Username>{e(USERNAME)}</Username>
      <Password>{e(PASSWORD)}</Password>
    </AuthenticationHeader>
  </soap:Header>
  <soap:Body>
    <AddOrder xmlns="http://sma-promail/">
      <order>
        <Header>
          <ID>{e(order_id)}</ID>
{source_xml}          <Comments>SOAP format test - do not fulfill</Comments>
        </Header>
        <OrderedBy>
          <FirstName>{e(SHIP["first"])}</FirstName>
          <LastName>{e(SHIP["last"])}</LastName>
          <Address1>{e(SHIP["address1"])}</Address1>
          <City>{e(SHIP["city"])}</City>
          <State>{e(SHIP["state"])}</State>
          <PostalCode>{e(SHIP["zip"])}</PostalCode>
          <Country>{e(SHIP["country"])}</Country>
          <Phone>{e(SHIP["phone"])}</Phone>
        </OrderedBy>
        {ship_to_xml}
        <Offers>
          {offers_xml}
        </Offers>
      </order>
    </AddOrder>
  </soap:Body>
</soap:Envelope>"""


# Variant A: current approach — ShipToKey=0 + Flag=OrderedBy
XML_A = _envelope(
    "TOBB-TST-A",
    """<OfferOrdered>
          <Offer><Header><ID>OBB-BQ-11 Kits</ID></Header></Offer>
          <Quantity>1</Quantity>
          <ShipToKey>0</ShipToKey>
        </OfferOrdered>""",
    """<ShipTo>
          <OrderShipTo>
            <Key>0</Key>
            <Flag>OrderedBy</Flag>
          </OrderShipTo>
        </ShipTo>""",
)

# Variant B: no ShipToKey in OfferOrdered (it's optional per WSDL)
XML_B = _envelope(
    "TOBB-TST-B",
    """<OfferOrdered>
          <Offer><Header><ID>OBB-BQ-11 Kits</ID></Header></Offer>
          <Quantity>1</Quantity>
        </OfferOrdered>""",
    """<ShipTo>
          <OrderShipTo>
            <Key>0</Key>
            <Flag>OrderedBy</Flag>
          </OrderShipTo>
        </ShipTo>""",
)

# Variant C: full address in OrderShipTo (no Flag), ShipToKey=1
XML_C = _envelope(
    "TOBB-TST-C",
    """<OfferOrdered>
          <Offer><Header><ID>OBB-BQ-11 Kits</ID></Header></Offer>
          <Quantity>1</Quantity>
          <ShipToKey>1</ShipToKey>
        </OfferOrdered>""",
    f"""<ShipTo>
          <OrderShipTo>
            <Key>1</Key>
            <FirstName>{e(SHIP["first"])}</FirstName>
            <LastName>{e(SHIP["last"])}</LastName>
            <Address1>{e(SHIP["address1"])}</Address1>
            <City>{e(SHIP["city"])}</City>
            <State>{e(SHIP["state"])}</State>
            <PostalCode>{e(SHIP["zip"])}</PostalCode>
            <Country>{e(SHIP["country"])}</Country>
            <Phone>{e(SHIP["phone"])}</Phone>
          </OrderShipTo>
        </ShipTo>""",
)

# Variant D: no ShipTo block at all
XML_D = _envelope(
    "TOBB-TST-D",
    """<OfferOrdered>
          <Offer><Header><ID>OBB-BQ-11 Kits</ID></Header></Offer>
          <Quantity>1</Quantity>
        </OfferOrdered>""",
    "",
)

# Variant E: same as C (full address, ShipToKey=1) BUT with <Source>OBB</Source> in Header
# Research finding: VeraCore requires a Source Code in Utilities > Source Codes;
# without it, the offer can't resolve a Ship To Key regardless of carrier setup.
XML_E = _envelope(
    "TOBB-TST-E",
    """<OfferOrdered>
          <Offer><Header><ID>OBB-BQ-11 Kits</ID></Header></Offer>
          <Quantity>1</Quantity>
          <ShipToKey>1</ShipToKey>
        </OfferOrdered>""",
    f"""<ShipTo>
          <OrderShipTo>
            <Key>1</Key>
            <FirstName>{e(SHIP["first"])}</FirstName>
            <LastName>{e(SHIP["last"])}</LastName>
            <Address1>{e(SHIP["address1"])}</Address1>
            <City>{e(SHIP["city"])}</City>
            <State>{e(SHIP["state"])}</State>
            <PostalCode>{e(SHIP["zip"])}</PostalCode>
            <Country>{e(SHIP["country"])}</Country>
            <Phone>{e(SHIP["phone"])}</Phone>
          </OrderShipTo>
        </ShipTo>""",
    source="OBB",
)

# Variant F: same as E but source = "OBB USPS" (matches the carrier Brian just set up)
XML_F = _envelope(
    "TOBB-TST-F",
    """<OfferOrdered>
          <Offer><Header><ID>OBB-BQ-11 Kits</ID></Header></Offer>
          <Quantity>1</Quantity>
          <ShipToKey>1</ShipToKey>
        </OfferOrdered>""",
    f"""<ShipTo>
          <OrderShipTo>
            <Key>1</Key>
            <FirstName>{e(SHIP["first"])}</FirstName>
            <LastName>{e(SHIP["last"])}</LastName>
            <Address1>{e(SHIP["address1"])}</Address1>
            <City>{e(SHIP["city"])}</City>
            <State>{e(SHIP["state"])}</State>
            <PostalCode>{e(SHIP["zip"])}</PostalCode>
            <Country>{e(SHIP["country"])}</Country>
            <Phone>{e(SHIP["phone"])}</Phone>
          </OrderShipTo>
        </ShipTo>""",
    source="OBB USPS",
)


def send(label, xml):
    print(f"\n{'='*60}")
    print(f"VARIANT {label}")
    print("="*60)
    try:
        r = httpx.post(
            SOAP_URL,
            content=xml.encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": '"http://sma-promail/AddOrder"',
            },
            timeout=30,
        )
        body = r.text
        print(f"HTTP {r.status_code}")
        try:
            root = ET.fromstring(body)
            fault = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Fault")
            if fault is not None:
                fs = fault.find("faultstring")
                detail = fault.find("detail")
                print(f"SOAP FAULT: {fs.text if fs is not None else '(no faultstring)'}")
                if detail is not None:
                    print(f"DETAIL: {ET.tostring(detail, encoding='unicode')[:600]}")
            else:
                seq = root.find(".//OrderSeqID")
                oid = root.find(".//OrderID")
                print(f"SUCCESS! OrderSeqID={seq.text if seq else '?'} OrderID={oid.text if oid else '?'}")
        except ET.ParseError:
            print(f"RAW (non-XML): {body[:400]}")
    except Exception as ex:
        print(f"EXCEPTION: {ex}")


# Variant G: official sample format — <OrderShipToKey><Key>0</Key></OrderShipToKey>
# The official VeraCore AddOrder PDF sample call uses this, not <ShipToKey>
XML_G = _envelope(
    "TOBB-TST-G2",
    """<OfferOrdered>
          <Offer><Header><ID>OBB-BQ-11 Kits</ID></Header></Offer>
          <Quantity>1</Quantity>
          <OrderShipToKey>
            <Key>0</Key>
          </OrderShipToKey>
        </OfferOrdered>""",
    f"""<ShipTo>
          <OrderShipTo>
            <Key>0</Key>
            <FirstName>{e(SHIP["first"])}</FirstName>
            <LastName>{e(SHIP["last"])}</LastName>
            <Address1>{e(SHIP["address1"])}</Address1>
            <City>{e(SHIP["city"])}</City>
            <State>{e(SHIP["state"])}</State>
            <PostalCode>{e(SHIP["zip"])}</PostalCode>
            <Country>{e(SHIP["country"])}</Country>
            <Phone>{e(SHIP["phone"])}</Phone>
            <Flag>Other</Flag>
          </OrderShipTo>
        </ShipTo>""",
)

send("A — ShipToKey=0 + Flag=OrderedBy (current)", XML_A)
send("B — no ShipToKey in OfferOrdered", XML_B)
send("C — full address in OrderShipTo, ShipToKey=1", XML_C)
send("D — no ShipTo block at all", XML_D)
send("E — full address + ShipToKey=1 + Source=OBB", XML_E)
send("F — full address + ShipToKey=1 + Source=OBB USPS", XML_F)
send("G — official sample format: OrderShipToKey+Key=0, Flag=Other", XML_G)

print("\n\n-- INTERPRETATION --")
print("A-D same 'does not have a Ship To Key' -> missing Source Code config in VeraCore.")
print("E or F different error / success -> Source element is relevant.")
print("E or F SUCCESS -> ask Brian to add that Source Code in Utilities > Source Codes.")
print("All 6 same error -> Brian needs to assign Ship To Key to OBB offers in the offer editor.")
print("--------------------------------------------------------------------")
