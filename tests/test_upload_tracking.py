"""
Threads 20/21 — /decisions/upload-tracking behaviour against an in-memory fake DB and fake
Shopify / Cratejoy clients (no network, no real DB). Reproduces the production incident:
re-uploading the same Pirate Ship tracking file must NOT mark any other box shipped.

Run:  OBB_DISABLE_SCHEDULER=1 python -m pytest tests/test_upload_tracking.py -v
"""

import asyncio
import copy
import os
import sys
from datetime import datetime, timedelta

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as appmod  # noqa: E402
from starlette.requests import Request  # noqa: E402


# ─────────────────────────── fakes ───────────────────────────

class _Res:
    def __init__(self, data):
        self.data = data


class _Query:
    def __init__(self, db, table):
        self.db, self.table, self.filters, self._limit = db, table, [], None
        self._update, self._embed = None, False

    def select(self, cols="*", **_):
        self._embed = "customers(" in cols
        return self

    def update(self, data):
        self._update = data
        return self

    def _f(self, fn):
        self.filters.append(fn)
        return self

    def eq(self, k, v):     return self._f(lambda r: str(r.get(k)) == str(v))
    def gte(self, k, v):    return self._f(lambda r: r.get(k) is not None and str(r.get(k)) >= str(v))
    def lte(self, k, v):    return self._f(lambda r: r.get(k) is not None and str(r.get(k)) <= str(v))
    def in_(self, k, vs):   return self._f(lambda r: r.get(k) in vs)
    def ilike(self, k, v):  return self._f(lambda r: str(r.get(k) or "").lower() == str(v).lower())
    def is_(self, k, v):    return self._f(lambda r: r.get(k) is None) if v == "null" else self

    def limit(self, n):
        self._limit = n
        return self

    def order(self, *_a, **_k):
        return self

    def execute(self):
        rows = [r for r in self.db.tables[self.table] if all(f(r) for f in self.filters)]
        if self._update is not None:
            for r in rows:
                r.update(self._update)
            return _Res(copy.deepcopy(rows))
        if self._limit:
            rows = rows[: self._limit]
        out = copy.deepcopy(rows)
        if self._embed:
            custs = {c["id"]: c for c in self.db.tables["customers"]}
            for r in out:
                r["customers"] = copy.deepcopy(custs.get(r.get("customer_id")) or {})
        return _Res(out)


class FakeDB:
    def __init__(self, customers, decisions):
        self.tables = {"customers": customers, "decisions": decisions}

    def table(self, name):
        return _Query(self, name)


class FakeCratejoy:
    def __init__(self, shipments):
        self.shipments = {str(s["id"]): s for s in shipments}
        self.add_calls = []

    def get_shipment(self, sid):
        return copy.deepcopy(self.shipments[str(sid)])

    def list_customer_shipments(self, cj_cid):
        return [copy.deepcopy(s) for s in self.shipments.values() if s["customer_id"] == cj_cid]

    def add_tracking(self, shipment_id, tracking_number, carrier=None, tracking_url=None, shipped_at=None):
        self.add_calls.append((str(shipment_id), tracking_number))
        s = self.shipments[str(shipment_id)]
        s["status"], s["tracking_number"] = "shipped", tracking_number
        return s


class FakeShopify:
    def __init__(self):
        self.fulfilled = {}
        self.calls = []

    def find_order_id_by_name(self, name):
        return None

    def fulfill_order(self, order_id, tracking_number, carrier=None, tracking_url=None, notify_customer=True):
        self.calls.append((order_id, tracking_number))
        if order_id in self.fulfilled:
            return {"status": "already", "order_id": order_id}
        self.fulfilled[order_id] = tracking_number
        return {"status": "fulfilled", "order_id": order_id}


# ─────────────────────────── harness ───────────────────────────

def _recent():
    return (datetime.utcnow() - timedelta(hours=2)).isoformat()


def _box(sid, sub, target, status="unshipped", cj_cid="7093449651"):
    return {"id": int(sid), "status": status, "target_at": target, "customer_id": cj_cid,
            "created_at": "2026-09-09T16:13:57-07:00", "tracking_number": None,
            "fulfillments": [{"subscription_id": int(sub)}]}


def _upload(monkeypatch, db, cj, sc, csv_text):
    monkeypatch.setattr(appmod, "get_supabase", lambda: db)
    monkeypatch.setattr(appmod, "get_cratejoy_client", lambda: cj)
    monkeypatch.setattr(appmod, "get_shopify_client", lambda: sc)

    async def _no_log(*a, **k):
        return None
    monkeypatch.setattr(appmod, "log_activity", _no_log)

    boundary = "XBOUNDARY"
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"t.csv\"\r\n"
            f"Content-Type: text/csv\r\n\r\n{csv_text}\r\n--{boundary}--\r\n").encode()
    scope = {"type": "http", "method": "POST", "path": "/decisions/upload-tracking", "headers": [
        (b"content-type", f"multipart/form-data; boundary={boundary}".encode()),
        (b"content-length", str(len(body)).encode())], "query_string": b""}
    sent = {"done": False}

    async def receive():
        if sent["done"]:
            return {"type": "http.disconnect"}
        sent["done"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    resp = asyncio.run(appmod.upload_tracking(Request(scope, receive)))
    import json
    return json.loads(resp.body)


def _bednarfive_world():
    """Leah's box (decision knows its shipment) + Lana's subscription, all boxes still unshipped."""
    customers = [{"id": "c-leah", "email": "bednarfive@gmail.com", "first_name": "Susan", "last_name": "Bednarczyk",
                  "cratejoy_customer_id": "7093449651"}]
    decisions = [{"id": "0069318c-03cb-4489-bfeb-5bc1f8509d58", "customer_id": "c-leah", "platform": "cratejoy",
                  "status": "shipped", "order_id": "7093457035", "cratejoy_shipment_id": "7093457045",
                  "ship_first_name": "Leah", "ship_last_name": "Hudson", "tracking_number": None,
                  "updated_at": _recent()}]
    boxes = [_box(7093457045, 7093457035, "2026-09-09T23:08:36Z"),   # Leah Sept
             _box(7093457046, 7093457035, "2026-10-01T07:00:00Z"),   # Leah Oct
             _box(7093458562, 7093458554, "2026-09-09T23:13:57Z"),   # Lana Sept
             _box(7093458563, 7093458554, "2026-10-01T07:00:00Z"),   # Lana Oct
             _box(7093458564, 7093458554, "2026-11-01T07:00:00Z")]   # Lana Nov
    return FakeDB(customers, decisions), FakeCratejoy(boxes)


CSV_HEADER = "Name,Email,Tracking Number\n"


# ─────────────────────────── tests ───────────────────────────

def test_reupload_same_file_five_times_marks_only_the_processed_box(monkeypatch):
    db, cj = _bednarfive_world()
    csv_text = CSV_HEADER + "Leah Hudson,bednarfive@gmail.com,9334611043900211332504\n"
    first = _upload(monkeypatch, db, cj, FakeShopify(), csv_text)
    assert first["summary"]["fulfilled"] == 1
    assert cj.add_calls == [("7093457045", "9334611043900211332504")]   # Leah's exact box
    for _ in range(4):                                                    # the 9/14 incident
        again = _upload(monkeypatch, db, cj, FakeShopify(), csv_text)
        assert again["summary"]["already"] == 1 and again["summary"]["fulfilled"] == 0
    assert len(cj.add_calls) == 1                                         # no future box touched
    assert all(cj.shipments[s]["status"] == "unshipped"
               for s in ("7093457046", "7093458562", "7093458563", "7093458564"))


def test_cratejoy_decision_without_shipment_id_marks_due_box_never_future(monkeypatch):
    db, cj = _bednarfive_world()
    db.tables["decisions"][0].update({"cratejoy_shipment_id": None, "order_id": "7093458554",
                                      "ship_first_name": "Lana", "ship_last_name": "Bain"})
    out = _upload(monkeypatch, db, cj, FakeShopify(), CSV_HEADER + "Lana Bain,bednarfive@gmail.com,TRK1\n")
    assert out["summary"]["fulfilled"] == 1
    assert cj.add_calls == [("7093458562", "TRK1")]                       # Lana's Sept box
    assert db.tables["decisions"][0]["cratejoy_shipment_id"] == "7093458562"  # linked for next time
    assert db.tables["decisions"][0]["tracking_number"] == "TRK1"


def test_two_open_boxes_same_email_without_name_is_ambiguous_and_touches_nothing(monkeypatch):
    db, cj = _bednarfive_world()
    db.tables["customers"].append({"id": "c-lana", "email": "bednarfive@gmail.com", "first_name": "Lana",
                                   "last_name": "Bain", "cratejoy_customer_id": "7093449651"})
    db.tables["decisions"].append({"id": "11111111-0000-0000-0000-000000000000", "customer_id": "c-lana",
                                   "platform": "cratejoy", "status": "shipped", "order_id": "7093458554",
                                   "cratejoy_shipment_id": "7093458562", "ship_first_name": "Lana",
                                   "ship_last_name": "Bain", "tracking_number": None, "updated_at": _recent()})
    out = _upload(monkeypatch, db, cj, FakeShopify(), "Email,Tracking Number\nbednarfive@gmail.com,TRK9\n")
    assert out["summary"]["ambiguous"] == 1
    assert cj.add_calls == []
    # …and with the recipient name it goes to exactly the right one
    out = _upload(monkeypatch, db, cj, FakeShopify(), CSV_HEADER + "Lana Bain,bednarfive@gmail.com,TRK9\n")
    assert out["summary"]["fulfilled"] == 1 and cj.add_calls == [("7093458562", "TRK9")]


def test_shopify_row_uses_decision_order_and_reupload_is_noop(monkeypatch):
    customers = [{"id": "c-n", "email": "nicgoldstein@goldsteinus.net", "first_name": "Nicole", "last_name": "Goldstein"}]
    decisions = [{"id": "68ffd1be-569d-4c29-b955-7847955567bf", "customer_id": "c-n", "platform": "shopify",
                  "status": "shipped", "order_id": "7548199141665", "ship_first_name": "Kailin",
                  "ship_last_name": "Goldstein", "tracking_number": None, "updated_at": _recent()}]
    db, sc = FakeDB(customers, decisions), FakeShopify()
    csv_text = CSV_HEADER + "Kailin Goldstein,nicgoldstein@goldsteinus.net,9400TRK\n"
    assert _upload(monkeypatch, db, None, sc, csv_text)["summary"]["fulfilled"] == 1
    assert sc.calls == [("7548199141665", "9400TRK")]
    assert _upload(monkeypatch, db, None, sc, csv_text)["summary"]["already"] == 1
    assert len(sc.calls) == 1                                             # never re-sent


def test_obb_ref_in_order_id_column_matches_exact_box(monkeypatch):
    db, cj = _bednarfive_world()
    out = _upload(monkeypatch, db, cj, FakeShopify(),
                  "Order ID,Tracking Number\nOBB-0069318c,TRK-REF\n")
    assert out["summary"]["fulfilled"] == 1
    assert cj.add_calls == [("7093457045", "TRK-REF")]


def test_old_box_outside_window_is_unmatched(monkeypatch):
    db, cj = _bednarfive_world()
    db.tables["decisions"][0]["updated_at"] = (datetime.utcnow() - timedelta(days=40)).isoformat()
    out = _upload(monkeypatch, db, cj, FakeShopify(), CSV_HEADER + "Leah Hudson,bednarfive@gmail.com,TRKOLD\n")
    assert out["summary"]["unmatched"] == 1 and cj.add_calls == []


def test_tracking_url_column_is_not_mistaken_for_tracking_number(monkeypatch):
    db, cj = _bednarfive_world()
    out = _upload(monkeypatch, db, cj, FakeShopify(),
                  "Tracking URL,Name,Email,Tracking Number\nhttps://t/x,Leah Hudson,bednarfive@gmail.com,TRKU\n")
    assert cj.add_calls == [("7093457045", "TRKU")] and out["summary"]["fulfilled"] == 1
