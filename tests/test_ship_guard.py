"""
Threads 20/21 — J2 / B8: guard against double shipment history when marking a decision
shipped. Nicole's duplicate: a manual shipment row already existed for the same customer +
kit this week, then `ship_decision` created a SECOND shipments row for the same box. The
"create a new shipment" path (only) in `ship_decision` and the bulk `ship` branch now reuse an
existing shipment for the same customer_id + kit_sku with ship_date within
±SHIP_DUPLICATE_WINDOW_DAYS instead of inserting again.

Run:  OBB_DISABLE_SCHEDULER=1 python -m pytest tests/test_ship_guard.py -v
"""

import asyncio
import copy
import itertools
import os
import sys
from datetime import date, timedelta

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as appmod  # noqa: E402

_ids = itertools.count(1)


# ─────────────────────────── fake DB (pattern copied from tests/test_recipient_profiles.py) ───────────────────────────

class _Res:
    def __init__(self, data):
        self.data = data


class _Q:
    def __init__(self, db, table):
        self.db, self.t, self.f, self.lim = db, table, [], None
        self.op, self.payload, self.desc, self.okey, self._single = "select", None, False, None, False

    def select(self, *_a, **_k): return self
    def insert(self, data): self.op, self.payload = "insert", data; return self
    def update(self, data): self.op, self.payload = "update", data; return self
    def _add(self, fn): self.f.append(fn); return self
    def eq(self, k, v): return self._add(lambda r: str(r.get(k)) == str(v))
    def gte(self, k, v): return self._add(lambda r: r.get(k) is not None and str(r.get(k)) >= str(v))
    def lte(self, k, v): return self._add(lambda r: r.get(k) is not None and str(r.get(k)) <= str(v))
    def in_(self, k, vs): return self._add(lambda r: r.get(k) in vs)
    def ilike(self, k, v): return self._add(lambda r: str(v).lower().strip("%") in str(r.get(k) or "").lower())
    def is_(self, k, v): return self._add(lambda r: r.get(k) is None) if v == "null" else self

    def limit(self, n): self.lim = n; return self
    def order(self, k, desc=False): self.okey, self.desc = k, desc; return self
    def single(self): self._single = True; return self

    def execute(self):
        rows = self.db.tables.setdefault(self.t, [])
        if self.op == "insert":
            recs = self.payload if isinstance(self.payload, list) else [self.payload]
            out = []
            for rec in recs:
                rec = dict(rec)
                rec.setdefault("id", f"{self.t[:4]}-{next(_ids):04d}")
                rows.append(rec)
                out.append(copy.deepcopy(rec))
            return _Res(out if not self._single else (out[0] if out else None))
        hit = [r for r in rows if all(fn(r) for fn in self.f)]
        if self.op == "update":
            for r in hit:
                r.update(self.payload)
            out = copy.deepcopy(hit)
            return _Res(out if not self._single else (out[0] if out else None))
        if self.okey:
            hit = sorted(hit, key=lambda r: str(r.get(self.okey) or ""), reverse=self.desc)
        if self.lim:
            hit = hit[: self.lim]
        out = copy.deepcopy(hit)
        return _Res(out if not self._single else (out[0] if out else None))


class FakeDB:
    def __init__(self, **tables):
        self.tables = {k: [dict(r) for r in v] for k, v in tables.items()}

    def table(self, name):
        return _Q(self, name)


class _FakeRequest:
    """ship_decision only reads request.headers.get('referer', ...) — no real ASGI needed."""
    def __init__(self):
        self.headers = {}


def _no_sheet_sync(monkeypatch):
    monkeypatch.setattr(appmod, "update_decision_status_in_sheet", lambda **k: None)


# ─────────────────────────── _find_recent_shipment_for_kit (pure-ish DB helper) ───────────────────────────

def test_find_recent_shipment_matches_same_customer_and_kit_within_window():
    today = date.today()
    db = FakeDB(shipments=[
        {"id": "ship-1", "customer_id": "cust-1", "kit_sku": "WK21",
         "ship_date": (today - timedelta(days=3)).isoformat(), "notes": "Manual entry"},
    ])
    got = appmod._find_recent_shipment_for_kit(db, "cust-1", "WK21")
    assert got == "ship-1"


def test_find_recent_shipment_ignores_different_kit_sku():
    today = date.today()
    db = FakeDB(shipments=[
        {"id": "ship-1", "customer_id": "cust-1", "kit_sku": "OTHER-SKU",
         "ship_date": today.isoformat(), "notes": "Manual entry"},
    ])
    assert appmod._find_recent_shipment_for_kit(db, "cust-1", "WK21") is None


def test_find_recent_shipment_ignores_shipment_outside_window():
    today = date.today()
    db = FakeDB(shipments=[
        {"id": "ship-1", "customer_id": "cust-1", "kit_sku": "WK21",
         "ship_date": (today - timedelta(days=30)).isoformat(), "notes": "Old shipment, unrelated"},
    ])
    assert appmod._find_recent_shipment_for_kit(db, "cust-1", "WK21") is None


def test_find_recent_shipment_skips_lookup_with_no_kit_sku():
    db = FakeDB(shipments=[
        {"id": "ship-1", "customer_id": "cust-1", "kit_sku": None, "ship_date": date.today().isoformat()},
    ])
    assert appmod._find_recent_shipment_for_kit(db, "cust-1", None) is None


# ─────────────────────────── ship_decision — end to end against the fake DB ───────────────────────────

def _nicole_world():
    """Nicole's box, already approved, plus a MANUAL shipment row already on file for the same
    kit this week — the exact shape of the production duplicate (B8)."""
    today = date.today()
    return FakeDB(
        decisions=[{"id": "68ffd1be-569d-4c29-b955-7847955567bf", "customer_id": "cust-nicole",
                    "status": "approved", "kit_id": None, "kit_sku": "WK21", "platform": "shopify",
                    "order_id": "7548199141665", "trimester": 3}],
        customers=[{"id": "cust-nicole", "email": "nicgoldstein@goldsteinus.net",
                    "first_name": "Nicole", "last_name": "Goldstein", "subscription_status": "active"}],
        shipments=[{"id": "862f794d-5f02-44bf-8edf-0bf3f819acb2", "customer_id": "cust-nicole",
                    "kit_sku": "WK21", "ship_date": (today - timedelta(days=2)).isoformat(),
                    "notes": "Manual entry — added by Sheena"}],
        activity_log=[],
    )


def test_ship_decision_reuses_existing_shipment_instead_of_duplicating(monkeypatch):
    db = _nicole_world()
    monkeypatch.setattr(appmod, "get_supabase", lambda: db)
    _no_sheet_sync(monkeypatch)

    asyncio.run(appmod.ship_decision(_FakeRequest(), "68ffd1be-569d-4c29-b955-7847955567bf"))

    assert len(db.tables["shipments"]) == 1, "B8 guard must not insert a second shipment row"
    reused = db.tables["shipments"][0]
    assert reused["id"] == "862f794d-5f02-44bf-8edf-0bf3f819acb2"
    assert reused["ship_date"] == date.today().isoformat()
    dec = next(d for d in db.tables["decisions"] if d["id"] == "68ffd1be-569d-4c29-b955-7847955567bf")
    assert dec["status"] == "shipped"


def test_ship_decision_still_creates_a_shipment_when_none_exists(monkeypatch):
    db = _nicole_world()
    db.tables["shipments"] = []  # no pre-existing shipment this time
    monkeypatch.setattr(appmod, "get_supabase", lambda: db)
    _no_sheet_sync(monkeypatch)

    asyncio.run(appmod.ship_decision(_FakeRequest(), "68ffd1be-569d-4c29-b955-7847955567bf"))

    assert len(db.tables["shipments"]) == 1
    dec = next(d for d in db.tables["decisions"] if d["id"] == "68ffd1be-569d-4c29-b955-7847955567bf")
    assert dec["status"] == "shipped"


def test_ship_decision_does_not_reuse_a_different_customers_shipment(monkeypatch):
    db = _nicole_world()
    db.tables["shipments"][0]["customer_id"] = "someone-else"
    monkeypatch.setattr(appmod, "get_supabase", lambda: db)
    _no_sheet_sync(monkeypatch)

    asyncio.run(appmod.ship_decision(_FakeRequest(), "68ffd1be-569d-4c29-b955-7847955567bf"))

    # The "someone else" shipment is untouched, a NEW one was created for Nicole's box.
    assert len(db.tables["shipments"]) == 2
    nicole_ships = [s for s in db.tables["shipments"] if s["customer_id"] == "cust-nicole"]
    assert len(nicole_ships) == 1
