"""Webhook Replay must never create a duplicate box, and must not run non-order Shopify payloads
through the order path (2026-10-01: replaying the Oct 1 failures by hand showed both traps)."""
import asyncio
import os
import sys
from datetime import date

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402


class _Q:
    def __init__(self, db, name):
        self.db, self.name, self.filters, self.op, self.payload, self.one = db, name, [], "select", None, False

    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self.filters.append(lambda r: str(r.get(col)) == str(val))
        return self

    def ilike(self, col, val):
        v = str(val).strip("%").lower()
        self.filters.append(lambda r: v in str(r.get(col) or "").lower())
        return self

    def in_(self, col, vals):
        self.filters.append(lambda r: r.get(col) in set(vals))
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def single(self):
        self.one = True
        return self

    def insert(self, payload):
        self.op, self.payload = "insert", payload
        return self

    def update(self, payload):
        self.op, self.payload = "update", payload
        return self

    def execute(self):
        rows = self.db.tables.setdefault(self.name, [])
        if self.op == "insert":
            new = [dict(x) for x in (self.payload if isinstance(self.payload, list) else [self.payload])]
            for n, x in enumerate(new):
                x.setdefault("id", f"{self.name}-{len(rows) + n}")
            rows.extend(new)
            self.db.inserts.setdefault(self.name, []).extend(new)
            return type("R", (), {"data": new})()
        hit = [r for r in rows if all(f(r) for f in self.filters)]
        if self.op == "update":
            for r in hit:
                r.update(self.payload)
        data = (hit[0] if hit else None) if self.one else hit
        return type("R", (), {"data": data})()


class FakeDB:
    def __init__(self, tables):
        self.tables, self.inserts = tables, {}

    def table(self, name):
        return _Q(self, name)


def _setup(monkeypatch, db):
    monkeypatch.setattr(app, "get_supabase", lambda: db)
    monkeypatch.setattr(app, "write_decision_to_sheet", lambda *a, **k: None)


def test_base_event_type_strips_replay_prefixes():
    assert app._base_event_type("replay_replay_orders/create") == "orders/create"
    assert app._base_event_type("customers/update") == "customers/update"
    assert app._base_event_type("") == ""


def test_existing_box_shopify_any_decision_blocks():
    db = FakeDB({"decisions": [{"id": "d1", "order_id": "111", "status": "rejected", "ship_date": "2026-05-01"}]})
    assert app._replay_existing_box(db, "111", "shopify")["id"] == "d1"
    assert app._replay_existing_box(db, "222", "shopify") is None


def test_existing_box_cratejoy_only_open_or_this_month_blocks():
    this_month = date.today().isoformat()[:7] + "-01"
    old = {"id": "d1", "order_id": "S1", "status": "shipped", "ship_date": "2026-01-01"}
    assert app._replay_existing_box(FakeDB({"decisions": [old]}), "S1", "cratejoy") is None      # last month's box
    open_box = {"id": "d2", "order_id": "S1", "status": "pending", "ship_date": "2026-01-01"}
    assert app._replay_existing_box(FakeDB({"decisions": [old, open_box]}), "S1", "cratejoy")["id"] == "d2"
    shipped_now = {"id": "d3", "order_id": "S1", "status": "shipped", "ship_date": this_month}
    assert app._replay_existing_box(FakeDB({"decisions": [shipped_now]}), "S1", "cratejoy")["id"] == "d3"


def test_replay_refuses_customer_update_payload(monkeypatch):
    db = FakeDB({"webhook_logs": [{"id": "w1", "source": "shopify", "event_type": "customers/update", "status": "failed",
                                    "event_id": "e1", "payload": {"id": 555, "email": "a@x.com",
                                                                  "default_address": {"address1": "1 Main"}}}],
                 "decisions": [], "customers": []})
    _setup(monkeypatch, db)
    asyncio.run(app.replay_webhook("w1"))
    assert not db.inserts.get("decisions") and not db.inserts.get("customers")
    replay = [r for r in db.tables["webhook_logs"] if r["id"] != "w1"][0]
    assert replay["status"] == "failed" and "Only Shopify order webhooks" in replay["error_message"]


def test_replay_of_order_that_already_has_a_box_creates_no_new_box(monkeypatch):
    payload = {"id": 777, "name": "#OBB-1", "email": "b@x.com", "total_price": "0", "source_name": "subscription_contract",
               "customer": {"id": 9, "email": "b@x.com", "first_name": "B", "last_name": "X"},
               "shipping_address": {"first_name": "B", "last_name": "X", "address1": "2 Main", "zip": "1"},
               "line_items": [{"sku": "OBB-SUBPLAN-3"}], "note_attributes": []}
    db = FakeDB({"webhook_logs": [{"id": "w2", "source": "shopify", "event_type": "orders/create", "status": "failed",
                                    "event_id": "e2", "payload": payload}],
                 "decisions": [{"id": "dx", "order_id": "777", "status": "pending", "customer_id": "c1"}],
                 "customers": [{"id": "c1", "email": "b@x.com"}]})
    _setup(monkeypatch, db)
    monkeypatch.setattr(app, "upsert_shopify_recipient_profile", lambda db, **k: ("c1", None, "update"))
    monkeypatch.setattr(app, "_order_cancelled_at", lambda db, oid: None)

    async def no_assign(*_a, **_k):
        raise AssertionError("assign_kit must not run for an order that already has a box")
    monkeypatch.setattr(app, "assign_kit", no_assign)
    asyncio.run(app.replay_webhook("w2"))
    assert not db.inserts.get("decisions")
    assert len([d for d in db.tables["decisions"] if d["order_id"] == "777"]) == 1
    assert any("already has a box" in a["summary"] for a in db.tables.get("activity_log", []))  # the guard ran


def test_control_replay_of_order_without_a_box_still_creates_one(monkeypatch):
    payload = {"id": 888, "name": "#OBB-2", "email": "c@x.com", "total_price": "0", "source_name": "subscription_contract",
               "customer": {"id": 9, "email": "c@x.com", "first_name": "C", "last_name": "X"},
               "shipping_address": {"first_name": "C", "last_name": "X", "address1": "3 Main", "zip": "1"},
               "line_items": [{"sku": "OBB-SUBPLAN-3"}], "note_attributes": []}
    db = FakeDB({"webhook_logs": [{"id": "w3", "source": "shopify", "event_type": "orders/create", "status": "failed",
                                    "event_id": "e3", "payload": payload}],
                 "decisions": [], "customers": [{"id": "c2", "email": "c@x.com"}]})
    _setup(monkeypatch, db)
    monkeypatch.setattr(app, "upsert_shopify_recipient_profile", lambda db, **k: ("c2", None, "update"))
    monkeypatch.setattr(app, "_order_cancelled_at", lambda db, oid: None)
    monkeypatch.setattr(app, "_compute_order_type", lambda db, cid: "renewal")

    async def fake_assign(*_a, **_k):
        return {"decision_type": "needs-curation", "reason": "test", "kit_id": None, "kit_sku": None}
    monkeypatch.setattr(app, "assign_kit", fake_assign)
    asyncio.run(app.replay_webhook("w3"))
    assert [d["order_id"] for d in db.inserts.get("decisions", [])] == ["888"]
