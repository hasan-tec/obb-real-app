"""
Shopify cancellation sync — CANCELLATION_SYNC_PLAN.md §4.

In-memory fake Supabase; no network, no real DB. Shapes mirror the production cases found on
2026-09-26 (26 boxes re-curated onto cancelled orders; #OBB-15078 shipped after its cancel).

Run:  OBB_DISABLE_SCHEDULER=1 python -m pytest tests/test_shopify_cancellation.py -v
"""

import asyncio
import os
import sys
import uuid
from datetime import datetime

import pytest

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")  # never start the background jobs from a test
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as appmod  # noqa: E402


# ─── Minimal fake of the supabase-py query builder (only what these paths use) ───

class _Res:
    def __init__(self, data):
        self.data = data
        self.count = None


class _Not:
    def __init__(self, q):
        self.q = q

    def is_(self, col, _null):
        self.q.filters.append(lambda r, c=col: r.get(c) is not None)
        return self.q


class _Query:
    def __init__(self, db, table, op="select", payload=None):
        self.db, self.table, self.op, self.payload = db, table, op, payload
        self.filters, self._limit, self._order, self._single = [], None, None, False

    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self.filters.append(lambda r, c=col, v=val: r.get(c) == v)
        return self

    def neq(self, col, val):
        self.filters.append(lambda r, c=col, v=val: r.get(c) != v)
        return self

    def in_(self, col, vals):
        self.filters.append(lambda r, c=col, v=tuple(vals): r.get(c) in v)
        return self

    def gte(self, col, val):
        self.filters.append(lambda r, c=col, v=val: (r.get(c) or "") >= v)
        return self

    def is_(self, col, _null):
        self.filters.append(lambda r, c=col: r.get(c) is None)
        return self

    @property
    def not_(self):
        return _Not(self)

    def order(self, col, desc=False):
        self._order = (col, desc)
        return self

    def limit(self, n):
        self._limit = n
        return self

    def single(self):
        self._single = True
        return self

    def execute(self):
        if self.db.fail:
            raise RuntimeError("simulated DB failure")
        rows = self.db.tables.setdefault(self.table, [])
        if self.op == "insert":
            rec = dict(self.payload)
            rec.setdefault("id", str(uuid.uuid4()))
            rec.setdefault("created_at", datetime.utcnow().isoformat())
            rows.append(rec)
            return _Res([dict(rec)])
        if self.op == "update" and self.db.before_update:
            self.db.before_update(self.table, self.payload)
        hits = [r for r in rows if all(f(r) for f in self.filters)]
        if self.op == "update":
            for r in hits:
                r.update(self.payload)
            return _Res([dict(r) for r in hits])
        if self._order:
            col, desc = self._order
            hits = sorted(hits, key=lambda r: r.get(col) or "", reverse=desc)
        if self._limit is not None:
            hits = hits[: self._limit]
        out = [dict(r) for r in hits]
        if self._single:
            return _Res(out[0] if out else None)
        return _Res(out)


class _Table:
    def __init__(self, db, name):
        self.db, self.name = db, name

    def select(self, *_a, **_k):
        return _Query(self.db, self.name)

    def update(self, payload):
        return _Query(self.db, self.name, "update", payload)

    def insert(self, payload):
        return _Query(self.db, self.name, "insert", payload)


class FakeDB:
    def __init__(self):
        self.tables = {"decisions": [], "customers": [], "activity_log": []}
        self.fail = False
        self.before_update = None

    def table(self, name):
        return _Table(self, name)


ORDER = "7537762500897"
CANCELLED_AT = "2026-09-08T18:06:16-07:00"


def _dec(status, order_id=ORDER, customer_id="cust-1", kit="OBB-CQ-31 KITS", **extra):
    row = {"id": str(uuid.uuid4()), "status": status, "order_id": order_id, "customer_id": customer_id,
           "kit_sku": kit, "veracore_order_id": None, "reason": "", "order_cancelled_at": None,
           "created_at": datetime.utcnow().isoformat()}
    row.update(extra)
    return row


@pytest.fixture
def db(monkeypatch):
    fake = FakeDB()
    fake.tables["customers"].append({"id": "cust-1", "email": "a@example.com", "subscription_status": "active",
                                     "first_name": "A", "last_name": "B", "due_date": "2027-01-01",
                                     "clothing_size": "M", "trimester": 2})
    monkeypatch.setattr(appmod, "get_supabase", lambda: fake)  # routes log_activity into the fake too
    return fake


def run(coro):
    return asyncio.run(coro)


def apply(db, source="webhook", **kw):
    return run(appmod._apply_shopify_cancellation(db, ORDER, "#OBB-14670", CANCELLED_AT, "customer", source, **kw))


def warnings(db):
    return [a for a in db.tables["activity_log"] if a["result"] == "warning"]


# ─── reason text ───

def test_reason_starts_with_auto_rejected_and_names_order_date_reason_source():
    text = appmod._shopify_cancel_reason_text("#OBB-14670", CANCELLED_AT, "CUSTOMER", "webhook")
    assert text == "Auto-rejected: Shopify order #OBB-14670 cancelled 2026-09-08 (customer) — webhook."
    assert appmod._shopify_cancel_reason_text(None, None, None, "bulk re-curate").startswith(
        "Auto-rejected: Shopify order cancelled unknown date (no reason given)")


# ─── _apply_shopify_cancellation ───

def test_pending_closed_approved_flagged_shipped_left_and_every_row_stamped(db):
    pending, approved, shipped = _dec("pending"), _dec("approved", veracore_order_id="VC1"), _dec("shipped")
    db.tables["decisions"] += [pending, approved, shipped]

    counts = apply(db)

    assert counts == {"closed": 1, "approved_flagged": 1, "left_alone": 1, "stamped": 3}
    assert pending["status"] == "rejected" and pending["reason"].startswith("Auto-rejected: Shopify order #OBB-14670")
    assert approved["status"] == "approved"
    assert shipped["status"] == "shipped"
    assert all(d["order_cancelled_at"] == CANCELLED_AT for d in (pending, approved, shipped))
    [warn] = warnings(db)
    assert "Approved box" in warn["summary"] and "VeraCore (order VC1)" in warn["detail"]


def test_other_orders_and_customer_status_untouched(db):
    other = _dec("pending", order_id="999")
    db.tables["decisions"] += [_dec("pending"), other]
    apply(db)
    assert other["status"] == "pending" and other["order_cancelled_at"] is None
    assert db.tables["customers"][0]["subscription_status"] == "active"


def test_rerun_is_idempotent_and_approved_warning_deduplicated(db):
    db.tables["decisions"] += [_dec("pending"), _dec("approved")]
    first = apply(db)
    second = apply(db, source="daily reconcile")
    assert first["closed"] == 1 and second["closed"] == 0
    assert second["stamped"] == 0
    assert len(warnings(db)) == 1  # 7-day dedupe
    closed_logs = [a for a in db.tables["activity_log"] if a["summary"].startswith("Closed box")]
    assert len(closed_logs) == 1


def test_conditional_update_never_overwrites_a_box_approved_mid_flight(db):
    row = _dec("pending")
    db.tables["decisions"].append(row)

    def staff_approves_first(table, payload):
        if table == "decisions" and payload.get("status") == "rejected":
            row["status"] = "approved"  # staff clicked Approve between our read and our write
    db.before_update = staff_approves_first

    counts = apply(db)
    assert row["status"] == "approved"
    assert counts["closed"] == 0


def test_dry_run_writes_nothing(db):
    row = _dec("pending")
    db.tables["decisions"].append(row)
    counts = apply(db, dry_run=True)
    assert counts["closed"] == 1
    assert row["status"] == "pending" and row["order_cancelled_at"] is None
    assert db.tables["activity_log"] == []


def test_order_with_no_decisions_is_a_noop(db):
    assert apply(db) == {"closed": 0, "approved_flagged": 0, "left_alone": 0, "stamped": 0}


# ─── kit is not blacklisted ───

def test_cancellation_close_does_not_blacklist_the_kit_but_a_staff_reject_does(db):
    db.tables["decisions"].append(_dec("pending", kit="OBB-CQ-31 KITS"))
    apply(db)
    assert appmod.get_rejected_kit_map(db, "cust-1", []) == {}

    db.tables["decisions"].append(_dec("rejected", order_id="555", kit="OBB-CR-31 KITS", reason="staff said no"))
    assert list(appmod.get_rejected_kit_map(db, "cust-1", [])) == ["OBB-CR-31 KITS"]


# ─── _order_cancelled_at ───

def test_order_cancelled_at_sees_a_shipped_box_and_fails_open(db):
    db.tables["decisions"].append(_dec("shipped", order_cancelled_at=CANCELLED_AT))
    assert appmod._order_cancelled_at(db, ORDER) == CANCELLED_AT
    assert appmod._order_cancelled_at(db, "999") is None
    assert appmod._order_cancelled_at(db, None) is None
    db.fail = True
    assert appmod._order_cancelled_at(db, ORDER) is None


# ─── re-curate guard (the main production failure) ───

def test_recurate_blocked_when_inherited_order_is_cancelled_even_if_its_box_shipped(db, monkeypatch):
    # #OBB-15078 shape: the only box on the order already shipped, then the order was cancelled.
    db.tables["decisions"].append(_dec("shipped", order_cancelled_at=CANCELLED_AT))

    async def must_not_run(*_a, **_k):
        raise AssertionError("assign_kit ran for a cancelled order")
    monkeypatch.setattr(appmod, "assign_kit", must_not_run)

    result = run(appmod._recurate_customer_core(db, "cust-1", appmod.BackgroundTasks()))
    assert result["status"] == "blocked_cancelled"
    assert "2026-09-08" in result["message"]
    assert [d["status"] for d in db.tables["decisions"]] == ["shipped"]  # no new box


def test_recurate_not_blocked_when_order_is_not_cancelled(db, monkeypatch):
    db.tables["decisions"].append(_dec("shipped"))

    class Reached(Exception):
        pass

    async def reached(*_a, **_k):
        raise Reached
    monkeypatch.setattr(appmod, "assign_kit", reached)

    with pytest.raises(Reached):  # got past the guard to the decision engine
        run(appmod._recurate_customer_core(db, "cust-1", appmod.BackgroundTasks()))
