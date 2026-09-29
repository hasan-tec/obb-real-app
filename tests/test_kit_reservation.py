"""Thread 25: a kit is only handed out while on-hand stock minus pending boxes holding it is > 0."""
import os
import sys

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402


class _Q:
    def __init__(self, rows):
        self.rows, self.filters, self.lo, self.hi = rows, [], 0, None

    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self.filters.append(lambda r: r.get(col) == val)
        return self

    def in_(self, col, vals):
        self.filters.append(lambda r: r.get(col) in set(vals))
        return self

    def range(self, lo, hi):
        self.lo, self.hi = lo, hi
        return self

    def execute(self):
        rows = [r for r in self.rows if all(f(r) for f in self.filters)]
        rows = rows[self.lo:(self.hi + 1 if self.hi is not None else None)]
        return type("R", (), {"data": rows})()


class FakeDB:
    def __init__(self, decisions):
        self.decisions = decisions

    def table(self, name):
        assert name == "decisions"
        return _Q(self.decisions)


BT21 = {"id": "k-bt21", "sku": "OBB-BT-21 KITS", "quantity_available": 3}
CA21 = {"id": "k-ca21", "sku": "OBB-CA-21 KITS", "quantity_available": 24}


def test_kit_dropped_when_pending_boxes_hold_all_units():
    db = FakeDB([{"kit_id": "k-bt21", "status": "pending"}] * 3)
    assert [k["sku"] for k in app.kits_with_free_stock(db, [BT21, CA21], "t")] == ["OBB-CA-21 KITS"]


def test_only_pending_counts_not_rejected_or_approved():
    db = FakeDB([{"kit_id": "k-bt21", "status": "pending"}] * 2
                + [{"kit_id": "k-bt21", "status": "rejected"}] * 5
                + [{"kit_id": "k-bt21", "status": "approved"}] * 5)
    assert [k["sku"] for k in app.kits_with_free_stock(db, [BT21], "t")] == ["OBB-BT-21 KITS"]


def test_three_units_go_to_three_boxes_then_next_kit():
    decisions = []
    db = FakeDB(decisions)
    given = []
    for _ in range(5):
        kit = app.kits_with_free_stock(db, [BT21, CA21], "t")[0]  # engine takes the first (age_rank order)
        decisions.append({"kit_id": kit["id"], "status": "pending"})
        given.append(kit["sku"])
    assert given == ["OBB-BT-21 KITS"] * 3 + ["OBB-CA-21 KITS"] * 2


def test_reserved_counts_paginates_past_1000():
    db = FakeDB([{"kit_id": "k-ca21", "status": "pending"}] * 2500)
    assert app.kit_reserved_counts(db, ["k-ca21"]) == {"k-ca21": 2500}


def test_empty_inputs():
    assert app.kits_with_free_stock(FakeDB([]), [], "t") == []
    assert app.kit_reserved_counts(FakeDB([]), [None]) == {}
