"""'Shipped on' date filter: shipped_at is recorded when a box is shipped, and the filter matches
the viewer's local calendar day (Ting / Thread 26 close-out item)."""
import asyncio
import os
import sys

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "scripts"))
sys.path.insert(0, os.path.join(_root, "tests"))

import app  # noqa: E402
import backfill_shipped_at as bf  # noqa: E402
from test_replay_guard import FakeDB  # noqa: E402  (supports eq/ilike/gte/lt/neq/in_/insert/update)


def test_range_is_the_viewers_local_day():
    # Manila (UTC+8): browser getTimezoneOffset() = -480 -> Oct 3 local = Oct 2 16:00 .. Oct 3 16:00 UTC
    assert app.shipped_on_range("2026-10-03", "-480") == ("2026-10-02T16:00:00+00:00", "2026-10-03T16:00:00+00:00")
    # US Pacific (UTC-7): +420
    assert app.shipped_on_range("2026-10-03", "420") == ("2026-10-03T07:00:00+00:00", "2026-10-04T07:00:00+00:00")
    # no / junk offset -> UTC day
    assert app.shipped_on_range("2026-10-03", "") == ("2026-10-03T00:00:00+00:00", "2026-10-04T00:00:00+00:00")
    assert app.shipped_on_range("2026-10-03", "abc")[0] == "2026-10-03T00:00:00+00:00"
    assert app.shipped_on_range("2026-10-03", "99999")[0] == "2026-10-03T00:00:00+00:00"


def test_empty_or_bad_date_means_no_filter():
    assert app.shipped_on_range("", "-480") is None
    assert app.shipped_on_range("2026-13-40", "0") is None
    assert app.shipped_on_range("yesterday", "0") is None


def test_bulk_ship_records_shipped_at(monkeypatch):
    db = FakeDB({"decisions": [{"id": "d1-aaaaaaaa", "status": "approved", "customer_id": "c1", "kit_id": None,
                                "kit_sku": None, "customers": {"email": "a@x.com"}}],
                 "shipments": [], "kit_items": []})
    monkeypatch.setattr(app, "get_supabase", lambda: db)
    monkeypatch.setattr(app, "bulk_sync_sheet_statuses", lambda q: None)
    app._bulk_jobs["js"] = {"id": "js", "action": "ship", "total": 1, "done": 0, "succeeded": 0,
                            "skipped": 0, "failed": 0, "status": "running"}
    asyncio.run(app._run_bulk_action("js", "ship", ["d1-aaaaaaaa"], 0, False))
    d = db.tables["decisions"][0]
    assert d["status"] == "shipped"
    assert d["shipped_at"] and d["shipped_at"].endswith("+00:00")


def test_backfill_prefers_the_decisions_own_shipment():
    d = {"id": "abcdef12-0000", "customer_id": "c1", "order_id": "O1", "updated_at": "2026-09-30T10:00:00"}
    by_ref = {("c1", "abcdef12"): "2026-09-17"}
    by_order = {("c1", "O1"): "2026-09-20"}
    assert bf.pick_ship_day(d, by_ref, by_order) == ("2026-09-17", "shipment_for_decision")
    assert bf.pick_ship_day(d, {}, by_order) == ("2026-09-20", "shipment_same_order")
    assert bf.pick_ship_day(d, {}, {}) == ("2026-09-30", "updated_at_fallback")
    assert bf.pick_ship_day({**d, "order_id": None, "updated_at": None}, {}, by_order) == (None, "no_date")


def test_veracore_ship_time_is_used_when_present():
    import veracore_sync as vs
    assert vs._iso_or_now("2026-09-30T18:45:00Z") == "2026-09-30T18:45:00+00:00"
    assert vs._iso_or_now("2026-09-30T18:45:00-07:00") == "2026-10-01T01:45:00+00:00"
    assert vs._iso_or_now("") .endswith("+00:00")          # falls back to now
    assert vs._iso_or_now(None).endswith("+00:00")
