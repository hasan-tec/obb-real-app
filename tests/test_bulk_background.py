"""Thread 25: bulk actions run on a background thread (no more Heroku 30 s 'Application Error')."""
import asyncio
import os
import sys

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402
from starlette.requests import Request  # noqa: E402


class _Q:
    def __init__(self, db, name):
        self.db, self.name, self.filters, self.payload, self.op = db, name, [], None, "select"

    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self.filters.append(lambda r: r.get(col) == val)
        return self

    def in_(self, col, vals):
        self.filters.append(lambda r: r.get(col) in set(vals))
        return self

    def update(self, payload):
        self.op, self.payload = "update", payload
        return self

    def insert(self, payload):
        self.op, self.payload = "insert", payload
        return self

    def execute(self):
        rows = self.db.tables.setdefault(self.name, [])
        if self.op == "insert":
            new = [dict(x) for x in (self.payload if isinstance(self.payload, list) else [self.payload])]
            for n, x in enumerate(new):
                x.setdefault("id", f"{self.name}-{len(rows) + n}")
            rows.extend(new)
            return type("R", (), {"data": new})()
        hit = [r for r in rows if all(f(r) for f in self.filters)]
        if self.op == "update":
            if self.db.fail_on and any(r.get("id") == self.db.fail_on for r in hit):
                raise RuntimeError("boom")
            for r in hit:
                r.update(self.payload)
        return type("R", (), {"data": hit})()


class FakeDB:
    def __init__(self, decisions, fail_on=None):
        self.tables = {"decisions": decisions}
        self.fail_on = fail_on

    def table(self, name):
        return _Q(self, name)


def _setup(monkeypatch, db):
    monkeypatch.setattr(app, "get_supabase", lambda: db)
    monkeypatch.setattr(app, "bulk_sync_sheet_statuses", lambda q: None)


def _decision(i, status="pending"):
    return {"id": f"d{i}", "status": status, "customer_id": f"c{i}", "customers": {"email": f"c{i}@x.com"}}


def test_reject_job_counts_and_releases_claims(monkeypatch):
    db = FakeDB([_decision(1), _decision(2), _decision(3, status="shipped")])
    _setup(monkeypatch, db)
    app._bulk_jobs["j1"] = {"id": "j1", "action": "reject", "total": 3, "done": 0, "succeeded": 0,
                            "skipped": 0, "failed": 0, "status": "running"}
    app._bulk_action_inflight_decisions.update({"d1", "d2", "d3"})
    asyncio.run(app._run_bulk_action("j1", "reject", ["d1", "d2", "d3"], 0, False))
    job = app.bulk_job_snapshot("j1")
    assert (job["status"], job["done"], job["succeeded"], job["skipped"], job["failed"]) == ("done", 3, 2, 1, 0)
    assert [r["status"] for r in db.tables["decisions"]] == ["rejected", "rejected", "shipped"]
    assert not ({"d1", "d2", "d3"} & app._bulk_action_inflight_decisions)
    assert any("Bulk reject: 2 succeeded, 1 skipped" in a["summary"] for a in db.tables["activity_log"])


def test_row_error_counts_as_failed_and_claims_still_released(monkeypatch):
    db = FakeDB([_decision(1), _decision(2)], fail_on="d1")
    _setup(monkeypatch, db)
    app._bulk_jobs["j2"] = {"id": "j2", "action": "reject", "total": 2, "done": 0, "succeeded": 0,
                            "skipped": 0, "failed": 0, "status": "running"}
    app._bulk_action_inflight_decisions.update({"d1", "d2"})
    asyncio.run(app._run_bulk_action("j2", "reject", ["d1", "d2"], 0, False))
    job = app.bulk_job_snapshot("j2")
    assert (job["done"], job["succeeded"], job["failed"]) == (2, 1, 1)
    assert not ({"d1", "d2"} & app._bulk_action_inflight_decisions)


def _post(body: bytes):
    scope = {"type": "http", "method": "POST", "path": "/decisions/bulk-action", "query_string": b"",
             "headers": [(b"content-type", b"application/x-www-form-urlencoded"),
                         (b"content-length", str(len(body)).encode())]}
    sent = {"x": False}

    async def rcv():
        if sent["x"]:
            return {"type": "http.disconnect"}
        sent["x"] = True
        return {"type": "http.request", "body": body, "more_body": False}
    return Request(scope, rcv)


def test_route_returns_at_once_and_hands_the_work_to_a_thread(monkeypatch):
    started = []

    class FakeThread:
        def __init__(self, target, name, daemon, args):
            started.append(args)

        def start(self):
            pass
    monkeypatch.setattr(app.threading, "Thread", FakeThread)
    monkeypatch.setattr(app, "get_supabase", lambda: FakeDB([]))
    body = b"action=reject&decision_ids=x1&decision_ids=x2&redirect_qs=status%3Dpending%26bulk_job%3Dold"
    resp = asyncio.run(app.bulk_decision_action(_post(body), app.BackgroundTasks()))
    assert resp.status_code == 303
    loc = resp.headers["location"]
    assert loc.startswith("/decisions?status=pending&bulk_job=") and "old" not in loc
    job_id = loc.rsplit("=", 1)[1]
    assert started and started[0][0] == job_id and started[0][2] == ["x1", "x2"]
    assert app.bulk_job_snapshot(job_id)["status"] == "running"
    # the thread owns the claims now: they stay claimed until it releases them
    assert {"x1", "x2"} <= app._bulk_action_inflight_decisions
    app._bulk_action_inflight_decisions.difference_update({"x1", "x2"})


def test_status_endpoint_unknown_job():
    resp = asyncio.run(app.bulk_job_status("nope"))
    assert resp.body == b'{"status":"unknown"}'


def test_approve_queues_veracore_push_and_runs_it_after_the_loop(monkeypatch):
    db = FakeDB([_decision(1), _decision(2)])
    _setup(monkeypatch, db)
    pushed = []
    monkeypatch.setattr(app, "submit_to_veracore", lambda did, batch, ship_to_ob: pushed.append((did, ship_to_ob)))
    app._bulk_jobs["j3"] = {"id": "j3", "action": "approve", "total": 2, "done": 0, "succeeded": 0,
                            "skipped": 0, "failed": 0, "status": "running"}
    asyncio.run(app._run_bulk_action("j3", "approve", ["d1", "d2"], 0, True))
    assert app.bulk_job_snapshot("j3")["succeeded"] == 2
    assert [r["status"] for r in db.tables["decisions"]] == ["approved", "approved"]
    assert pushed == [("d1", True), ("d2", True)]
    assert len(db.tables["shipments"]) == 2


def test_bulk_thread_uses_its_own_db_client(monkeypatch):
    import threading
    shared, mine, seen = object(), object(), {}
    monkeypatch.setattr(app, "supabase", shared)
    monkeypatch.setattr(app, "create_client", lambda url, key: mine)

    async def fake_run(*_a):
        seen["thread"] = app.get_supabase()
    monkeypatch.setattr(app, "_run_bulk_action", fake_run)
    t = threading.Thread(target=app._bulk_job_thread, args=("j", "reject", [], 0, False))
    t.start()
    t.join()
    assert seen["thread"] is mine          # the job thread gets its own client
    assert app.get_supabase() is shared    # everyone else keeps the shared one
