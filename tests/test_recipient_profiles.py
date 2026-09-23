"""
Threads 20/21 — recipient-profile routing, end to end against an in-memory fake DB.
Covers the real production shapes: bednarfive (Cratejoy, 2 subscriptions), jessicamariehawley
(Shopify, 2 Recharge subscriptions), Nicole → Kailin (order edit), "same person" renewals.

Run:  OBB_DISABLE_SCHEDULER=1 python -m pytest tests/test_recipient_profiles.py -v
"""

import asyncio
import copy
import itertools
import os
import sys
from datetime import date

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as appmod  # noqa: E402

_ids = itertools.count(1)


class _Res:
    def __init__(self, data, count=None):
        self.data, self.count = data, count


class _Q:
    def __init__(self, db, table):
        self.db, self.t, self.f, self.lim = db, table, [], None
        self.op, self.payload, self.desc, self.okey = "select", None, False, None

    def select(self, *_a, **_k): return self
    def insert(self, data): self.op, self.payload = "insert", data; return self
    def update(self, data): self.op, self.payload = "update", data; return self
    def _add(self, fn): self.f.append(fn); return self
    def eq(self, k, v): return self._add(lambda r: str(r.get(k)) == str(v))
    def neq(self, k, v): return self._add(lambda r: str(r.get(k)) != str(v))
    def gte(self, k, v): return self._add(lambda r: r.get(k) is not None and str(r.get(k)) >= str(v))
    def lt(self, k, v): return self._add(lambda r: r.get(k) is not None and str(r.get(k)) < str(v))
    def lte(self, k, v): return self._add(lambda r: r.get(k) is not None and str(r.get(k)) <= str(v))
    def in_(self, k, vs): return self._add(lambda r: r.get(k) in vs)
    def ilike(self, k, v): return self._add(lambda r: str(r.get(k) or "").lower() == str(v).lower())
    def is_(self, k, v): return self._add(lambda r: r.get(k) is None) if v == "null" else self

    def or_(self, expr):
        # only the form used by the app: "ship_to_source.is.null,ship_to_source.eq.order"
        return self._add(lambda r: r.get("ship_to_source") in (None, "order"))

    def limit(self, n): self.lim = n; return self
    def order(self, k, desc=False): self.okey, self.desc = k, desc; return self

    def execute(self):
        rows = self.db.tables.setdefault(self.t, [])
        if self.op == "insert":
            recs = self.payload if isinstance(self.payload, list) else [self.payload]
            out = []
            for rec in recs:
                rec = dict(rec)
                rec.setdefault("id", f"{self.t[:4]}-{next(_ids):04d}")
                rec.setdefault("created_at", f"2026-09-{10 + next(_ids) % 10:02d}T00:00:00+00:00")
                if self.t == "customers":
                    key = ((rec.get("email") or "").lower(), rec.get("recipient_ref") or "")
                    if any(((r.get("email") or "").lower(), r.get("recipient_ref") or "") == key for r in rows):
                        raise Exception("duplicate key value violates unique constraint idx_customers_email_recipient")
                rows.append(rec)
                out.append(copy.deepcopy(rec))
            return _Res(out)
        hit = [r for r in rows if all(fn(r) for fn in self.f)]
        if self.op == "update":
            for r in hit:
                r.update(self.payload)
            return _Res(copy.deepcopy(hit))
        if self.okey:
            hit = sorted(hit, key=lambda r: str(r.get(self.okey) or ""), reverse=self.desc)
        if self.lim:
            hit = hit[: self.lim]
        return _Res(copy.deepcopy(hit))


class FakeDB:
    def __init__(self, **tables):
        self.tables = {k: [dict(r) for r in v] for k, v in tables.items()}

    def table(self, name):
        return _Q(self, name)


def _no_log(monkeypatch):
    async def _log(*a, **k):
        _log.calls.append(a)
    _log.calls = []
    monkeypatch.setattr(appmod, "log_activity", _log)
    return _log


# ─────────────────────── Cratejoy: bednarfive ───────────────────────

LEAH_SUB, LANA_SUB = "7093457035", "7093458554"


def _cj_box(ship_id, sub, target, to, street, city, state, zipc):
    return {"id": int(ship_id), "status": "unshipped", "target_at": target,
            "customer": {"id": 7093449651, "email": "bednarfive@gmail.com", "first_name": "Susan", "last_name": "Bednarczyk"},
            "fulfillments": [{"subscription_id": int(sub), "cycle_number": 0}],
            "ship_address": {"to": to, "street": street, "unit": "", "city": city, "state": state,
                             "zip_code": zipc, "country": "US", "phone_number": ""}}


def _bednarfive_db():
    return FakeDB(
        customers=[{"id": "cust-leah", "email": "bednarfive@gmail.com", "first_name": "Susan", "last_name": "Bednarczyk",
                    "cratejoy_customer_id": "7093449651", "platform": "cratejoy", "subscription_status": "active",
                    "history_pending": False, "recipient_ref": None, "recipient_name": None,
                    "created_at": "2026-09-10T20:08:36+00:00"}],
        decisions=[{"id": "dec-leah-sep", "customer_id": "cust-leah", "status": "shipped", "platform": "cratejoy",
                    "order_id": LEAH_SUB, "cratejoy_shipment_id": "7093457045", "ship_date": "2026-09-09",
                    "ship_first_name": "Leah", "ship_last_name": "Hudson", "created_at": "2026-09-10T20:08:39+00:00"}],
        activity_log=[],
    )


def _patch_cj_engine(monkeypatch):
    async def fake_assign_kit(cust_id, ship_date):
        return {"kit_id": "k1", "kit_sku": f"KIT-FOR-{cust_id}", "decision_type": "auto", "reason": "test"}

    async def fake_enrich(client, sub_id, ship_addr):
        surveys = {LEAH_SUB: {"due_date": "2027-01-24", "clothing_size": "M", "baby_gender": "boy"},
                   LANA_SUB: {"due_date": "2027-01-04", "clothing_size": "M"}}
        out = dict(surveys.get(sub_id, {}))
        out.update({"address_line1": ship_addr.get("street"), "city": ship_addr.get("city"),
                    "province": ship_addr.get("state"), "zip": ship_addr.get("zip_code"), "country": "US"})
        return out

    async def fake_refresh(*a, **k):
        return None

    monkeypatch.setattr(appmod, "assign_kit", fake_assign_kit)
    monkeypatch.setattr(appmod, "_cj_enrich_new_customer", fake_enrich)
    monkeypatch.setattr(appmod, "_cj_refresh_existing_customer", fake_refresh)
    monkeypatch.setattr(appmod, "_compute_order_type", lambda db, cid: "new")


def test_cratejoy_second_subscription_gets_its_own_profile_and_decision(monkeypatch):
    db = _bednarfive_db()
    log = _no_log(monkeypatch)
    _patch_cj_engine(monkeypatch)
    lana_box = _cj_box(7093458562, LANA_SUB, "2026-09-09T23:13:57Z", "Lana Bain", "8301 Southern Oaks Ct", "Lorton", "VA", "22079")

    tag = asyncio.run(appmod.process_cratejoy_box(db, None, lana_box, date(2026, 9, 11)))
    assert tag == "created"

    custs = {c["id"]: c for c in db.tables["customers"]}
    assert custs["cust-leah"]["recipient_ref"] == f"cj:{LEAH_SUB}"          # legacy profile learned identity
    assert custs["cust-leah"]["recipient_name"] == "Leah Hudson"
    lana = next(c for c in custs.values() if c["id"] != "cust-leah")
    assert (lana["first_name"], lana["last_name"], lana["recipient_ref"]) == ("Lana", "Bain", f"cj:{LANA_SUB}")
    assert (lana["due_date"], lana["address_line1"]) == ("2027-01-04", "8301 Southern Oaks Ct")   # her OWN survey/address

    lana_dec = next(d for d in db.tables["decisions"] if d["customer_id"] == lana["id"])
    assert lana_dec["cratejoy_shipment_id"] == "7093458562"
    assert (lana_dec["ship_first_name"], lana_dec["ship_address1"], lana_dec["ship_city"]) == ("Lana", "8301 Southern Oaks Ct", "Lorton")
    assert any("New recipient profile" in str(c[1]) for c in log.calls)

    # Leah's October box routes back to Leah's profile, not Lana's
    leah_oct = _cj_box(7093457046, LEAH_SUB, "2026-10-01T07:00:00Z", "Leah Hudson", "309 Sophia Rain Dr", "Nashville", "TN", "37218")
    assert asyncio.run(appmod.process_cratejoy_box(db, None, leah_oct, date(2026, 10, 1))) == "created"
    oct_dec = next(d for d in db.tables["decisions"] if d.get("cratejoy_shipment_id") == "7093457046")
    assert oct_dec["customer_id"] == "cust-leah" and oct_dec["ship_city"] == "Nashville"
    assert len(db.tables["customers"]) == 2


def test_cratejoy_rerun_is_idempotent(monkeypatch):
    db = _bednarfive_db()
    _no_log(monkeypatch)
    _patch_cj_engine(monkeypatch)
    lana_box = _cj_box(7093458562, LANA_SUB, "2026-09-09T23:13:57Z", "Lana Bain", "8301 Southern Oaks Ct", "Lorton", "VA", "22079")
    asyncio.run(appmod.process_cratejoy_box(db, None, lana_box, date(2026, 9, 11)))
    assert asyncio.run(appmod.process_cratejoy_box(db, None, lana_box, date(2026, 9, 12))) == "skip_exists"
    assert len(db.tables["customers"]) == 2 and len(db.tables["decisions"]) == 2


def test_cratejoy_dry_run_writes_nothing(monkeypatch):
    db = _bednarfive_db()
    _no_log(monkeypatch)
    _patch_cj_engine(monkeypatch)
    before = copy.deepcopy(db.tables)
    lana_box = _cj_box(7093458562, LANA_SUB, "2026-09-09T23:13:57Z", "Lana Bain", "8301 Southern Oaks Ct", "Lorton", "VA", "22079")
    assert asyncio.run(appmod.process_cratejoy_box(db, None, lana_box, date(2026, 9, 11), dry_run=True)) == "create_recipient"
    assert db.tables == before


# ─────────────────────── Shopify: two Recharge subscriptions ───────────────────────

def _record(first, last, addr1, due, size):
    return {"email": "jessicamariehawley@gmail.com", "first_name": first, "last_name": last,
            "shopify_customer_id": "999", "address_line1": addr1, "due_date": due, "clothing_size": size,
            "wants_daddy_item": False, "country": "US"}


def test_shopify_two_recipients_route_to_two_profiles(monkeypatch):
    db = FakeDB(customers=[], decisions=[])
    kw = dict(is_subscription_order=True, purchaser_first="Jessica", purchaser_last="Hawley", log_prefix="[T]")
    haylie_ship = {"first_name": "Haylie", "last_name": "Lazar", "address1": "1 Main St", "city": "Horicon"}
    blenc_ship = {"first_name": "The", "last_name": "Blencoes", "address1": "9 Oak Ave", "city": "Antioch"}

    cid_h, _, act_h = appmod.upsert_shopify_recipient_profile(
        db, email="jessicamariehawley@gmail.com", quiz={"rc_subscription_ids": "816965718"}, shipping=haylie_ship,
        customer_record=_record("Jessica", "Hawley", "1 Main St", "2027-01-13", "M"), **kw)
    cid_b, _, act_b = appmod.upsert_shopify_recipient_profile(
        db, email="jessicamariehawley@gmail.com", quiz={"rc_subscription_ids": "819699361"}, shipping=blenc_ship,
        customer_record=_record("Jessica", "Hawley", "9 Oak Ave", "2026-12-18", "XL"), **kw)
    assert (act_h, act_b) == ("create_new", "create_recipient") and cid_h != cid_b

    custs = {c["id"]: c for c in db.tables["customers"]}
    assert (custs[cid_h]["due_date"], custs[cid_h]["clothing_size"]) == ("2027-01-13", "M")
    assert (custs[cid_b]["due_date"], custs[cid_b]["clothing_size"], custs[cid_b]["first_name"]) == ("2026-12-18", "XL", "The")

    # Blencoes renewal — label name changed, same Recharge sub → same profile; Haylie untouched
    cid_b2, _, act_b2 = appmod.upsert_shopify_recipient_profile(
        db, email="jessicamariehawley@gmail.com", quiz={"rc_subscription_ids": "819699361"},
        shipping={"first_name": "Sarah", "last_name": "Blencoe", "address1": "9 Oak Ave", "city": "Antioch"},
        customer_record=_record("Jessica", "Hawley", "9 Oak Ave", "2026-12-18", "XL"), **kw)
    assert (cid_b2, act_b2) == (cid_b, "match")
    assert custs[cid_h]["due_date"] == "2027-01-13" and db.tables["customers"][0]["first_name"] == "Jessica"
    assert next(c for c in db.tables["customers"] if c["id"] == cid_b)["first_name"] == "The"  # never renamed to purchaser
    assert len(db.tables["customers"]) == 2


def test_shopify_same_person_legacy_profile_is_not_split(monkeypatch):
    # fatboy96at: legacy profile whose recent boxes ship to Connie Trips; renewal carries a Recharge id
    db = FakeDB(
        customers=[{"id": "c1", "email": "fatboy96at@gmail.com", "first_name": "Anthony", "last_name": "Trips",
                    "recipient_ref": None, "recipient_name": None, "created_at": "2026-06-25T00:00:00+00:00"}],
        decisions=[{"id": "d1", "customer_id": "c1", "status": "shipped", "platform": "shopify",
                    "ship_first_name": "Connie", "ship_last_name": "Trips", "created_at": "2026-09-17T00:00:00+00:00"}])
    cid, _, act = appmod.upsert_shopify_recipient_profile(
        db, email="fatboy96at@gmail.com", quiz={"rc_subscription_ids": "555"},
        shipping={"first_name": "Connie", "last_name": "Trips", "address1": "1 Rome St"},
        customer_record={"email": "fatboy96at@gmail.com", "first_name": "Anthony", "last_name": "Trips"},
        is_subscription_order=True, purchaser_first="Anthony", purchaser_last="Trips", log_prefix="[T]")
    assert (cid, act) == ("c1", "claim")
    assert db.tables["customers"][0]["recipient_ref"] == "rc:555" and len(db.tables["customers"]) == 1


def test_non_subscription_order_never_creates_a_profile(monkeypatch):
    db = FakeDB(customers=[{"id": "c1", "email": "a@b.c", "first_name": "Ann", "last_name": "F",
                            "recipient_name": "Ann F", "recipient_ref": "rc:1", "created_at": "2026-01-01"}],
                decisions=[])
    cid, _, act = appmod.upsert_shopify_recipient_profile(
        db, email="a@b.c", quiz={}, shipping={"first_name": "Gift", "last_name": "Friend", "address1": "x"},
        customer_record={"email": "a@b.c", "address_line1": "x"}, is_subscription_order=False,
        purchaser_first="Ann", purchaser_last="F", log_prefix="[T]")
    assert (cid, act) == (None, "skip") and len(db.tables["customers"]) == 1


# ─────────────────────── Shopify order edit → box ship-to (Nicole → Kailin) ───────────────────────

def test_order_edit_updates_pending_box_and_flags_shipped_box(monkeypatch):
    log = _no_log(monkeypatch)
    db = FakeDB(activity_log=[], decisions=[
        {"id": "68ffd1be-pending", "order_id": "7548199141665", "status": "pending", "veracore_order_id": None,
         "ship_to_source": None, "ship_first_name": "Nicole", "ship_last_name": "Goldstein"},
        {"id": "aaaaaaaa-shipped", "order_id": "7548199141665", "status": "shipped", "veracore_order_id": None,
         "ship_to_source": "order", "ship_first_name": "Nicole", "ship_last_name": "Goldstein"},
        {"id": "bbbbbbbb-manual", "order_id": "7548199141665", "status": "pending", "veracore_order_id": None,
         "ship_to_source": "manual", "ship_first_name": "Staff", "ship_last_name": "Set"},
    ])
    kailin = {"first_name": "Kailin", "last_name": "Goldstein", "address1": "1100 South Loop 336 West",
              "address2": "Apt 3213", "city": "Conroe", "province": "Texas", "zip": "77304", "country_code": "US"}
    changes = []
    cust = {"email": "nicgoldstein@goldsteinus.net", "first_name": "Nicole", "last_name": "Goldstein"}
    asyncio.run(appmod._sync_order_ship_to_from_shopify(db, "7548199141665", kailin, "Kailin Goldstein", cust, changes))
    boxes = {d["id"]: d for d in db.tables["decisions"]}
    assert (boxes["68ffd1be-pending"]["ship_first_name"], boxes["68ffd1be-pending"]["ship_city"]) == ("Kailin", "Conroe")
    assert boxes["aaaaaaaa-shipped"]["ship_first_name"] == "Nicole"            # shipped box not rewritten…
    assert any("after approval" in str(c[1]) for c in log.calls)                # …but flagged
    assert boxes["bbbbbbbb-manual"]["ship_first_name"] == "Staff"              # manual never touched
    # re-sent webhook: no second update, no duplicate warning spam within 7 days
    n_logs = len(log.calls)
    db.tables["activity_log"].append({"summary": "Ship-to changed in Shopify after approval — decision aaaaaaaa",
                                      "created_at": "2099-01-01T00:00:00"})
    asyncio.run(appmod._sync_order_ship_to_from_shopify(db, "7548199141665", kailin, "Kailin Goldstein", cust, []))
    assert len(log.calls) == n_logs
