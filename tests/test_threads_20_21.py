"""
Threads 20/21 — unit tests for the pure helpers (no DB, no HTTP).
Shapes are taken from real production cases found on 2026-09-23 (THREADS_20_21_FIX_PLAN.md §0).

Run:  OBB_DISABLE_SCHEDULER=1 python -m pytest tests/test_threads_20_21.py -v
"""

import os
import sys
from datetime import date

os.environ.setdefault("OBB_DISABLE_SCHEDULER", "1")  # never start the background jobs from a test
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import (  # noqa: E402
    norm_name,
    ship_to_cols_from_shopify,
    ship_to_cols_from_cratejoy,
    decision_ship_to,
    pick_cratejoy_shipment_for_tracking,
    choose_decision_for_tracking_row,
    recipient_ref_for_shopify,
    recipient_ref_for_cratejoy,
    resolve_recipient_profile,
)

TODAY = date(2026, 9, 14)


# ─── norm_name ───

def test_norm_name_case_spaces_and_care_of():
    assert norm_name("Allison  Swales ") == "allison swales"
    assert norm_name("Kylie Russell c/o Wayne Moss") == "kylie russell"
    assert norm_name(None) == ""


# ─── ship_to_cols_* ───

KAILIN_SHIPPING = {
    "first_name": "Kailin", "last_name": "Goldstein", "address1": "1100 South Loop 336 West",
    "address2": "Apt 3213", "city": "Conroe", "province": "Texas", "zip": "77304",
    "country_code": "US", "phone": None,
}


def test_ship_to_cols_from_shopify_gift_recipient():
    cols = ship_to_cols_from_shopify(KAILIN_SHIPPING, "Nicole", "Goldstein")
    assert cols["ship_first_name"] == "Kailin"
    assert cols["ship_address1"] == "1100 South Loop 336 West"
    assert cols["ship_address2"] == "Apt 3213"
    assert cols["ship_state"] == "Texas"
    assert cols["ship_to_source"] == "order"


def test_ship_to_cols_from_shopify_no_address_never_blanks():
    cols = ship_to_cols_from_shopify({}, "Nicole", "Goldstein")
    assert cols == {"ship_first_name": "Nicole", "ship_last_name": "Goldstein"}


def test_ship_to_cols_from_cratejoy_unit_and_one_word_name():
    cols = ship_to_cols_from_cratejoy({"to": "Leah Hudson", "street": "309 Sophia Rain Dr", "unit": "B",
                                       "city": "Nashville", "state": "TN", "zip_code": "37218",
                                       "country": "US", "phone_number": ""})
    assert (cols["ship_first_name"], cols["ship_last_name"]) == ("Leah", "Hudson")
    assert cols["ship_address2"] == "B"
    assert cols["ship_phone"] is None
    one = ship_to_cols_from_cratejoy({"to": "Cher"})
    assert one == {"ship_first_name": "Cher", "ship_last_name": None}


# ─── decision_ship_to ───

CUST_LANA_ADDR = {"first_name": "Susan", "last_name": "Bednarczyk", "email": "bednarfive@gmail.com",
                  "address_line1": "8301 Southern Oaks Ct", "city": "Lorton", "province": "VA",
                  "zip": "22079", "country": "US", "phone": "555"}


def test_decision_ship_to_decision_snapshot_wins():
    d = {"ship_first_name": "Leah", "ship_last_name": "Hudson", "ship_address1": "309 Sophia Rain Dr",
         "ship_city": "Nashville", "ship_state": "TN", "ship_zip": "37218", "ship_country": "US"}
    st = decision_ship_to(d, CUST_LANA_ADDR)
    assert st["name"] == "Leah Hudson"
    assert (st["address1"], st["city"], st["state"]) == ("309 Sophia Rain Dr", "Nashville", "TN")
    assert st["source"] == "decision"
    assert st["phone"] == "555"  # falls back to customer phone


def test_decision_ship_to_falls_back_to_customer_atomically():
    d = {"ship_first_name": "Leah", "ship_last_name": "Hudson", "ship_city": "Nashville"}  # no street
    st = decision_ship_to(d, CUST_LANA_ADDR)
    assert st["source"] == "customer"
    assert (st["address1"], st["city"]) == ("8301 Southern Oaks Ct", "Lorton")  # never mixed
    assert st["name"] == "Leah Hudson"


def test_decision_ship_to_name_is_atomic():
    d = {"ship_first_name": "Cher", "ship_last_name": None}
    assert decision_ship_to(d, CUST_LANA_ADDR)["name"] == "Cher"  # not "Cher Bednarczyk"
    assert decision_ship_to({}, CUST_LANA_ADDR)["name"] == "Susan Bednarczyk"


# ─── pick_cratejoy_shipment_for_tracking ───

def _ship(sid, status, target, sub="7093458554"):
    return {"id": sid, "status": status, "target_at": target, "created_at": "2026-09-09T16:13:57-07:00",
            "fulfillments": [{"subscription_id": int(sub)}]}


LANA_BOXES = [
    _ship(7093458562, "unshipped", "2026-09-09T23:13:57Z"),
    _ship(7093458563, "unshipped", "2026-10-01T07:00:00Z"),
    _ship(7093458564, "unshipped", "2026-11-01T07:00:00Z"),
]


def test_pick_exact_shipment_id():
    s, why = pick_cratejoy_shipment_for_tracking(LANA_BOXES, {"cratejoy_shipment_id": "7093458563"}, TODAY)
    assert (s["id"], why) == (7093458563, "ok")
    s, why = pick_cratejoy_shipment_for_tracking(LANA_BOXES, {"cratejoy_shipment_id": "999"}, TODAY)
    assert (s, why) == (None, "not_found")


def test_pick_due_box_never_future_box_with_same_created_at():
    s, why = pick_cratejoy_shipment_for_tracking(LANA_BOXES, {"order_id": "7093458554"}, TODAY)
    assert (s["id"], why) == (7093458562, "ok")


def test_pick_only_future_boxes_is_no_due_box():
    s, why = pick_cratejoy_shipment_for_tracking(LANA_BOXES[1:], {"order_id": "7093458554"}, TODAY)
    assert (s, why) == (None, "no_due_box")


def test_pick_two_due_same_date_is_ambiguous():
    boxes = [_ship(1, "unshipped", "2026-09-09T23:13:57Z"), _ship(2, "unshipped", "2026-09-09T23:13:57Z")]
    assert pick_cratejoy_shipment_for_tracking(boxes, {"order_id": "7093458554"}, TODAY) == (None, "ambiguous")


def test_pick_filters_other_subscription():
    boxes = [_ship(7093457046, "unshipped", "2026-09-09T23:08:36Z", sub="7093457035"),  # Leah's sub
             LANA_BOXES[0]]
    s, why = pick_cratejoy_shipment_for_tracking(boxes, {"order_id": "7093457035"}, TODAY)
    assert (s["id"], why) == (7093457046, "ok")


def test_pick_skips_already_shipped_in_rule_2():
    boxes = [_ship(1, "shipped", "2026-09-09T23:13:57Z"), LANA_BOXES[1]]
    assert pick_cratejoy_shipment_for_tracking(boxes, {"order_id": "7093458554"}, TODAY) == (None, "no_due_box")


# ─── choose_decision_for_tracking_row ───

def _dec(did, first, last):
    return {"id": did, "ship_first_name": first, "ship_last_name": last, "customers": {"email": "x@y.z"}}


def test_choose_decision_counts_and_names():
    leah, lana = _dec("a", "Leah", "Hudson"), _dec("b", "Lana", "Bain")
    assert choose_decision_for_tracking_row([], "Leah Hudson") == (None, "unmatched")
    assert choose_decision_for_tracking_row([leah], "") == (leah, "ok")
    assert choose_decision_for_tracking_row([leah, lana], "lana  bain") == (lana, "ok")
    assert choose_decision_for_tracking_row([leah, lana], "") == (None, "ambiguous")
    assert choose_decision_for_tracking_row([leah, lana], "Someone Else") == (None, "ambiguous")


# ─── recipient refs ───

def test_recipient_refs():
    assert recipient_ref_for_shopify({"rc_subscription_ids": "816965718"}) == "rc:816965718"
    assert recipient_ref_for_shopify({"rc_subscription_ids": "1, 2"}) == "rc:1"
    assert recipient_ref_for_shopify({"rc_subscription_ids": None}) is None
    assert recipient_ref_for_cratejoy("7093458554") == "cj:7093458554"
    assert recipient_ref_for_cratejoy("") is None


# ─── resolve_recipient_profile ───

def _row(rid, first, last, ref=None, recipient=None):
    return {"id": rid, "first_name": first, "last_name": last, "recipient_ref": ref, "recipient_name": recipient}


def test_resolve_no_rows_creates_new():
    assert resolve_recipient_profile([], "cj:1", "Leah Hudson") == (None, "create_new")


def test_resolve_bednarfive_second_subscription_creates_recipient():
    leah = _row("L", "Susan", "Bednarczyk", ref="cj:7093457035", recipient="Leah Hudson")
    assert resolve_recipient_profile([leah], "cj:7093458554", "Lana Bain") == (None, "create_recipient")
    # Leah's own next box still matches her profile
    assert resolve_recipient_profile([leah], "cj:7093457035", "Leah Hudson") == (leah, "match")


def test_resolve_spouse_name_on_renewal_matches_by_ref_not_split():
    # fatboy96at: profile claimed rc:X; renewal label says Connie Trips
    anthony = _row("A", "Anthony", "Trips", ref="rc:111", recipient="Anthony Trips")
    assert resolve_recipient_profile([anthony], "rc:111", "Connie Trips") == (anthony, "match")


def test_resolve_care_of_name_is_same_person():
    kylie = _row("K", "Kylie", "Russell", recipient="Kylie Russell")
    assert resolve_recipient_profile([kylie], None, "Kylie Russell c/o Wayne Moss") == (kylie, "match")


def test_resolve_legacy_unclaimed_row_is_claimed_by_first_subscription():
    legacy = _row("H", "Holly", "Sweis")  # no ref, no recipient_name (not backfilled)
    assert resolve_recipient_profile([legacy], "rc:555", "Maddy Kinney") == (legacy, "claim")


def test_resolve_name_match_with_new_ref_is_claim():
    row = _row("M", "Maddy", "Kinney", ref="rc:OLD", recipient="Maddy Kinney")
    assert resolve_recipient_profile([row], "rc:NEW", "Maddy Kinney") == (row, "claim")


def test_resolve_split_shopify_account_routes_each_recipient():
    # jessicamariehawley after the split
    haylie = _row("H", "Jessica", "Hawley", ref="rc:816965718", recipient="Haylie Lazar")
    blencoes = _row("B", "The", "Blencoes", ref="rc:819699361", recipient="The Blencoes")
    rows = [haylie, blencoes]
    assert resolve_recipient_profile(rows, "rc:819699361", "The Blencoes") == (blencoes, "match")
    assert resolve_recipient_profile(rows, "rc:816965718", "Haylie Lazar") == (haylie, "match")
    # checkout order for a third, brand-new person
    assert resolve_recipient_profile(rows, None, "New Person") == (None, "create_recipient")


def test_resolve_named_legacy_row_is_not_taken_over_by_someone_else():
    leah = _row("L", "Susan", "Bednarczyk", recipient="Leah Hudson")  # backfilled name, no ref yet
    assert resolve_recipient_profile([leah], "cj:7093458554", "Lana Bain") == (None, "create_recipient")


def test_resolve_single_row_without_ship_name_matches():
    row = _row("X", "Ann", "Frymoyer")
    assert resolve_recipient_profile([row], None, "") == (row, "match")


# ─── VeraCore OrderID (A7) ───

def test_veracore_order_id_unique_per_cratejoy_box():
    from app import veracore_order_id_for
    sept = {"id": "0069318c-x", "platform": "cratejoy", "order_id": "7093457035", "cratejoy_shipment_id": "7093457045"}
    octo = {"id": "aaaa1111-x", "platform": "cratejoy", "order_id": "7093457035", "cratejoy_shipment_id": "7093457046"}
    assert veracore_order_id_for(sept) == ("7093457045", "cratejoy_shipment_id")
    assert veracore_order_id_for(sept)[0] != veracore_order_id_for(octo)[0]
    assert veracore_order_id_for({"id": "b", "platform": "shopify", "order_id": "7548199141665"}) == ("7548199141665", "order_id")
    assert veracore_order_id_for({"id": "68ffd1be-569d", "platform": "cratejoy", "order_id": None}) == ("OBB-68ffd1be", "decision_id")
