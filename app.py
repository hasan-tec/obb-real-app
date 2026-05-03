"""
OBB Curation Engine — Phase 1 Server
Shopify + Cratejoy webhook receivers, Supabase DB, Dashboard UI
"""

import os
import re
import json
import hmac
import hashlib
import base64
import time
import logging
import csv
import io
import uuid
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from fastapi import FastAPI, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from supabase import create_client, Client
import httpx
import gspread
from google.oauth2.service_account import Credentials

# ─── Load env ───
load_dotenv()

# ─── Logging ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("obb")

# ─── Supabase client ───
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY", "")

supabase: Client = None  # type: ignore

def get_supabase() -> Client:
    """Lazy-init Supabase client."""
    global supabase
    if supabase is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            logger.error("SUPABASE_URL or SUPABASE_KEY not set!")
            raise RuntimeError("Supabase credentials not configured")
        logger.info(f"Connecting to Supabase: {SUPABASE_URL}")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized successfully")
    return supabase


# ─── Shopify config ───
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "")
SHOPIFY_CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET", "")
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "")

# ─── Cratejoy config ───
CRATEJOY_CLIENT_ID = os.getenv("CRATEJOY_CLIENT_ID", "")
CRATEJOY_CLIENT_SECRET = os.getenv("CRATEJOY_CLIENT_SECRET", "")

# ─── VeraCore config (Phase 3 — fulfillment push) ───
# When VERACORE_BASE_URL is empty/unset, all VeraCore calls become no-ops and
# the app routes approved decisions to the CSV fallback instead.  This is how
# local-dev + creds-pending periods stay functional.
VERACORE_BASE_URL   = os.getenv("VERACORE_BASE_URL", "").rstrip("/")
VERACORE_USER_ID    = os.getenv("VERACORE_USER_ID", "")
VERACORE_PASSWORD   = os.getenv("VERACORE_PASSWORD", "")
VERACORE_SYSTEM_ID  = os.getenv("VERACORE_SYSTEM_ID", "")
VERACORE_AUTH_MODE  = os.getenv("VERACORE_AUTH_MODE", "basic")  # basic | base64_json | oauth2
# Optional endpoint-path overrides (match Ting's tenant Swagger if non-standard).
VERACORE_INVENTORY_PATH = os.getenv("VERACORE_INVENTORY_PATH", "/inventory")
VERACORE_ORDER_PATH     = os.getenv("VERACORE_ORDER_PATH", "/orders")
VERACORE_SHIPMENT_PATH  = os.getenv("VERACORE_SHIPMENT_PATH", "/shipments")

_vc_client = None

def veracore_enabled() -> bool:
    """True when creds are configured and live VeraCore calls should fire."""
    return bool(VERACORE_BASE_URL and VERACORE_USER_ID and VERACORE_PASSWORD)

def get_veracore_client():
    """
    Lazy-init VeraCoreClient.  Returns None when creds are missing — callers
    MUST handle that (they do — submit_to_veracore short-circuits to no-op).
    """
    global _vc_client
    if not veracore_enabled():
        return None
    if _vc_client is None:
        from veracore_client import VeraCoreClient
        logger.info("[VERACORE] Initializing client for base_url=%s", VERACORE_BASE_URL)
        _vc_client = VeraCoreClient(
            base_url=VERACORE_BASE_URL,
            user_id=VERACORE_USER_ID,
            password=VERACORE_PASSWORD,
            system_id=VERACORE_SYSTEM_ID,
            auth_mode=VERACORE_AUTH_MODE,
            inventory_path=VERACORE_INVENTORY_PATH,
            order_path=VERACORE_ORDER_PATH,
            shipment_path=VERACORE_SHIPMENT_PATH,
        )
    return _vc_client

# ─── App config ───
BASE_URL = os.getenv("BASE_URL", "https://obb-real-d4e16a8bb2ff.herokuapp.com")

# ─── Sort whitelists (prevent SQL injection via sort params) ───
ALLOWED_SORTS_DECISIONS = {"created_at", "trimester", "decision_type", "kit_sku", "status", "customer_name"}
ALLOWED_SORTS_CUSTOMERS = {"first_name", "email", "trimester", "due_date", "clothing_size", "subscription_status", "created_at"}
ALLOWED_SORTS_KITS = {"sku", "trimester", "size_variant", "quantity_available", "age_rank", "name"}

# ─── Google Sheets config ───
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Phase1 Decisions")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = str(Path(__file__).resolve().parent / "service_account.json")

_gsheet_client = None

def get_gsheet():
    """Lazy-init Google Sheets client and return the worksheet."""
    global _gsheet_client
    if not GOOGLE_SHEET_ID:
        logger.warning("[GSHEETS] GOOGLE_SHEET_ID not set, skipping")
        return None
    # Check for credentials: env var JSON first, then local file
    has_env_json = bool(GOOGLE_SERVICE_ACCOUNT_JSON)
    has_file = os.path.exists(SERVICE_ACCOUNT_FILE)
    if not has_env_json and not has_file:
        logger.warning("[GSHEETS] No Google credentials found (no env var or file), skipping")
        return None
    try:
        if _gsheet_client is None:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            if has_env_json:
                import json as _json
                service_info = _json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
                creds = Credentials.from_service_account_info(service_info, scopes=scopes)
                logger.info("[GSHEETS] Using credentials from GOOGLE_SERVICE_ACCOUNT_JSON env var")
            else:
                creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
                logger.info("[GSHEETS] Using credentials from service_account.json file")
            _gsheet_client = gspread.authorize(creds)
            logger.info("[GSHEETS] Google Sheets client authorized successfully")
        sheet = _gsheet_client.open_by_key(GOOGLE_SHEET_ID)
        try:
            worksheet = sheet.worksheet(GOOGLE_SHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=GOOGLE_SHEET_NAME, rows=1000, cols=12)
            # Add header row — matches the columns Ting expects
            worksheet.update('A1:L1', [[
                "received_at", "platform", "customer_name", "email",
                "trimester", "order_type", "assigned_kit", "decision_status",
                "reason", "external_order_id", "due_date", "clothing_size"
            ]])
            logger.info(f"[GSHEETS] Created worksheet '{GOOGLE_SHEET_NAME}' with headers")
        return worksheet
    except Exception as e:
        logger.error(f"[GSHEETS] Error connecting to Google Sheets: {e}", exc_info=True)
        return None


def write_decision_to_sheet(decision_data: dict):
    """Write a NEW decision row to Google Sheets. Non-blocking — logs errors but doesn't raise."""
    try:
        ws = get_gsheet()
        if ws is None:
            logger.info("[GSHEETS] Skipping write — Google Sheets not configured")
            return
        # Column order matches sheet headers exactly:
        # received_at, platform, topic(trimester), customer_name, email,
        # order_type, decision_status, assigned_kit, reason,
        # external_order_id, due_date, clothing_size
        row = [
            decision_data.get("date", date.today().isoformat()),
            decision_data.get("platform", ""),
            f"T{decision_data.get('trimester', '?')}",
            decision_data.get("customer_name", ""),
            decision_data.get("email", ""),
            decision_data.get("order_type", "renewal"),
            decision_data.get("decision_type", ""),
            decision_data.get("kit_sku", "—"),
            decision_data.get("reason", ""),
            decision_data.get("order_id", ""),
            decision_data.get("due_date", ""),
            decision_data.get("clothing_size", ""),
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"[GSHEETS] Wrote decision row for {decision_data.get('email', '?')} — due_date={decision_data.get('due_date', '')}, size={decision_data.get('clothing_size', '')}")
    except Exception as e:
        logger.error(f"[GSHEETS] Error writing to sheet: {e}", exc_info=True)


def update_decision_status_in_sheet(email: str, order_id: str, new_status: str, reason_prefix: str = ""):
    """
    Update an EXISTING row in Google Sheets instead of appending a duplicate.
    Finds the row by email + order_id (searches bottom-to-top to find most recent match).
    Handles both old column layout (email at col D/index 3) and new layout (email at col E/index 4).
    Updates:
      - Column G (decision_status) → new_status
      - Column I (reason) → prepend prefix to existing reason
    Falls back to logging a warning if the row is not found.
    """
    try:
        ws = get_gsheet()
        if ws is None:
            logger.info("[GSHEETS] Skipping update — Google Sheets not configured")
            return

        all_values = ws.get_all_values()
        target_row = None
        target_email = email.strip().lower()

        # Guard: if email doesn't look valid, skip (catches decision_id[:8] fallback)
        if "@" not in target_email:
            logger.warning(f"[GSHEETS] Skipping update — '{email}' is not a valid email")
            return

        target_order_id = str(order_id or "").strip()

        # Search bottom-to-top to find the MOST RECENT matching row
        for idx in range(len(all_values) - 1, 0, -1):  # reverse, skip header at idx 0
            row = all_values[idx]
            # Check email at both old position (col D, index 3) and new position (col E, index 4)
            row_email_old = (row[3] if len(row) > 3 else "").strip().lower()
            row_email_new = (row[4] if len(row) > 4 else "").strip().lower()
            row_order_id = (row[9] if len(row) > 9 else "").strip()
            email_match = (row_email_old == target_email) or (row_email_new == target_email)
            if email_match and row_order_id == target_order_id:
                target_row = idx + 1  # gspread is 1-indexed
                break

        if target_row:
            # Update decision_status column in-place: G (col 7)
            ws.update_cell(target_row, 7, new_status)  # decision_status
            if reason_prefix:
                existing_reason = all_values[target_row - 1][8] if len(all_values[target_row - 1]) > 8 else ""
                # Don't double-prefix if already has it
                if not existing_reason.startswith(f"[{reason_prefix}]"):
                    ws.update_cell(target_row, 9, f"[{reason_prefix}] {existing_reason}")
            logger.info(f"[GSHEETS] Updated row {target_row} for {email} → status={new_status}")
        else:
            logger.warning(f"[GSHEETS] Row not found for email={email}, order_id={order_id} — cannot update status to '{new_status}'")
    except Exception as e:
        logger.error(f"[GSHEETS] Error updating sheet row: {e}", exc_info=True)


def fix_gsheet_headers():
    """Update the Google Sheet header row to match our current column format."""
    try:
        ws = get_gsheet()
        if ws is None:
            return False
        headers = [
            "received_at", "platform", "topic", "customer_name",
            "email", "order_type", "decision_status", "assigned_kit",
            "reason", "external_order_id", "due_date", "clothing_size"
        ]
        ws.update('A1:L1', [headers], value_input_option="USER_ENTERED")
        logger.info("[GSHEETS] ✅ Updated header row to new format")
        return True
    except Exception as e:
        logger.error(f"[GSHEETS] Error fixing headers: {e}", exc_info=True)
        return False


# ─── FastAPI app ───
app = FastAPI(title="OBB Curation Engine")
_BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(_BASE_DIR / "templates"))


# ─── Monthly curation report scheduler ───
# Runs on the 1st of each month at ~6 AM UTC. Uses a lightweight background thread.
_scheduler_started = False

def _monthly_report_scheduler():
    """Background thread that checks once per hour if it's the 1st and triggers the report."""
    import time as _time
    last_run_month = None
    last_inventory_day = None  # Phase 3 — daily VeraCore inventory sync guard
    last_shipment_poll_day = None  # Phase 3 — daily VeraCore shipment poll guard
    while True:
        try:
            now = datetime.utcnow()
            current_month_key = f"{now.year}-{now.month:02d}"
            # Trigger on 1st of month, after 6 AM UTC, only once per month
            if now.day == 1 and now.hour >= 6 and last_run_month != current_month_key:
                logger.info(f"[SCHEDULER] Monthly auto-run triggered for {current_month_key}")
                try:
                    db = get_supabase()
                    from curation_report import run_monthly_report as _sched_run
                    year, month_num = now.year, now.month
                    ship_date = date(year, month_num, 14)  # default ship day
                    report = _sched_run(
                        db=db,
                        report_month=current_month_key,
                        ship_date=ship_date,
                        warehouse_minimum=100,
                        include_paused=False,
                        lookback_months=4,
                        recency_months=3,
                    )
                    # Save to DB
                    run_insert = db.table("curation_runs").insert({
                        "report_month": current_month_key,
                        "ship_date": str(ship_date),
                        "warehouse_minimum": 100,
                        "include_paused": False,
                        "lookback_months": 4,
                        "status": "completed",
                        "summary_json": report["executive"],
                        "completed_at": str(date.today()),
                    }).execute()
                    run_id = run_insert.data[0]["id"]
                    _save_report_details(db, run_id, report)
                    logger.info(f"[SCHEDULER] Auto-generated curation report for {current_month_key}, run_id={run_id}")
                    last_run_month = current_month_key
                except Exception as e:
                    logger.error(f"[SCHEDULER] Failed to auto-generate report: {e}", exc_info=True)

            # Phase 3 — Daily VeraCore inventory sync at ~04:00 UTC (≈11 PM ET).
            # Skipped entirely when VeraCore is not configured.
            current_day_key = now.strftime("%Y-%m-%d")
            if (veracore_enabled()
                    and now.hour == 4
                    and last_inventory_day != current_day_key):
                logger.info("[SCHEDULER] Daily VeraCore inventory sync triggered for %s", current_day_key)
                try:
                    db = get_supabase()
                    vc = get_veracore_client()
                    if vc is not None:
                        from veracore_sync import run_inventory_sync
                        inv_result = run_inventory_sync(db, vc)
                        logger.info("[SCHEDULER] Inventory sync result: %s", inv_result)
                    last_inventory_day = current_day_key
                except Exception as e:
                    logger.error("[SCHEDULER] VeraCore inventory sync failed: %s", e, exc_info=True)

            # Phase 3 — Daily VeraCore shipment poll at ~05:00 UTC (1 hour after inventory).
            if (veracore_enabled()
                    and now.hour == 5
                    and last_shipment_poll_day != current_day_key):
                logger.info("[SCHEDULER] Daily VeraCore shipment poll triggered for %s", current_day_key)
                try:
                    db = get_supabase()
                    vc = get_veracore_client()
                    if vc is not None:
                        from veracore_sync import run_shipment_poll
                        poll_result = run_shipment_poll(db, vc)
                        logger.info("[SCHEDULER] Shipment poll result: %s", poll_result)
                    last_shipment_poll_day = current_day_key
                except Exception as e:
                    logger.error("[SCHEDULER] VeraCore shipment poll failed: %s", e, exc_info=True)
        except Exception as e:
            logger.error(f"[SCHEDULER] Scheduler loop error: {e}", exc_info=True)
        _time.sleep(3600)  # Check every hour


def _start_scheduler():
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
        t = threading.Thread(target=_monthly_report_scheduler, daemon=True, name="monthly-scheduler")
        t.start()
        logger.info("[SCHEDULER] Monthly curation report scheduler started")


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def verify_shopify_hmac(body: bytes, hmac_header: str) -> bool:
    """Verify Shopify webhook HMAC-SHA256 signature."""
    if not SHOPIFY_WEBHOOK_SECRET:
        logger.warning("SHOPIFY_WEBHOOK_SECRET not set, skipping HMAC verification")
        return True
    computed = base64.b64encode(
        hmac.new(
            SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
            body,
            hashlib.sha256
        ).digest()
    ).decode("utf-8")
    valid = hmac.compare_digest(computed, hmac_header)
    if not valid:
        logger.warning(f"HMAC verification failed. Expected: {computed}, Got: {hmac_header}")
    return valid


def calculate_trimester(due_date: date, ship_date: date) -> int:
    """
    OBB trimester calculation:
    ship_date + 19 days = T4 (postpartum)
    + 13 weeks from T4 cutoff = T3
    + 14 weeks from T3 boundary = T2
    beyond T2 boundary = T1
    """
    t4_cutoff = ship_date + timedelta(days=19)
    t3_cutoff = t4_cutoff + timedelta(weeks=13)
    t2_cutoff = t3_cutoff + timedelta(weeks=14)

    if due_date <= t4_cutoff:
        return 4  # Postpartum
    elif due_date <= t3_cutoff:
        return 3
    elif due_date <= t2_cutoff:
        return 2
    else:
        return 1


async def log_activity(type_: str, summary: str, detail: str = "", result: str = "success"):
    """Log activity to Supabase activity_log table."""
    try:
        db = get_supabase()
        db.table("activity_log").insert({
            "type": type_,
            "summary": summary,
            "detail": detail,
            "result": result,
        }).execute()
        logger.info(f"[ACTIVITY] [{result.upper()}] {type_}: {summary}")
    except Exception as e:
        logger.error(f"Failed to log activity: {e}")


def normalize_clothing_size(raw_size: str) -> Optional[str]:
    """Normalize clothing size strings to S/M/L/XL. Handles 'med', 'lrg', etc."""
    if not raw_size:
        return None
    s = raw_size.strip().lower()
    size_map = {
        "s": "S", "sm": "S", "small": "S",
        "m": "M", "med": "M", "medium": "M",
        "l": "L", "lg": "L", "lrg": "L", "large": "L",
        "xl": "XL", "x-large": "XL", "xlarge": "XL", "x-lg": "XL",
        # XXL maps to XL — OBB groups XXL+XL into the same kit variant (per spreadsheet data)
        "xxl": "XL", "2xl": "XL", "xx-large": "XL", "xxlarge": "XL",
    }
    result = size_map.get(s)
    if not result:
        logger.warning(f"[SIZE] Unknown clothing size value: '{raw_size}'")
    return result


def extract_quiz_data(note_attributes: list, line_items: list) -> dict:
    """
    Extract quiz data from Shopify order.

    OBB note_attribute names (from Shopify 'Additional Details'):
      q_due_date, q_size, q_expecting, q_second_parent, q_past_experience
    Recharge fields: rc_charge_id, rc_subscription_ids, rc_address_id
    """
    quiz = {
        "due_date_str": None,
        "clothing_size": None,
        "baby_gender": None,
        "wants_daddy": False,
        "previous_obb": False,
        "rc_charge_id": None,
        "rc_subscription_ids": None,
        "subscription_plan": None,
        "is_gift": False,
    }

    # --- note_attributes (primary source) ---
    for attr in (note_attributes or []):
        name = str(attr.get("name") or "").strip().lower()
        value = str(attr.get("value") or "").strip()
        if not value:
            continue

        # Due date: q_due_date or anything with "due" + "date"
        if name == "q_due_date" or ("due" in name and "date" in name):
            quiz["due_date_str"] = value
            logger.info(f"[QUIZ] Found due_date: '{value}' from attr '{name}'")
        # Size: q_size or "size"/"clothing"
        elif name == "q_size" or "size" in name or "clothing" in name:
            quiz["clothing_size"] = normalize_clothing_size(value)
            logger.info(f"[QUIZ] Found size: '{value}' → normalized: {quiz['clothing_size']}")
        # Gender/expecting: q_expecting or "gender"/"expecting"
        elif name == "q_expecting" or "gender" in name or "expecting" in name:
            quiz["baby_gender"] = value
            logger.info(f"[QUIZ] Found gender/expecting: '{value}' from attr '{name}'")
        # Daddy/second parent: q_second_parent or "daddy"/"second_parent"
        elif name == "q_second_parent" or "daddy" in name or "second_parent" in name:
            quiz["wants_daddy"] = "yes" in value.lower() or "daddy" in value.lower()
            logger.info(f"[QUIZ] Found second_parent: '{value}' → wants_daddy={quiz['wants_daddy']}")
        # Past experience: q_past_experience or "previous"/"past_experience"
        elif name == "q_past_experience" or "past" in name or ("previous" in name):
            quiz["previous_obb"] = value.lower() in ("yes", "true", "1")
            logger.info(f"[QUIZ] Found past_experience: '{value}' → previous_obb={quiz['previous_obb']}")
        # Recharge fields
        elif name == "rc_charge_id":
            quiz["rc_charge_id"] = value
        elif name == "rc_subscription_ids":
            quiz["rc_subscription_ids"] = value

    # --- line_item properties (fallback) ---
    for item in (line_items or []):
        for prop in (item.get("properties") or []):
            name = (prop.get("name") or "").strip().lower()
            value = (prop.get("value") or "").strip()
            if not value:
                continue
            if ("due" in name and "date" in name) and not quiz["due_date_str"]:
                quiz["due_date_str"] = value
            elif ("size" in name) and not quiz["clothing_size"]:
                quiz["clothing_size"] = normalize_clothing_size(value)
            elif ("gender" in name or "expecting" in name) and not quiz["baby_gender"]:
                quiz["baby_gender"] = value
            elif ("daddy" in name or "second_parent" in name) and not quiz["wants_daddy"]:
                quiz["wants_daddy"] = "yes" in value.lower()

        # Detect subscription plan from line item SKU
        item_sku = (item.get("sku") or "").upper()
        if "SUBPLAN" in item_sku:
            quiz["subscription_plan"] = item_sku
            quiz["is_gift"] = "GIFT" in item_sku
            logger.info(f"[QUIZ] Found subscription plan: {item_sku}, is_gift={quiz['is_gift']}")

    return quiz


def parse_due_date(due_date_str: str) -> Optional[date]:
    """Parse due date from various formats."""
    if not due_date_str:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(due_date_str, fmt).date()
        except ValueError:
            continue
    logger.warning(f"[PARSE] Could not parse due_date: '{due_date_str}'")
    return None


def compute_age_rank_from_sku(sku: str) -> int:
    """
    Auto-compute FIFO age rank from kit SKU prefix.

    Handles both old format (CK41) and VeraCore format (OBB-CK-41 KITS).

    Cleaning steps:
      1. Strip leading 'RW-' or 'RW' (reworked kit marker)
      2. Strip leading 'OBB-' (VeraCore prefix)
      3. Strip trailing ' KITS' or ' KIT' suffix
      4. Remove internal dashes (WK-C1 → WKC1)
      5. Extract alpha prefix = all letters before the first digit

    Rank rules:
      - Welcome kits (prefix starts with 'WK'):
          'WK'  alone       → 10001 (oldest WK)
          'WKA'             → 10002
          ...
          'WKH'             → 10009
      - Regular kits (alphabetical batches):
          Single letter A-Z → rank 1-26
          Double letters AA-ZZ:
            rank = (pos_of_first_letter) * 26 + (pos_of_second_letter)
            AA=27, AB=28 ... AZ=52, BA=53 ... CK=89
      - Returns 0 if the prefix can't be parsed (keeps manual override).
    """
    if not sku:
        return 0

    clean = sku.strip().upper()

    # Strip reworked prefix
    if clean.startswith("RW-"):
        clean = clean[3:]
    elif clean.startswith("RW"):
        clean = clean[2:]

    # Strip VeraCore prefix and suffix (OBB-WK-C1 KITS → WKC1)
    if clean.startswith("OBB-"):
        clean = clean[4:]
        logger.debug(f"[AGE_RANK] Stripped OBB- prefix → '{clean}'")
    # Strip trailing ' KITS' or ' KIT' suffix
    if clean.endswith(" KITS"):
        clean = clean[:-5]
    elif clean.endswith(" KIT"):
        clean = clean[:-4]
    # Remove internal dashes (WK-C1 → WKC1)
    clean = clean.replace("-", "")
    logger.debug(f"[AGE_RANK] Cleaned SKU: '{sku}' → '{clean}'")

    # Extract alpha prefix (letters before first digit)
    prefix = ""
    for ch in clean:
        if ch.isalpha():
            prefix += ch
        else:
            break

    if not prefix:
        logger.warning(f"[AGE_RANK] Could not extract alpha prefix from SKU '{sku}' — returning 0")
        return 0

    # Welcome kits: WK, WKA … WKZ
    if prefix.startswith("WK"):
        suffix = prefix[2:]  # everything after 'WK'
        if suffix == "":
            return 10001
        if len(suffix) == 1 and suffix.isalpha():
            return 10001 + (ord(suffix) - ord("A") + 1)
        # Long WK prefix — fall through to 0
        logger.warning(f"[AGE_RANK] Unrecognised WK prefix '{prefix}' for SKU '{sku}'")
        return 0

    # BP special T1 box — treat as regular double-letter
    # Regular batches: single or double letters
    if len(prefix) == 1:
        return ord(prefix[0]) - ord("A") + 1  # A=1 … Z=26

    if len(prefix) == 2:
        first = ord(prefix[0]) - ord("A") + 1
        second = ord(prefix[1]) - ord("A") + 1
        return first * 26 + second  # AA=27, BT=72, CK=89

    # Triple-letter and beyond (future-proofing)
    if len(prefix) == 3:
        a = ord(prefix[0]) - ord("A") + 1
        b = ord(prefix[1]) - ord("A") + 1
        c = ord(prefix[2]) - ord("A") + 1
        return a * 676 + b * 26 + c

    logger.warning(f"[AGE_RANK] Prefix '{prefix}' too long for SKU '{sku}' — returning 0")
    return 0


def parse_history_item_refs(raw_value: str) -> list[str]:
    """Split comma/newline separated item refs for manual shipment history entry."""
    if not raw_value:
        return []
    refs = []
    for chunk in re.split(r"[\n,]+", raw_value):
        ref = chunk.strip()
        if ref:
            refs.append(ref)
    return refs


def resolve_history_item_ids(raw_value: str) -> tuple[list[str], list[str]]:
    """Resolve manual item refs to item IDs using exact SKU or exact item name."""
    refs = parse_history_item_refs(raw_value)
    if not refs:
        return [], []

    db = get_supabase()
    resolved_item_ids: list[str] = []
    unresolved_refs: list[str] = []
    seen_item_ids: set[str] = set()

    for ref in refs:
        item_row = None
        sku_match = db.table("items").select("id, name, sku").eq("sku", ref.upper()).execute()
        if sku_match.data:
            item_row = sku_match.data[0]
        else:
            name_match = db.table("items").select("id, name, sku").ilike("name", ref).execute()
            if name_match.data:
                item_row = name_match.data[0]

        if item_row:
            item_id = item_row["id"]
            if item_id not in seen_item_ids:
                seen_item_ids.add(item_id)
                resolved_item_ids.append(item_id)
                logger.info(
                    f"[SHIPMENT ITEMS] Resolved manual item ref '{ref}' -> "
                    f"{item_row.get('name') or item_row.get('sku') or item_id}"
                )
        else:
            unresolved_refs.append(ref)
            logger.warning(f"[SHIPMENT ITEMS] Could not resolve manual item ref '{ref}'")

    return resolved_item_ids, unresolved_refs


def populate_shipment_items(shipment_id: Optional[str], kit_id: Optional[str], manual_item_refs: str = "") -> tuple[int, list[str]]:
    """Populate shipment_items from kit_items and optional manual item refs."""
    if not shipment_id:
        logger.warning("[SHIPMENT ITEMS] Missing shipment_id, cannot populate shipment items")
        return 0, []

    db = get_supabase()
    added_item_ids: set[str] = set()
    unresolved_refs: list[str] = []

    if kit_id:
        kit_items = db.table("kit_items").select("item_id").eq("kit_id", kit_id).execute()
        logger.info(f"[SHIPMENT ITEMS] Kit {kit_id} has {len(kit_items.data or [])} mapped items")
        for kit_item in (kit_items.data or []):
            item_id = kit_item["item_id"]
            if item_id in added_item_ids:
                continue
            try:
                db.table("shipment_items").insert({
                    "shipment_id": shipment_id,
                    "item_id": item_id,
                }).execute()
                added_item_ids.add(item_id)
            except Exception as item_err:
                logger.warning(f"[SHIPMENT ITEMS] Could not add mapped kit item {item_id}: {item_err}")

    manual_item_ids, unresolved_refs = resolve_history_item_ids(manual_item_refs)
    if manual_item_ids:
        logger.info(f"[SHIPMENT ITEMS] Adding {len(manual_item_ids)} manual history item(s) to shipment {shipment_id}")
    for item_id in manual_item_ids:
        if item_id in added_item_ids:
            continue
        try:
            db.table("shipment_items").insert({
                "shipment_id": shipment_id,
                "item_id": item_id,
            }).execute()
            added_item_ids.add(item_id)
        except Exception as item_err:
            logger.warning(f"[SHIPMENT ITEMS] Could not add manual history item {item_id}: {item_err}")

    logger.info(
        f"[SHIPMENT ITEMS] Shipment {shipment_id} now has {len(added_item_ids)} recorded item(s); "
        f"unresolved refs={unresolved_refs}"
    )
    return len(added_item_ids), unresolved_refs


def load_customer_shipments_with_items(customer_id: str) -> list[dict]:
    """Load a customer's shipments plus the recorded items inside each shipment."""
    db = get_supabase()
    shipments = db.table("shipments").select("*").eq("customer_id", customer_id).order("created_at", desc=True).execute()
    shipment_rows = shipments.data or []
    enriched_shipments: list[dict] = []

    for shipment in shipment_rows:
        shipment_items = db.table("shipment_items").select("item_id, items(*)").eq("shipment_id", shipment["id"]).execute()
        shipment_row = dict(shipment)
        shipment_row["shipment_items"] = shipment_items.data or []
        shipment_row["shipment_item_text"] = ", ".join(
            (si.get("items") or {}).get("sku") or (si.get("items") or {}).get("name") or si.get("item_id") or ""
            for si in shipment_row["shipment_items"]
            if (si.get("items") or {}).get("sku") or (si.get("items") or {}).get("name") or si.get("item_id")
        )
        enriched_shipments.append(shipment_row)

    logger.info(f"[CUSTOMER DETAIL] Loaded {len(enriched_shipments)} shipment(s) with item history for customer {customer_id}")
    return enriched_shipments


async def assign_kit(customer_id: str, ship_date_val: date) -> dict:
    """
    Core decision engine: Assign the best kit for a customer.

    1. Get customer → trimester
    2. Get customer's item history from past shipments
    3. Get available kits for trimester with stock > 0
    4. Filter by size variant
    5. Exclude kits with duplicate items (including alternatives)
    6. Sort by age_rank (FIFO — oldest first)
    7. Return best match or needs-curation
    """
    db = get_supabase()

    # 1. Get customer
    customer_result = db.table("customers").select("*").eq("id", customer_id).single().execute()
    if not customer_result.data:
        logger.error(f"[DECISION ENGINE] Customer {customer_id} not found")
        return {"decision_type": "incomplete-data", "reason": "Customer not found", "kit_id": None, "kit_sku": None}

    cust = customer_result.data
    trimester = cust.get("trimester")
    if not trimester:
        logger.warning(f"[DECISION ENGINE] Customer {customer_id} has no trimester (missing due date)")
        return {"decision_type": "incomplete-data", "reason": "No trimester — missing due date", "kit_id": None, "kit_sku": None}

    clothing_size = cust.get("clothing_size")
    # Re-normalize at engine time — guards against old/raw DB values (e.g. "xxl", "med", "lrg")
    if clothing_size:
        clothing_size = normalize_clothing_size(clothing_size) or clothing_size
    logger.info(f"[DECISION ENGINE] Customer: {cust.get('email')}, T{trimester}, Size: {clothing_size or 'unknown (no size on record)'}")

    # 2. Get customer's item history from past shipments
    shipments = db.table("shipments").select("id, kit_sku").eq("customer_id", customer_id).execute()
    received_item_ids = set()
    received_kit_skus = set()

    for ship in (shipments.data or []):
        if ship.get("kit_sku"):
            received_kit_skus.add(ship["kit_sku"])
        ship_items = db.table("shipment_items").select("item_id").eq("shipment_id", ship["id"]).execute()
        for si in (ship_items.data or []):
            received_item_ids.add(si["item_id"])

    logger.info(f"[DECISION ENGINE] History: {len(received_kit_skus)} kits received, {len(received_item_ids)} items received")

    # Build blocked items list (received items + their alternatives)
    blocked_item_ids = set(received_item_ids)
    if received_item_ids:
        for item_id in list(received_item_ids):
            alts = db.table("item_alternatives").select("alternative_item_id").eq("item_id", item_id).execute()
            for alt in (alts.data or []):
                blocked_item_ids.add(alt["alternative_item_id"])
            alts_rev = db.table("item_alternatives").select("item_id").eq("alternative_item_id", item_id).execute()
            for alt in (alts_rev.data or []):
                blocked_item_ids.add(alt["item_id"])

    if blocked_item_ids:
        logger.info(f"[DECISION ENGINE] Blocked items: {len(blocked_item_ids)} (incl {len(blocked_item_ids) - len(received_item_ids)} alternatives)")

    # 3. Get available kits for this trimester
    is_new = len(shipments.data or []) == 0
    logger.info(f"[DECISION ENGINE] Customer is {'NEW (→ welcome kits)' if is_new else 'RENEWAL (→ regular kits)'}")

    if is_new:
        kits_result = db.table("kits").select("*").eq("trimester", trimester).eq("is_welcome_kit", True).gt("quantity_available", 0).order("age_rank").execute()
    else:
        kits_result = db.table("kits").select("*").eq("trimester", trimester).eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()

    available_kits = kits_result.data or []
    logger.info(f"[DECISION ENGINE] Found {len(available_kits)} {'welcome' if is_new else 'regular'} kits for T{trimester} with stock > 0")

    if not available_kits:
        kit_type = "welcome" if is_new else "regular"
        return {
            "decision_type": "needs-curation",
            "reason": f"No {kit_type} kits with stock > 0 for T{trimester}. Add kits in the Kits page first.",
            "kit_id": None,
            "kit_sku": None,
        }

    # 4. Filter by size variant
    # is_universal=True  → kit has no sized items, goes to ALL customers regardless of size
    # is_universal=False → kit has a sized item; match customer's size
    # size_to_variant: S=1, M=2, L=3, XL=4 (matches SKU second digit convention)
    if clothing_size:
        size_to_variant = {"S": 1, "M": 2, "L": 3, "XL": 4}
        customer_variant = size_to_variant.get(clothing_size, 1)
        filtered = [k for k in available_kits if k.get("is_universal") or k["size_variant"] == customer_variant]
        logger.info(f"[DECISION ENGINE] Size filter: clothing_size={clothing_size}, customer_variant={customer_variant}")
    else:
        # No size info → universal kits ONLY
        # size_variant=1 with is_universal=False are S-only kits — wrong to assign to unknown-size customers
        filtered = [k for k in available_kits if k.get("is_universal")]
        logger.info(f"[DECISION ENGINE] No size on record → universal-only kits ({len(filtered)} available)")

    logger.info(f"[DECISION ENGINE] After size filter: {len(filtered)} kits (customer size: {clothing_size or 'universal-only'})")

    if not filtered:
        return {
            "decision_type": "needs-curation",
            "reason": f"No kits match size {clothing_size or 'universal'} for T{trimester}. {len(available_kits)} kits exist but wrong size.",
            "kit_id": None,
            "kit_sku": None,
        }

    # 5. Check for duplicate items per kit
    valid_kits = []
    for kit in filtered:
        # Skip if already received this exact kit
        if kit["sku"] in received_kit_skus:
            logger.info(f"[DECISION ENGINE] Kit {kit['sku']} excluded: already received by customer")
            continue

        # Get this kit's items
        kit_items_result = db.table("kit_items").select("item_id").eq("kit_id", kit["id"]).execute()
        kit_item_ids = {ki["item_id"] for ki in (kit_items_result.data or [])}

        # If kit has no items mapped yet, allow it (items not set up yet)
        if not kit_item_ids:
            logger.info(f"[DECISION ENGINE] Kit {kit['sku']} has no items mapped — allowing (items not configured)")
            valid_kits.append(kit)
            continue

        # Check item overlap with blocked items
        overlap = kit_item_ids & blocked_item_ids
        if overlap:
            logger.info(f"[DECISION ENGINE] Kit {kit['sku']} excluded: {len(overlap)} duplicate items")
            continue

        valid_kits.append(kit)

    logger.info(f"[DECISION ENGINE] After duplicate check: {len(valid_kits)} valid kits remaining")

    if not valid_kits:
        return {
            "decision_type": "needs-curation",
            "reason": f"All T{trimester} kits have duplicate items with customer history. "
                      f"Checked {len(filtered)} kits, blocked {len(blocked_item_ids)} items. "
                      f"Customer received {len(received_kit_skus)} previous kits.",
            "kit_id": None,
            "kit_sku": None,
        }

    # 6. Sort by age_rank (FIFO — lowest = oldest) and pick first
    valid_kits.sort(key=lambda k: k.get("age_rank", 0))
    chosen = valid_kits[0]

    logger.info(f"[DECISION ENGINE] ✅ Assigned: {chosen['sku']} (age_rank={chosen['age_rank']}, "
                f"qty={chosen['quantity_available']}, {'welcome' if chosen.get('is_welcome_kit') else 'regular'})")

    return {
        "decision_type": "auto",
        "reason": (
            f"Assigned {chosen['sku']} — age rank {chosen['age_rank']}, "
            f"{'welcome' if chosen.get('is_welcome_kit') else 'regular'} kit for T{trimester}, "
            f"size: {'universal' if chosen.get('is_universal') else clothing_size}. "
            f"{len(valid_kits)} valid kit(s), {len(filtered)} checked."
        ),
        "kit_id": chosen["id"],
        "kit_sku": chosen["sku"],
    }


# ═══════════════════════════════════════════════════════════
# SHOPIFY WEBHOOK ENDPOINT
# ═══════════════════════════════════════════════════════════

@app.post("/webhooks/shopify/orders/create")
async def shopify_order_webhook(request: Request):
    """
    Receives Shopify orders/create webhook.
    1. Verify HMAC
    2. Log raw payload
    3. Extract customer + order info
    4. Upsert customer to Supabase
    5. Log activity
    """
    start_time = time.time()
    body = await request.body()
    logger.info(f"[SHOPIFY WEBHOOK] Received webhook, body size: {len(body)} bytes")

    # ─── HMAC Verification ───
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if hmac_header and not verify_shopify_hmac(body, hmac_header):
        logger.error("[SHOPIFY WEBHOOK] HMAC verification FAILED — rejecting")
        await log_activity("webhook", "Shopify webhook HMAC failed", "", "error")
        return JSONResponse({"error": "HMAC verification failed"}, status_code=401)

    # ─── Parse payload ───
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error(f"[SHOPIFY WEBHOOK] JSON decode error: {e}")
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    # ─── Idempotency check ───
    event_id = request.headers.get("X-Shopify-Webhook-Id", "")
    shopify_topic = request.headers.get("X-Shopify-Topic", "orders/create")
    shopify_order_id = str(payload.get("id", ""))
    logger.info(f"[SHOPIFY WEBHOOK] Event ID: {event_id}, Topic: {shopify_topic}, Order ID: {shopify_order_id}")

    db = get_supabase()

    # Check for duplicate event
    if event_id:
        existing = db.table("webhook_logs").select("id").eq("source", "shopify").eq("event_id", event_id).execute()
        if existing.data:
            logger.info(f"[SHOPIFY WEBHOOK] Duplicate event {event_id} — skipping")
            return JSONResponse({"status": "duplicate"}, status_code=200)

    # ─── Log raw webhook ───
    headers_dict = {
        "X-Shopify-Topic": shopify_topic,
        "X-Shopify-Hmac-Sha256": hmac_header[:20] + "..." if hmac_header else "",
        "X-Shopify-Shop-Domain": request.headers.get("X-Shopify-Shop-Domain", ""),
        "X-Shopify-Webhook-Id": event_id,
    }

    webhook_log = db.table("webhook_logs").insert({
        "source": "shopify",
        "event_type": shopify_topic,
        "event_id": event_id or None,
        "payload": payload,
        "headers": headers_dict,
        "status": "received",
    }).execute()
    webhook_log_id = webhook_log.data[0]["id"] if webhook_log.data else None
    logger.info(f"[SHOPIFY WEBHOOK] Logged webhook, log_id: {webhook_log_id}")

    # ─── Extract customer info ───
    try:
        customer_data = payload.get("customer", {})
        email = (customer_data.get("email") or payload.get("email") or "").strip().lower()
        first_name = customer_data.get("first_name", "")
        last_name = customer_data.get("last_name", "")
        shopify_customer_id = str(customer_data.get("id", ""))
        phone = customer_data.get("phone", "")

        # Extract shipping address
        # NOTE: Must use `or {}` — when shipping_address is explicitly null in JSON,
        # dict.get(key, default) returns None, not the default value.
        shipping = payload.get("shipping_address") or {}
        address_line1 = shipping.get("address1", "")
        city = shipping.get("city", "")
        province = shipping.get("province", "")
        zip_code = shipping.get("zip", "")
        country = shipping.get("country_code", "US")
        logger.info(f"[SHOPIFY WEBHOOK] shipping_address present: {bool(shipping)}")

        # Extract quiz data using helper (handles q_due_date, q_size, q_expecting, etc.)
        line_items = payload.get("line_items", [])
        note_attributes = payload.get("note_attributes", [])
        quiz = extract_quiz_data(note_attributes, line_items)

        due_date_str = quiz["due_date_str"]
        clothing_size = quiz["clothing_size"]
        baby_gender = quiz["baby_gender"]
        wants_daddy = quiz["wants_daddy"]
        previous_obb = quiz["previous_obb"]

        # Parse due date
        due_date = parse_due_date(due_date_str)
        if due_date:
            logger.info(f"[SHOPIFY WEBHOOK] Parsed due_date: {due_date} from '{due_date_str}'")

        # Calculate trimester
        trimester = None
        if due_date:
            trimester = calculate_trimester(due_date, date.today())
            logger.info(f"[SHOPIFY WEBHOOK] Calculated trimester: T{trimester} for due_date {due_date}")

        # Determine order type
        total_price = float(payload.get("total_price", "0") or "0")
        source_name = payload.get("source_name", "")
        is_renewal = (total_price == 0) or (not source_name) or (source_name not in ("web", "shopify_draft_order"))

        # Override: if there's a subscription plan SKU, it's a new subscription
        if quiz["subscription_plan"]:
            is_renewal = False
            logger.info(f"[SHOPIFY WEBHOOK] Subscription plan detected: {quiz['subscription_plan']} (gift={quiz['is_gift']})")

        # ─── Detect if this is a subscription-related order ───
        # Indicators: $0 price (Recharge renewal), subscription SKU, or Recharge note attrs
        is_recharge_renewal = (total_price == 0.0)
        has_sub_sku = quiz["subscription_plan"] is not None
        has_rc_attrs = bool(quiz["rc_charge_id"] or quiz["rc_subscription_ids"])
        is_subscription_order = is_recharge_renewal or has_sub_sku or has_rc_attrs
        logger.info(
            f"[SHOPIFY WEBHOOK] is_subscription_order={is_subscription_order} "
            f"(recharge_renewal={is_recharge_renewal}, sub_sku={has_sub_sku}, rc_attrs={has_rc_attrs})"
        )
        if not is_subscription_order:
            logger.info(
                f"[SHOPIFY WEBHOOK] NON-SUBSCRIPTION order — "
                f"line item SKUs: {[i.get('sku', '') for i in line_items]}, price=${total_price}, source={source_name}. "
                f"Will update existing customer (address only) but skip creating new customers and decision engine."
            )

        logger.info(f"[SHOPIFY WEBHOOK] Customer: {email}, Name: {first_name} {last_name}")
        logger.info(f"[SHOPIFY WEBHOOK] Price: ${total_price}, Source: {source_name}, Is Renewal: {is_renewal}")
        logger.info(f"[SHOPIFY WEBHOOK] Due Date: {due_date}, Size: {clothing_size}, Gender: {baby_gender}, Daddy: {wants_daddy}")

        # ─── Upsert customer ───
        if email:
            existing_customer = db.table("customers").select("*").ilike("email", email).execute()

            # Non-subscription order from unknown email → skip entirely (no ghost customers)
            if not is_subscription_order and not existing_customer.data:
                logger.info(f"[SHOPIFY WEBHOOK] Non-sub order from unknown email {email} — skipping entirely, no customer created")
                await log_activity("webhook", f"Non-sub order skipped — unknown email {email}", f"order={shopify_order_id}, SKUs={[i.get('sku','') for i in line_items]}", "info")
                return JSONResponse({"status": "skipped", "reason": "non-subscription order from unknown customer"})

            customer_record = {
                "email": email,
                "first_name": first_name or None,
                "last_name": last_name or None,
                "shopify_customer_id": shopify_customer_id or None,
                "phone": phone or None,
                "address_line1": address_line1 or None,
                "city": city or None,
                "province": province or None,
                "zip": zip_code or None,
                "country": country or "US",
            }

            if due_date:
                customer_record["due_date"] = due_date.isoformat()
            if trimester:
                customer_record["trimester"] = trimester
            if clothing_size:
                customer_record["clothing_size"] = clothing_size
            if baby_gender:
                customer_record["baby_gender"] = baby_gender
            customer_record["wants_daddy_item"] = wants_daddy

            if existing_customer.data:
                cust_id = existing_customer.data[0]["id"]
                if existing_customer.data[0].get("cratejoy_customer_id"):
                    customer_record["platform"] = "both"
                else:
                    customer_record["platform"] = "shopify"
                # Non-sub order for existing customer → update address fields only, don't overwrite quiz data
                if not is_subscription_order:
                    address_only = {k: customer_record[k] for k in ("email", "phone", "address_line1", "city", "province", "zip", "country", "platform") if k in customer_record}
                    db.table("customers").update(address_only).eq("id", cust_id).execute()
                    logger.info(f"[SHOPIFY WEBHOOK] Non-sub order — updated address only for existing customer: {cust_id}")
                else:
                    db.table("customers").update(customer_record).eq("id", cust_id).execute()
                    logger.info(f"[SHOPIFY WEBHOOK] Updated existing customer: {cust_id}")
            else:
                customer_record["platform"] = "shopify"
                customer_record["subscription_status"] = "active"
                result = db.table("customers").insert(customer_record).execute()
                cust_id = result.data[0]["id"] if result.data else None
                logger.info(f"[SHOPIFY WEBHOOK] Created new customer: {cust_id}")

            # ─── Run Decision Engine (subscription orders only) ───
            if cust_id and is_subscription_order:
                kit_decision = await assign_kit(cust_id, date.today())
                logger.info(f"[SHOPIFY WEBHOOK] Decision engine result: {kit_decision['decision_type']} — {kit_decision.get('kit_sku', 'none')}")

                decision = {
                    "customer_id": cust_id,
                    "kit_id": kit_decision.get("kit_id"),
                    "kit_sku": kit_decision.get("kit_sku"),
                    "decision_type": kit_decision["decision_type"],
                    "reason": kit_decision["reason"],
                    "status": "pending",
                    "order_id": shopify_order_id,
                    "platform": "shopify",
                    "trimester": trimester,
                    "ship_date": date.today().isoformat(),
                }
                db.table("decisions").insert(decision).execute()
                logger.info(f"[SHOPIFY WEBHOOK] Saved decision: {kit_decision['decision_type']}, kit: {kit_decision.get('kit_sku', 'none')}")

                # ─── Write to Google Sheets ───
                write_decision_to_sheet({
                    "date": date.today().isoformat(),
                    "customer_name": f"{first_name or ''} {last_name or ''}".strip(),
                    "email": email,
                    "platform": "shopify",
                    "trimester": trimester,
                    "order_type": "renewal" if is_renewal else "new",
                    "kit_sku": kit_decision.get("kit_sku", "—"),
                    "decision_type": kit_decision["decision_type"],
                    "reason": kit_decision["reason"],
                    "order_id": str(shopify_order_id),
                    "due_date": due_date.isoformat() if due_date else "",
                    "clothing_size": clothing_size or "",
                })

        # ─── Update webhook log status ───
        elapsed_ms = int((time.time() - start_time) * 1000)
        if webhook_log_id:
            db.table("webhook_logs").update({
                "status": "processed",
                "processing_time_ms": elapsed_ms,
            }).eq("id", webhook_log_id).execute()

        await log_activity(
            "webhook",
            f"Shopify order #{shopify_order_id} processed",
            f"Customer: {email}, Trimester: T{trimester or '?'}, "
            f"Type: {'Renewal' if is_renewal else 'New'}, sub_order={is_subscription_order}",
            "success"
        )

        logger.info(f"[SHOPIFY WEBHOOK] Processing complete in {elapsed_ms}ms")
        return JSONResponse({"status": "ok", "processing_time_ms": elapsed_ms})

    except Exception as e:
        logger.error(f"[SHOPIFY WEBHOOK] Error processing order: {e}", exc_info=True)
        if webhook_log_id:
            db.table("webhook_logs").update({
                "status": "failed",
                "error_message": str(e),
                "processing_time_ms": int((time.time() - start_time) * 1000),
            }).eq("id", webhook_log_id).execute()
        await log_activity("webhook", f"Shopify webhook failed: {e}", "", "error")
        return JSONResponse({"status": "ok"}, status_code=200)  # Always return 200 to Shopify


# ═══════════════════════════════════════════════════════════
# CRATEJOY WEBHOOK ENDPOINT
# ═══════════════════════════════════════════════════════════

@app.post("/webhooks/cratejoy/order")
async def cratejoy_order_webhook(request: Request):
    """
    Receives Cratejoy webhook (order_new, subscription_renewed, etc.)
    Cratejoy sends JSON payload via POST.
    No HMAC — Cratejoy doesn't sign webhooks. We validate by checking source.
    """
    start_time = time.time()
    body = await request.body()
    logger.info(f"[CRATEJOY WEBHOOK] Received webhook, body size: {len(body)} bytes")

    # ─── Parse payload ───
    try:
        raw_text = body.decode("utf-8")
        # Cratejoy sends escaped JSON strings sometimes (double-encoded)
        # e.g., body = '"{\"type\": \"subscription\", ...}"' → first parse → str → second parse → dict
        payload = json.loads(raw_text)
        if isinstance(payload, str):
            logger.info(f"[CRATEJOY WEBHOOK] Payload was double-encoded JSON string, parsing inner layer")
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            logger.error(f"[CRATEJOY WEBHOOK] Payload parsed but is not a dict: type={type(payload).__name__}")
            payload = {"raw": str(payload)}
        logger.info(f"[CRATEJOY WEBHOOK] Parsed payload keys: {list(payload.keys()) if isinstance(payload, dict) else 'N/A'}")
    except Exception as e:
        logger.error(f"[CRATEJOY WEBHOOK] Parse error: {e}", exc_info=True)
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    db = get_supabase()

    # Determine event type from payload structure
    # Cratejoy sends the object directly — e.g. subscription_renewed sends the subscription object
    # The "type" field is the object type: "subscription", "order", "customer"
    event_type = "unknown"
    payload_type = payload.get("type", "") if isinstance(payload, dict) else ""

    if payload_type == "subscription":
        event_type = "subscription_renewed"
    elif payload_type == "order":
        event_type = "order_new"
    elif payload_type == "customer":
        event_type = "customer_new"
    elif "subscription" in payload:
        event_type = "subscription_renewed"
    elif "order" in payload:
        event_type = "order_new"
    elif "customer" in payload:
        event_type = "customer_new"

    logger.info(f"[CRATEJOY WEBHOOK] Detected event_type={event_type} from payload_type='{payload_type}'")

    # ─── Check subscription status (cancelled, expired, etc.) ───
    sub_status_raw = payload.get("status", "") if isinstance(payload, dict) else ""
    # Cratejoy sends status as string or int: 2=Active, 3=Cancelled, 4=Suspended, 5=Expired
    sub_status_map = {1: "unpaid", 2: "active", 3: "cancelled", 4: "suspended", 5: "expired",
                      6: "past_due", 7: "pending_renewal", 8: "renewing"}
    if isinstance(sub_status_raw, int):
        sub_status_str = sub_status_map.get(sub_status_raw, str(sub_status_raw))
    else:
        sub_status_str = str(sub_status_raw).lower().strip()

    logger.info(f"[CRATEJOY WEBHOOK] Subscription status: '{sub_status_str}' (raw: '{sub_status_raw}')")

    # Determine if this is a cancelled subscription and whether prepaid boxes remain
    is_cancelled = sub_status_str == "cancelled"
    is_expired = sub_status_str == "expired"
    is_prepaid = False
    prepaid_end_date = None

    if is_cancelled or is_expired:
        # Check if prepaid subscription with remaining boxes
        term_data = payload.get("term", {}) if isinstance(payload, dict) else {}
        num_cycles = term_data.get("num_cycles", 1) if isinstance(term_data, dict) else 1
        end_date_str = payload.get("end_date", "") if isinstance(payload, dict) else ""

        if num_cycles and num_cycles > 1 and end_date_str:
            try:
                # Parse end_date (e.g. "2026-05-01T07:54:00Z")
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).date()
                if end_dt > date.today():
                    is_prepaid = True
                    prepaid_end_date = end_dt
                    logger.info(f"[CRATEJOY WEBHOOK] Cancelled BUT prepaid — end_date {end_dt} is in the future ({num_cycles} cycles). Will set cancelled-prepaid.")
                else:
                    logger.info(f"[CRATEJOY WEBHOOK] Cancelled AND expired — end_date {end_dt} is in the past. Will set cancelled-expired.")
            except Exception as e:
                logger.warning(f"[CRATEJOY WEBHOOK] Could not parse end_date '{end_date_str}': {e}")

        if is_cancelled and not is_prepaid:
            logger.info(f"[CRATEJOY WEBHOOK] Subscription is cancelled (no prepaid remaining). Will skip decision engine.")
        if is_expired:
            logger.info(f"[CRATEJOY WEBHOOK] Subscription is expired. Will skip decision engine.")

    # Use a combo key for idempotency 
    cj_order_id = ""
    if isinstance(payload, dict):
        cj_order_id = str(payload.get("id", payload.get("order", {}).get("id", "")))

    event_id = f"cj_{event_type}_{cj_order_id}" if cj_order_id else None
    logger.info(f"[CRATEJOY WEBHOOK] Event: {event_type}, CJ Order/Sub ID: {cj_order_id}")

    # Check duplicate
    if event_id:
        existing = db.table("webhook_logs").select("id").eq("source", "cratejoy").eq("event_id", event_id).execute()
        if existing.data:
            logger.info(f"[CRATEJOY WEBHOOK] Duplicate event {event_id} — skipping")
            return JSONResponse({"status": "duplicate"}, status_code=200)

    # ─── Log raw webhook ───
    webhook_log = db.table("webhook_logs").insert({
        "source": "cratejoy",
        "event_type": event_type,
        "event_id": event_id,
        "payload": payload if isinstance(payload, dict) else {"raw": str(payload)},
        "headers": {"content-type": request.headers.get("content-type", "")},
        "status": "received",
    }).execute()
    webhook_log_id = webhook_log.data[0]["id"] if webhook_log.data else None

    # ─── Extract customer info ───
    try:
        # Cratejoy payloads vary by event type — normalize
        # Case 1: Payload IS the subscription object (type=subscription) — subscription_renewed
        # Case 2: Payload IS the order object (type=order) — order_new
        # Case 3: Payload wraps: {"subscription": {...}, "order": {...}} — less common
        if payload_type == "subscription":
            subscription_data = payload
            order_data = payload.get("order", {})
            customer_data = payload.get("customer", {})
            logger.info(f"[CRATEJOY WEBHOOK] Payload IS a subscription object (id={payload.get('id')})")
        elif payload_type == "order":
            order_data = payload
            subscription_data = payload.get("subscription", {})
            customer_data = payload.get("customer", order_data.get("customer", {}))
            logger.info(f"[CRATEJOY WEBHOOK] Payload IS an order object (id={payload.get('id')})")
        elif payload_type == "customer":
            customer_data = payload
            order_data = {}
            subscription_data = {}
            logger.info(f"[CRATEJOY WEBHOOK] Payload IS a customer object (id={payload.get('id')})")
        else:
            order_data = payload.get("order", payload)
            subscription_data = payload.get("subscription", {})
            customer_data = payload.get("customer", order_data.get("customer", order_data) if isinstance(order_data, dict) else {})
            logger.info(f"[CRATEJOY WEBHOOK] Payload has generic structure, extracting from nested keys")

        # Ensure customer_data is a dict (safety)
        if not isinstance(customer_data, dict):
            logger.warning(f"[CRATEJOY WEBHOOK] customer_data is not a dict: {type(customer_data).__name__}")
            customer_data = {}

        email = (customer_data.get("email", "") or "").strip().lower()
        first_name = customer_data.get("first_name", customer_data.get("name", ""))
        last_name = customer_data.get("last_name", "")
        cratejoy_customer_id = str(customer_data.get("id", ""))

        # Cratejoy address
        address_data = customer_data.get("shipping_address", customer_data.get("address", {}))
        if isinstance(address_data, dict):
            address_line1 = address_data.get("street", address_data.get("address1", ""))
            city = address_data.get("city", "")
            province = address_data.get("state", address_data.get("province", ""))
            zip_code = address_data.get("zip_code", address_data.get("zip", ""))
            country = address_data.get("country", "US")
        else:
            address_line1 = city = province = zip_code = ""
            country = "US"

        # Try to extract due date from Cratejoy subscription note or custom fields
        due_date_str = None
        due_date = None
        note = subscription_data.get("note", order_data.get("note", ""))
        if note and isinstance(note, str):
            date_match = re.search(r'\d{4}-\d{2}-\d{2}', note)
            if date_match:
                due_date_str = date_match.group()
        # Also check custom_fields if present
        custom_fields = customer_data.get("custom_fields", subscription_data.get("custom_fields", {}))
        if isinstance(custom_fields, dict):
            for key, val in custom_fields.items():
                if "due" in key.lower() and val:
                    due_date_str = str(val)
                    break

        if due_date_str:
            due_date = parse_due_date(due_date_str)
            logger.info(f"[CRATEJOY WEBHOOK] Parsed due_date: {due_date} from '{due_date_str}'")

        trimester = None
        if due_date:
            trimester = calculate_trimester(due_date, date.today())
            logger.info(f"[CRATEJOY WEBHOOK] Calculated trimester: T{trimester}")

        logger.info(f"[CRATEJOY WEBHOOK] Customer: {email}, Name: {first_name} {last_name}")

        if email:
            existing_customer = db.table("customers").select("*").ilike("email", email).execute()

            customer_record = {
                "email": email,
                "first_name": first_name or None,
                "last_name": last_name or None,
                "cratejoy_customer_id": cratejoy_customer_id or None,
                "address_line1": address_line1 or None,
                "city": city or None,
                "province": province or None,
                "zip": zip_code or None,
                "country": country or "US",
            }

            if due_date:
                customer_record["due_date"] = due_date.isoformat()
            if trimester:
                customer_record["trimester"] = trimester

            # ─── Set subscription status based on Cratejoy status ───
            if is_cancelled and is_prepaid:
                customer_record["subscription_status"] = "cancelled-prepaid"
                logger.info(f"[CRATEJOY WEBHOOK] Setting status: cancelled-prepaid (prepaid until {prepaid_end_date})")
            elif is_cancelled or is_expired:
                customer_record["subscription_status"] = "cancelled-expired"
                logger.info(f"[CRATEJOY WEBHOOK] Setting status: cancelled-expired")
            elif sub_status_str in ("suspended", "paused"):
                customer_record["subscription_status"] = "paused"
                logger.info(f"[CRATEJOY WEBHOOK] Setting status: paused")

            if existing_customer.data:
                cust_id = existing_customer.data[0]["id"]
                existing_cust = existing_customer.data[0]
                if existing_cust.get("shopify_customer_id"):
                    customer_record["platform"] = "both"
                else:
                    customer_record["platform"] = "cratejoy"

                # IMPORTANT: Don't overwrite existing data with empty values from Cratejoy
                # If existing customer already has due_date/trimester/clothing_size from Shopify, preserve it
                if not due_date and existing_cust.get("due_date"):
                    logger.info(f"[CRATEJOY WEBHOOK] Preserving existing due_date: {existing_cust['due_date']} (Cratejoy has no quiz data)")
                    # Don't add due_date to update — keep existing
                if not trimester and existing_cust.get("trimester"):
                    logger.info(f"[CRATEJOY WEBHOOK] Preserving existing trimester: T{existing_cust['trimester']} (from Shopify/DB)")
                # Don't overwrite subscription_status if existing is active and CJ is sending a renewal
                if not is_cancelled and not is_expired and sub_status_str not in ("suspended", "paused"):
                    if existing_cust.get("subscription_status") == "active":
                        customer_record.pop("subscription_status", None)  # Keep existing active status

                db.table("customers").update(customer_record).eq("id", cust_id).execute()
                logger.info(f"[CRATEJOY WEBHOOK] Updated existing customer: {cust_id}")
            else:
                customer_record["platform"] = "cratejoy"
                if "subscription_status" not in customer_record:
                    customer_record["subscription_status"] = "active"
                result = db.table("customers").insert(customer_record).execute()
                cust_id = result.data[0]["id"] if result.data else None
                logger.info(f"[CRATEJOY WEBHOOK] Created new customer: {cust_id}")

            # ─── Re-read customer from DB to get merged data (Shopify + Cratejoy) ───
            if cust_id:
                merged_cust = db.table("customers").select("*").eq("id", cust_id).single().execute()
                merged_data = merged_cust.data if merged_cust.data else {}
                db_trimester = merged_data.get("trimester")
                db_due_date = merged_data.get("due_date")
                db_clothing_size = merged_data.get("clothing_size", "")
                db_status = merged_data.get("subscription_status", "active")

                logger.info(f"[CRATEJOY WEBHOOK] Merged customer data — trimester: {f'T{db_trimester}' if db_trimester else 'MISSING'}, "
                            f"due_date: {db_due_date or 'MISSING'}, size: {db_clothing_size or 'MISSING'}, status: {db_status}")

            # ─── Fetch Cratejoy Survey Results (quiz data: due date, size, gender, daddy) ───
            if cust_id and CRATEJOY_CLIENT_ID and CRATEJOY_CLIENT_SECRET:
                # IMPORTANT: payload.id points to different objects depending on event type:
                #   customer_new       → payload.id = customer_id   (NOT subscription_id)
                #   order_new          → payload.id = order_id       (NOT subscription_id)
                #   subscription_*     → payload.id = subscription_id (correct)
                # The survey results API requires a real subscription_id, so we resolve it carefully.
                if event_type == "customer_new" and payload_type == "customer":
                    # Look up subscription by customer.id
                    cj_sub_id_for_survey = None
                    cj_customer_id_for_lookup = str(payload.get("id", ""))
                    logger.info(
                        f"[CRATEJOY WEBHOOK] customer_new — will resolve subscription_id "
                        f"for CJ customer {cj_customer_id_for_lookup}"
                    )
                elif payload_type == "order":
                    # For order_new, subscription_id lives in the nested subscription object, NOT payload.id
                    nested_sub = payload.get("subscription") or {}
                    cj_sub_id_for_survey = str(nested_sub.get("id", "") or "")
                    cj_customer_id_for_lookup = None
                    if cj_sub_id_for_survey:
                        logger.info(f"[CRATEJOY WEBHOOK] order_new — using nested subscription_id={cj_sub_id_for_survey} for survey (order_id={payload.get('id')})")
                    else:
                        # Fallback: look up by customer if subscription relation is missing
                        nested_cust = payload.get("customer") or {}
                        cj_customer_id_for_lookup = str(nested_cust.get("id", "") or "")
                        logger.warning(
                            f"[CRATEJOY WEBHOOK] order_new has no nested subscription.id — "
                            f"falling back to customer lookup for customer {cj_customer_id_for_lookup}"
                        )
                else:
                    # subscription_renewed / subscription_cancelled — payload.id IS the subscription_id
                    cj_sub_id_for_survey = str(payload.get("id", "") or "")
                    cj_customer_id_for_lookup = None
                    logger.info(f"[CRATEJOY WEBHOOK] {event_type} — using payload id={cj_sub_id_for_survey} as subscription_id for survey")

                # Only proceed if we have a customer or subscription id to work with
                if cj_sub_id_for_survey or cj_customer_id_for_lookup:
                    logger.info(f"[CRATEJOY WEBHOOK] Starting survey enrichment...")
                    try:
                        auth_str = base64.b64encode(f"{CRATEJOY_CLIENT_ID}:{CRATEJOY_CLIENT_SECRET}".encode()).decode()
                        cj_headers = {"Authorization": f"Basic {auth_str}", "Accept": "application/json"}

                        survey_due_date = None
                        survey_size = None
                        survey_gender = None
                        survey_daddy = None
                        survey_past_subscriber = None

                        async with httpx.AsyncClient(timeout=15.0) as client:
                            # Step 1 (customer_new only): resolve subscription_id from customer_id
                            if cj_customer_id_for_lookup and not cj_sub_id_for_survey:
                                sub_lookup_url = f"https://api.cratejoy.com/v1/subscriptions/?customer.id={cj_customer_id_for_lookup}&limit=1"
                                logger.info(f"[CRATEJOY WEBHOOK] Subscription lookup for customer {cj_customer_id_for_lookup}: {sub_lookup_url}")
                                sub_lookup_resp = await client.get(sub_lookup_url, headers=cj_headers)
                                if sub_lookup_resp.status_code == 200:
                                    sub_results = sub_lookup_resp.json().get("results", [])
                                    if sub_results:
                                        cj_sub_id_for_survey = str(sub_results[0].get("id", ""))
                                        logger.info(f"[CRATEJOY WEBHOOK] Resolved subscription_id={cj_sub_id_for_survey} for customer {cj_customer_id_for_lookup}")
                                    else:
                                        logger.info(f"[CRATEJOY WEBHOOK] No subscriptions found for customer {cj_customer_id_for_lookup} — survey enrichment skipped")
                                else:
                                    logger.warning(
                                        f"[CRATEJOY WEBHOOK] Subscription lookup returned {sub_lookup_resp.status_code}: "
                                        f"{sub_lookup_resp.text[:200]}"
                                    )

                            # Step 2: fetch survey results using resolved subscription_id
                            if cj_sub_id_for_survey:
                                survey_url = f"https://api.cratejoy.com/v1/product_survey_results/?subscription_id={cj_sub_id_for_survey}"
                                logger.info(f"[CRATEJOY WEBHOOK] Calling survey API: {survey_url}")
                                survey_resp = await client.get(survey_url, headers=cj_headers)

                                if survey_resp.status_code == 200:
                                    survey_data = survey_resp.json()
                                    logger.info(f"[CRATEJOY WEBHOOK] Survey API response (count={survey_data.get('count', '?')}): {json.dumps(survey_data)[:1000]}")

                                    survey_results = survey_data.get("results", [])
                                    for sr in survey_results:
                                        answers = sr.get("answers", [])
                                        for ans in answers:
                                            field_name = ""
                                            field = ans.get("field", {})
                                            if isinstance(field, dict):
                                                field_name = field.get("name", "").lower().strip()
                                            value = str(ans.get("value", "")).strip()

                                            if not value:
                                                continue

                                            logger.info(f"[CRATEJOY WEBHOOK] Survey answer: '{field_name}' = '{value}'")

                                            # Match OBB's 5 survey questions by keyword matching
                                            if "due date" in field_name:
                                                survey_due_date = value
                                                logger.info(f"[CRATEJOY WEBHOOK] → Due date: {value}")
                                            elif "clothing size" in field_name:
                                                survey_size = value
                                                logger.info(f"[CRATEJOY WEBHOOK] → Clothing size: {value}")
                                            elif "expecting" in field_name or "whom" in field_name:
                                                survey_gender = value
                                                logger.info(f"[CRATEJOY WEBHOOK] → Expecting: {value}")
                                            elif "second parent" in field_name or "matching apparel" in field_name:
                                                survey_daddy = value
                                                logger.info(f"[CRATEJOY WEBHOOK] → Daddy item: {value}")
                                            elif "received" in field_name and "oh baby box" in field_name:
                                                survey_past_subscriber = value
                                                logger.info(f"[CRATEJOY WEBHOOK] → Past subscriber: {value}")
                                else:
                                    logger.warning(f"[CRATEJOY WEBHOOK] Survey API returned {survey_resp.status_code}: {survey_resp.text[:300]}")

                        # Apply survey data to customer
                        update_from_survey = {}

                        if survey_due_date:
                            parsed_due = parse_due_date(survey_due_date)
                            if parsed_due:
                                update_from_survey["due_date"] = parsed_due.isoformat()
                                update_from_survey["trimester"] = calculate_trimester(parsed_due, date.today())
                                db_due_date = parsed_due.isoformat()
                                db_trimester = update_from_survey["trimester"]
                                logger.info(f"[CRATEJOY WEBHOOK] Survey → due_date={db_due_date}, T{db_trimester}")

                        if survey_size:
                            norm_size = normalize_clothing_size(survey_size)
                            if norm_size:
                                update_from_survey["clothing_size"] = norm_size
                                db_clothing_size = norm_size
                                logger.info(f"[CRATEJOY WEBHOOK] Survey → clothing_size={norm_size}")

                        if survey_gender:
                            # Normalize: "Baby Boy" → "boy", "Baby Girl" → "girl", etc.
                            gender_lower = survey_gender.lower().strip()
                            if "boy" in gender_lower:
                                update_from_survey["baby_gender"] = "boy"
                            elif "girl" in gender_lower:
                                update_from_survey["baby_gender"] = "girl"
                            else:
                                update_from_survey["baby_gender"] = "unknown"
                            logger.info(f"[CRATEJOY WEBHOOK] Survey → baby_gender={update_from_survey['baby_gender']}")

                        if survey_daddy:
                            daddy_lower = survey_daddy.lower().strip()
                            update_from_survey["wants_daddy_item"] = daddy_lower in ("yes", "y", "true", "sure", "ok", "okay")
                            logger.info(f"[CRATEJOY WEBHOOK] Survey → wants_daddy_item={update_from_survey['wants_daddy_item']}")

                        if update_from_survey:
                            db.table("customers").update(update_from_survey).eq("id", cust_id).execute()
                            logger.info(f"[CRATEJOY WEBHOOK] Updated customer {cust_id} with survey data: {list(update_from_survey.keys())}")
                        else:
                            logger.info(f"[CRATEJOY WEBHOOK] No survey data found to update customer")

                    except Exception as e:
                        logger.warning(f"[CRATEJOY WEBHOOK] Survey enrichment failed (non-fatal): {e}", exc_info=True)

            # ─── Decide: run decision engine or flag as needs-data-entry ───
            if cust_id:
                # Skip decision engine for truly cancelled/expired subscriptions (no prepaid remaining)
                if (is_cancelled and not is_prepaid) or is_expired:
                    logger.info(f"[CRATEJOY WEBHOOK] Skipping decision engine — subscription is {sub_status_str} (no prepaid remaining)")
                    cancel_decision = {
                        "customer_id": cust_id,
                        "kit_id": None,
                        "kit_sku": None,
                        "decision_type": "skipped",
                        "reason": f"Subscription {sub_status_str} on Cratejoy (no prepaid boxes remaining). No kit assignment needed.",
                        "status": "rejected",
                        "order_id": cj_order_id,
                        "platform": "cratejoy",
                        "trimester": db_trimester if db_due_date else None,
                        "ship_date": date.today().isoformat(),
                    }
                    db.table("decisions").insert(cancel_decision).execute()
                    logger.info(f"[CRATEJOY WEBHOOK] Saved skipped decision for cancelled/expired subscription")

                    write_decision_to_sheet({
                        "date": date.today().isoformat(),
                        "customer_name": f"{first_name or ''} {last_name or ''}".strip(),
                        "email": email,
                        "platform": "cratejoy",
                        "trimester": db_trimester if db_due_date else None,
                        "order_type": event_type,
                        "kit_sku": "—",
                        "decision_type": "skipped",
                        "reason": f"Subscription {sub_status_str} — no prepaid remaining",
                        "order_id": str(cj_order_id),
                        "due_date": db_due_date or "",
                        "clothing_size": db_clothing_size or "",
                    })

                # If customer STILL has no trimester → can't assign a kit, flag for manual data entry
                elif not db_trimester:
                    logger.warning(f"[CRATEJOY WEBHOOK] Customer {email} has NO trimester after all enrichment. Flagging as needs-data-entry.")
                    needs_data_decision = {
                        "customer_id": cust_id,
                        "kit_id": None,
                        "kit_sku": None,
                        "decision_type": "needs-data-entry",
                        "reason": "Cratejoy customer missing quiz data (due date, size). Cratejoy does not capture this data — please edit the customer to add due date and clothing size, then click Re-curate.",
                        "status": "pending",
                        "order_id": cj_order_id,
                        "platform": "cratejoy",
                        "trimester": None,
                        "ship_date": date.today().isoformat(),
                    }
                    db.table("decisions").insert(needs_data_decision).execute()
                    logger.info(f"[CRATEJOY WEBHOOK] Saved needs-data-entry decision — waiting for manual data entry")

                    write_decision_to_sheet({
                        "date": date.today().isoformat(),
                        "customer_name": f"{first_name or ''} {last_name or ''}".strip(),
                        "email": email,
                        "platform": "cratejoy",
                        "trimester": None,
                        "order_type": event_type,
                        "kit_sku": "—",
                        "decision_type": "needs-data-entry",
                        "reason": "Missing quiz data — add due date & size manually, then re-curate",
                        "order_id": str(cj_order_id),
                        "due_date": "",
                        "clothing_size": "",
                    })

                # Customer has trimester → run decision engine normally
                else:
                    kit_decision = await assign_kit(cust_id, date.today())
                    logger.info(f"[CRATEJOY WEBHOOK] Decision engine result: {kit_decision['decision_type']} — {kit_decision.get('kit_sku', 'none')}")

                    decision = {
                        "customer_id": cust_id,
                        "kit_id": kit_decision.get("kit_id"),
                        "kit_sku": kit_decision.get("kit_sku"),
                        "decision_type": kit_decision["decision_type"],
                        "reason": kit_decision["reason"],
                        "status": "pending",
                        "order_id": cj_order_id,
                        "platform": "cratejoy",
                        "trimester": db_trimester,
                        "ship_date": date.today().isoformat(),
                    }
                    db.table("decisions").insert(decision).execute()
                    logger.info(f"[CRATEJOY WEBHOOK] Saved decision: {kit_decision['decision_type']}, kit: {kit_decision.get('kit_sku', 'none')}")

                    # ─── Write to Google Sheets ───
                    write_decision_to_sheet({
                        "date": date.today().isoformat(),
                        "customer_name": f"{first_name or ''} {last_name or ''}".strip(),
                        "email": email,
                        "platform": "cratejoy",
                        "trimester": db_trimester,
                        "order_type": event_type,
                        "kit_sku": kit_decision.get("kit_sku", "—"),
                        "decision_type": kit_decision["decision_type"],
                        "reason": kit_decision["reason"],
                        "order_id": str(cj_order_id),
                        "due_date": db_due_date or "",
                        "clothing_size": db_clothing_size or "",
                    })

        elapsed_ms = int((time.time() - start_time) * 1000)
        if webhook_log_id:
            db.table("webhook_logs").update({
                "status": "processed",
                "processing_time_ms": elapsed_ms,
            }).eq("id", webhook_log_id).execute()

        await log_activity(
            "webhook",
            f"Cratejoy {event_type} processed",
            f"Customer: {email}, CJ ID: {cj_order_id}",
            "success"
        )
        return JSONResponse({"status": "ok", "processing_time_ms": elapsed_ms})

    except Exception as e:
        logger.error(f"[CRATEJOY WEBHOOK] Error processing: {e}", exc_info=True)
        if webhook_log_id:
            db.table("webhook_logs").update({
                "status": "failed",
                "error_message": str(e),
            }).eq("id", webhook_log_id).execute()
        await log_activity("webhook", f"Cratejoy webhook failed: {e}", "", "error")
        return JSONResponse({"status": "ok"}, status_code=200)


# ═══════════════════════════════════════════════════════════
# CRATEJOY WEBHOOK REGISTRATION (run once)
# ═══════════════════════════════════════════════════════════

@app.post("/api/cratejoy/register-webhooks")
async def register_cratejoy_webhooks():
    """Register webhooks with Cratejoy API. Idempotent — skips already-registered hooks."""
    if not CRATEJOY_CLIENT_ID or not CRATEJOY_CLIENT_SECRET:
        return JSONResponse({"error": "Cratejoy credentials not set"}, status_code=400)

    auth_str = base64.b64encode(f"{CRATEJOY_CLIENT_ID}:{CRATEJOY_CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_str}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    target_url = f"{BASE_URL}/webhooks/cratejoy/order"
    events_to_register = [
        ("order_new", "OBB Order New"),
        ("subscription_renewed", "OBB Sub Renewed"),
        ("customer_new", "OBB Customer New"),
        ("subscription_cancelled", "OBB Sub Cancelled"),
    ]

    results = []
    async with httpx.AsyncClient() as client:
        # First list existing hooks to avoid duplicates
        existing_events = set()
        logger.info("[CRATEJOY] Listing existing webhooks...")
        try:
            list_resp = await client.get("https://api.cratejoy.com/v1/hooks/", headers=headers)
            logger.info(f"[CRATEJOY] List hooks response: {list_resp.status_code}")
            if list_resp.status_code == 200:
                existing_hooks = list_resp.json().get("results", [])
                logger.info(f"[CRATEJOY] Found {len(existing_hooks)} existing webhooks")
                for hook in existing_hooks:
                    hook_event = hook.get("event", "")
                    hook_target = hook.get("target", "")
                    logger.info(f"[CRATEJOY]   - {hook_event}: {hook_target} (enabled={hook.get('enabled')})")
                    # Consider it registered if same event + same target (or any target with our URL)
                    if hook_target == target_url:
                        existing_events.add(hook_event)
        except Exception as e:
            logger.error(f"[CRATEJOY] Error listing hooks: {e}")

        for event, name in events_to_register:
            # Skip if already registered
            if event in existing_events:
                logger.info(f"[CRATEJOY] Hook '{event}' already registered — skipping")
                results.append({
                    "event": event,
                    "status": 200,
                    "response": "Already registered",
                    "skipped": True,
                })
                continue

            hook_data = {
                "name": name,
                "target": target_url,
                "request_type": "POST",
                "event": event,
            }
            try:
                resp = await client.post(
                    "https://api.cratejoy.com/v1/hooks/",
                    headers=headers,
                    json=hook_data,
                )
                logger.info(f"[CRATEJOY] Register '{event}': {resp.status_code} — {resp.text}")
                results.append({
                    "event": event,
                    "status": resp.status_code,
                    "response": resp.json() if resp.status_code in (200, 201) else resp.text,
                    "skipped": False,
                })
            except Exception as e:
                logger.error(f"[CRATEJOY] Error registering '{event}': {e}")
                results.append({"event": event, "status": "error", "response": str(e), "skipped": False})

    registered = sum(1 for r in results if r.get("status") in (200, 201))
    skipped = sum(1 for r in results if r.get("skipped"))
    summary = f"Registered: {registered}, Already existed: {skipped}"
    logger.info(f"[CRATEJOY] Registration complete. {summary}")
    await log_activity("cratejoy", f"Webhook registration: {summary}", json.dumps(results), "success")
    return JSONResponse({"results": results, "summary": summary})


# ═══════════════════════════════════════════════════════════
# DASHBOARD UI ROUTES
# ═══════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard with stats."""
    try:
        db = get_supabase()

        # Count customers
        customers = db.table("customers").select("id, platform, subscription_status", count="exact").execute()
        total_customers = customers.count or 0
        shopify_count = len([c for c in (customers.data or []) if c.get("platform") in ("shopify", "both")])
        cratejoy_count = len([c for c in (customers.data or []) if c.get("platform") in ("cratejoy", "both")])

        # Count webhooks
        webhooks = db.table("webhook_logs").select("id, source, status", count="exact").execute()
        total_webhooks = webhooks.count or 0
        shopify_webhooks = len([w for w in (webhooks.data or []) if w.get("source") == "shopify"])
        cratejoy_webhooks = len([w for w in (webhooks.data or []) if w.get("source") == "cratejoy"])

        # Count decisions
        decisions = db.table("decisions").select("id, status, decision_type", count="exact").execute()
        total_decisions = decisions.count or 0
        pending_decisions = len([d for d in (decisions.data or []) if d.get("status") == "pending"])

        # Count kits
        kits = db.table("kits").select("id, quantity_available", count="exact").execute()
        total_kits = kits.count or 0
        total_kit_units = sum(k.get("quantity_available", 0) for k in (kits.data or []))

        # Recent activity
        activity = db.table("activity_log").select("*").order("created_at", desc=True).limit(10).execute()

        # Phase 3 — VeraCore mini stats (safe if migration not applied yet)
        vc_pending_push = 0
        vc_failed       = 0
        vc_last_sync    = None
        try:
            vc_rows = db.table("decisions").select("status, veracore_status").execute().data or []
            for r in vc_rows:
                if r.get("status") == "approved" and not r.get("veracore_status"):
                    vc_pending_push += 1
                if r.get("veracore_status") == "failed":
                    vc_failed += 1
            last = (db.table("veracore_sync_log")
                      .select("run_at")
                      .eq("status", "ok")
                      .eq("sync_type", "inventory")
                      .order("run_at", desc=True)
                      .limit(1)
                      .execute().data or [])
            vc_last_sync = last[0]["run_at"] if last else None
        except Exception as vc_err:
            logger.debug("[DASHBOARD] VC widget skipped (migration 012?): %s", vc_err)

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "total_customers": total_customers,
            "shopify_count": shopify_count,
            "cratejoy_count": cratejoy_count,
            "total_webhooks": total_webhooks,
            "shopify_webhooks": shopify_webhooks,
            "cratejoy_webhooks": cratejoy_webhooks,
            "total_decisions": total_decisions,
            "pending_decisions": pending_decisions,
            "total_kits": total_kits,
            "total_kit_units": total_kit_units,
            "recent_activity": activity.data or [],
            "vc_enabled":      veracore_enabled(),
            "vc_pending_push": vc_pending_push,
            "vc_failed":       vc_failed,
            "vc_last_sync":    vc_last_sync,
            "page": "dashboard",
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error: {e}", exc_info=True)
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "total_customers": 0, "shopify_count": 0, "cratejoy_count": 0,
            "total_webhooks": 0, "shopify_webhooks": 0, "cratejoy_webhooks": 0,
            "total_decisions": 0, "pending_decisions": 0,
            "total_kits": 0, "total_kit_units": 0,
            "recent_activity": [],
            "vc_enabled": False, "vc_pending_push": 0, "vc_failed": 0, "vc_last_sync": None,
            "error": str(e),
            "page": "dashboard",
        })


@app.get("/webhooks", response_class=HTMLResponse)
async def webhooks_page(request: Request):
    """View all received webhooks."""
    try:
        db = get_supabase()
        logs = db.table("webhook_logs").select("*").order("created_at", desc=True).limit(50).execute()
        return templates.TemplateResponse("webhooks.html", {
            "request": request,
            "webhooks": logs.data or [],
            "page": "webhooks",
        })
    except Exception as e:
        logger.error(f"[WEBHOOKS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("webhooks.html", {
            "request": request,
            "webhooks": [],
            "error": str(e),
            "page": "webhooks",
        })


@app.get("/webhooks/{webhook_id}", response_class=HTMLResponse)
async def webhook_detail(request: Request, webhook_id: str):
    """View a single webhook's full payload."""
    try:
        db = get_supabase()
        log = db.table("webhook_logs").select("*").eq("id", webhook_id).single().execute()
        return templates.TemplateResponse("webhook_detail.html", {
            "request": request,
            "webhook": log.data,
            "page": "webhooks",
        })
    except Exception as e:
        logger.error(f"[WEBHOOK DETAIL] Error: {e}", exc_info=True)
        return HTMLResponse(f"Webhook not found: {e}", status_code=404)


@app.post("/webhooks/{webhook_id}/replay")
async def replay_webhook(webhook_id: str):
    """
    Replay a failed/received webhook by re-processing its stored payload.
    Creates a NEW webhook_log entry for the replay attempt.
    Skips HMAC verification and idempotency checks.
    """
    start_time = time.time()
    db = get_supabase()
    replay_log_id = None

    try:
        # ─── Load original webhook ───
        original = db.table("webhook_logs").select("*").eq("id", webhook_id).single().execute()
        if not original.data:
            logger.error(f"[WEBHOOK REPLAY] Webhook {webhook_id} not found")
            return RedirectResponse(f"/webhooks/{webhook_id}", status_code=303)

        wh = original.data
        source = wh.get("source", "")
        payload = wh.get("payload", {})
        original_event_id = wh.get("event_id", "")
        original_status = wh.get("status", "")

        logger.info(f"[WEBHOOK REPLAY] Starting replay for webhook {webhook_id}, source={source}, event_type={wh.get('event_type')}, original_status={original_status}")

        # ─── Allow replay of any status (creates a new log entry — completely safe) ───
        # Allowing 'processed' so survey enrichment fixes can be applied to already-processed webhooks
        allowed_statuses = ("failed", "received", "processed", "replayed", "duplicate")
        if original_status not in allowed_statuses:
            logger.warning(f"[WEBHOOK REPLAY] Cannot replay webhook with status '{original_status}'")
            return RedirectResponse(f"/webhooks/{webhook_id}", status_code=303)
        logger.info(f"[WEBHOOK REPLAY] Replaying webhook with original_status='{original_status}'")

        # ─── Create new webhook log for replay ───
        replay_event_id = f"replay_{original_event_id or webhook_id}_{int(time.time())}"
        replay_log = db.table("webhook_logs").insert({
            "source": source,
            "event_type": f"replay_{wh.get('event_type', 'unknown')}",
            "event_id": replay_event_id,
            "payload": payload,
            "headers": {"replayed_from": webhook_id, "original_event_id": original_event_id},
            "status": "received",
        }).execute()
        replay_log_id = replay_log.data[0]["id"] if replay_log.data else None
        logger.info(f"[WEBHOOK REPLAY] Created replay log: {replay_log_id}")

        # ═══════════════════════════════════════════════════════
        # SHOPIFY REPLAY
        # ═══════════════════════════════════════════════════════
        if source == "shopify":
            logger.info(f"[WEBHOOK REPLAY] Processing Shopify payload")

            shopify_order_id = str(payload.get("id", ""))
            customer_data = payload.get("customer", {})
            email = (str(customer_data.get("email") or payload.get("email") or "")).strip().lower()
            first_name = customer_data.get("first_name", "")
            last_name = customer_data.get("last_name", "")
            shopify_customer_id = str(customer_data.get("id", ""))
            phone = customer_data.get("phone", "")

            # Must use `or {}` — shipping_address can be explicitly null in Shopify JSON
            shipping = payload.get("shipping_address") or {}
            address_line1 = shipping.get("address1", "")
            city = shipping.get("city", "")
            province = shipping.get("province", "")
            zip_code = shipping.get("zip", "")
            country = shipping.get("country_code", "US")
            logger.info(f"[WEBHOOK REPLAY] shipping_address present: {bool(shipping)}")

            line_items = payload.get("line_items", [])
            note_attributes = payload.get("note_attributes", [])
            quiz = extract_quiz_data(note_attributes, line_items)

            due_date_str = quiz["due_date_str"]
            clothing_size = quiz["clothing_size"]
            baby_gender = quiz["baby_gender"]
            wants_daddy = quiz["wants_daddy"]

            due_date = parse_due_date(due_date_str)
            trimester = None
            if due_date:
                trimester = calculate_trimester(due_date, date.today())
                logger.info(f"[WEBHOOK REPLAY] Shopify: due_date={due_date}, T{trimester}, size={clothing_size}")

            total_price = float(payload.get("total_price", "0") or "0")
            source_name = payload.get("source_name", "")
            is_renewal = (total_price == 0) or (not source_name) or (source_name not in ("web", "shopify_draft_order"))
            if quiz["subscription_plan"]:
                is_renewal = False

            # Non-subscription order guard (same logic as live webhook)
            is_recharge_renewal = (total_price == 0.0)
            has_sub_sku = quiz["subscription_plan"] is not None
            has_rc_attrs = bool(quiz["rc_charge_id"] or quiz["rc_subscription_ids"])
            is_subscription_order = is_recharge_renewal or has_sub_sku or has_rc_attrs
            logger.info(
                f"[WEBHOOK REPLAY] Shopify customer: {email}, order: {shopify_order_id}, "
                f"renewal={is_renewal}, is_subscription_order={is_subscription_order}"
            )
            if not is_subscription_order:
                logger.info(f"[WEBHOOK REPLAY] NON-SUBSCRIPTION order — SKUs: {[i.get('sku', '') for i in line_items]}, price=${total_price}. Skipping decision engine.")

            if email:
                existing_customer = db.table("customers").select("*").ilike("email", email).execute()

                # Non-subscription order from unknown email → skip entirely (no ghost customers)
                if not is_subscription_order and not existing_customer.data:
                    logger.info(f"[WEBHOOK REPLAY] Non-sub order from unknown email {email} — skipping entirely")
                    db.table("webhook_logs").update({"status": "replayed", "replayed_at": date.today().isoformat()}).eq("id", webhook_id).execute()
                    return RedirectResponse(f"/webhooks/{webhook_id}", status_code=303)

                customer_record = {
                    "email": email,
                    "first_name": first_name or None,
                    "last_name": last_name or None,
                    "shopify_customer_id": shopify_customer_id or None,
                    "phone": phone or None,
                    "address_line1": address_line1 or None,
                    "city": city or None,
                    "province": province or None,
                    "zip": zip_code or None,
                    "country": country or "US",
                }
                if due_date:
                    customer_record["due_date"] = due_date.isoformat()
                if trimester:
                    customer_record["trimester"] = trimester
                if clothing_size:
                    customer_record["clothing_size"] = clothing_size
                if baby_gender:
                    customer_record["baby_gender"] = baby_gender
                customer_record["wants_daddy_item"] = wants_daddy

                if existing_customer.data:
                    cust_id = existing_customer.data[0]["id"]
                    if existing_customer.data[0].get("cratejoy_customer_id"):
                        customer_record["platform"] = "both"
                    else:
                        customer_record["platform"] = "shopify"
                    # Non-sub order for existing customer → address only
                    if not is_subscription_order:
                        address_only = {k: customer_record[k] for k in ("email", "phone", "address_line1", "city", "province", "zip", "country", "platform") if k in customer_record}
                        db.table("customers").update(address_only).eq("id", cust_id).execute()
                        logger.info(f"[WEBHOOK REPLAY] Non-sub order — updated address only for existing customer: {cust_id}")
                    else:
                        db.table("customers").update(customer_record).eq("id", cust_id).execute()
                        logger.info(f"[WEBHOOK REPLAY] Updated existing customer: {cust_id}")
                else:
                    customer_record["platform"] = "shopify"
                    customer_record["subscription_status"] = "active"
                    result = db.table("customers").insert(customer_record).execute()
                    cust_id = result.data[0]["id"] if result.data else None
                    logger.info(f"[WEBHOOK REPLAY] Created new customer: {cust_id}")

                if cust_id and is_subscription_order:
                    kit_decision = await assign_kit(cust_id, date.today())
                    logger.info(f"[WEBHOOK REPLAY] Decision: {kit_decision['decision_type']} — {kit_decision.get('kit_sku', 'none')}")

                    decision = {
                        "customer_id": cust_id,
                        "kit_id": kit_decision.get("kit_id"),
                        "kit_sku": kit_decision.get("kit_sku"),
                        "decision_type": kit_decision["decision_type"],
                        "reason": kit_decision["reason"],
                        "status": "pending",
                        "order_id": shopify_order_id,
                        "platform": "shopify",
                        "trimester": trimester,
                        "ship_date": date.today().isoformat(),
                    }
                    db.table("decisions").insert(decision).execute()
                    logger.info(f"[WEBHOOK REPLAY] Saved Shopify decision")

                    write_decision_to_sheet({
                        "date": date.today().isoformat(),
                        "customer_name": f"{first_name or ''} {last_name or ''}".strip(),
                        "email": email,
                        "platform": "shopify",
                        "trimester": trimester,
                        "order_type": "renewal" if is_renewal else "new",
                        "kit_sku": kit_decision.get("kit_sku", "—"),
                        "decision_type": kit_decision["decision_type"],
                        "reason": kit_decision["reason"],
                        "order_id": str(shopify_order_id),
                        "due_date": due_date.isoformat() if due_date else "",
                        "clothing_size": clothing_size or "",
                    })

            # Mark replay as successful
            elapsed_ms = int((time.time() - start_time) * 1000)
            if replay_log_id:
                db.table("webhook_logs").update({"status": "processed", "processing_time_ms": elapsed_ms}).eq("id", replay_log_id).execute()
            # Mark original as replayed (isolated try/except — don't crash if CHECK constraint hasn't been updated yet)
            try:
                db.table("webhook_logs").update({"status": "replayed", "error_message": f"Replayed successfully → {replay_log_id}"}).eq("id", webhook_id).execute()
            except Exception as e:
                logger.warning(f"[WEBHOOK REPLAY] Could not mark original as 'replayed' (CHECK constraint?): {e}")
                # Fallback: mark as processed with a note
                try:
                    db.table("webhook_logs").update({"error_message": f"Replayed → {replay_log_id}"}).eq("id", webhook_id).execute()
                except Exception:
                    pass
            await log_activity("webhook", f"Replayed Shopify webhook (order #{shopify_order_id})", f"Original: {webhook_id}, Replay: {replay_log_id}", "success")
            logger.info(f"[WEBHOOK REPLAY] Shopify replay complete in {elapsed_ms}ms")
            return RedirectResponse(f"/webhooks/{replay_log_id}", status_code=303)

        # ═══════════════════════════════════════════════════════
        # CRATEJOY REPLAY
        # ═══════════════════════════════════════════════════════
        elif source == "cratejoy":
            logger.info(f"[WEBHOOK REPLAY] Processing Cratejoy payload")

            payload_type = payload.get("type", "") if isinstance(payload, dict) else ""
            event_type = wh.get("event_type", "unknown")

            # Subscription status
            sub_status_raw = payload.get("status", "") if isinstance(payload, dict) else ""
            sub_status_map = {1: "unpaid", 2: "active", 3: "cancelled", 4: "suspended", 5: "expired",
                              6: "past_due", 7: "pending_renewal", 8: "renewing"}
            if isinstance(sub_status_raw, int):
                sub_status_str = sub_status_map.get(sub_status_raw, str(sub_status_raw))
            else:
                sub_status_str = str(sub_status_raw).lower().strip()

            is_cancelled = sub_status_str == "cancelled"
            is_expired = sub_status_str == "expired"
            is_prepaid = False

            if is_cancelled or is_expired:
                term_data = payload.get("term", {}) if isinstance(payload, dict) else {}
                num_cycles = term_data.get("num_cycles", 1) if isinstance(term_data, dict) else 1
                end_date_str = payload.get("end_date", "") if isinstance(payload, dict) else ""
                if num_cycles and num_cycles > 1 and end_date_str:
                    try:
                        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).date()
                        if end_dt > date.today():
                            is_prepaid = True
                            logger.info(f"[WEBHOOK REPLAY] CJ cancelled but prepaid — end_date {end_dt} in future")
                    except Exception:
                        pass

            # Extract customer
            if payload_type == "subscription":
                customer_data = payload.get("customer", {})
                subscription_data = payload
                order_data = payload.get("order", {})
            elif payload_type == "order":
                customer_data = payload.get("customer", payload.get("order", {}).get("customer", {}))
                order_data = payload
                subscription_data = payload.get("subscription", {})
            else:
                customer_data = payload.get("customer", {})
                order_data = payload.get("order", payload)
                subscription_data = payload.get("subscription", {})

            if not isinstance(customer_data, dict):
                customer_data = {}

            email = (str(customer_data.get("email", "") or "")).strip().lower()
            first_name = customer_data.get("first_name", customer_data.get("name", ""))
            last_name = customer_data.get("last_name", "")
            cratejoy_customer_id = str(customer_data.get("id", ""))
            cj_order_id = str(payload.get("id", ""))

            address_data = customer_data.get("shipping_address", customer_data.get("address", {}))
            if isinstance(address_data, dict):
                address_line1 = address_data.get("street", address_data.get("address1", ""))
                city = address_data.get("city", "")
                province = address_data.get("state", address_data.get("province", ""))
                zip_code = address_data.get("zip_code", address_data.get("zip", ""))
                country = address_data.get("country", "US")
            else:
                address_line1 = city = province = zip_code = ""
                country = "US"

            # Due date from note
            due_date_str = None
            due_date = None
            note = (subscription_data or {}).get("note", (order_data or {}).get("note", "")) if isinstance(subscription_data, dict) else ""
            if note and isinstance(note, str):
                date_match = re.search(r'\d{4}-\d{2}-\d{2}', note)
                if date_match:
                    due_date_str = date_match.group()
            # Also check custom_fields
            custom_fields = customer_data.get("custom_fields", (subscription_data or {}).get("custom_fields", {})) if isinstance(subscription_data, dict) else {}
            if isinstance(custom_fields, dict):
                for key, val in custom_fields.items():
                    if "due" in key.lower() and val:
                        due_date_str = str(val)
                        break

            if due_date_str:
                due_date = parse_due_date(due_date_str)

            trimester = None
            if due_date:
                trimester = calculate_trimester(due_date, date.today())

            logger.info(f"[WEBHOOK REPLAY] CJ customer: {email}, sub_status={sub_status_str}, due_date={due_date}, T{trimester}")

            if email:
                existing_customer = db.table("customers").select("*").ilike("email", email).execute()
                customer_record = {
                    "email": email,
                    "first_name": first_name or None,
                    "last_name": last_name or None,
                    "cratejoy_customer_id": cratejoy_customer_id or None,
                    "address_line1": address_line1 or None,
                    "city": city or None,
                    "province": province or None,
                    "zip": zip_code or None,
                    "country": country or "US",
                }
                if due_date:
                    customer_record["due_date"] = due_date.isoformat()
                if trimester:
                    customer_record["trimester"] = trimester

                if is_cancelled and is_prepaid:
                    customer_record["subscription_status"] = "cancelled-prepaid"
                elif is_cancelled or is_expired:
                    customer_record["subscription_status"] = "cancelled-expired"

                if existing_customer.data:
                    cust_id = existing_customer.data[0]["id"]
                    existing_cust = existing_customer.data[0]
                    customer_record["platform"] = "both" if existing_cust.get("shopify_customer_id") else "cratejoy"
                    # Preserve existing data
                    if not due_date and existing_cust.get("due_date"):
                        logger.info(f"[WEBHOOK REPLAY] Preserving existing due_date: {existing_cust['due_date']}")
                    if not is_cancelled and not is_expired and sub_status_str not in ("suspended", "paused"):
                        if existing_cust.get("subscription_status") == "active":
                            customer_record.pop("subscription_status", None)
                    db.table("customers").update(customer_record).eq("id", cust_id).execute()
                    logger.info(f"[WEBHOOK REPLAY] Updated existing CJ customer: {cust_id}")
                else:
                    customer_record["platform"] = "cratejoy"
                    if "subscription_status" not in customer_record:
                        customer_record["subscription_status"] = "active"
                    result = db.table("customers").insert(customer_record).execute()
                    cust_id = result.data[0]["id"] if result.data else None
                    logger.info(f"[WEBHOOK REPLAY] Created new CJ customer: {cust_id}")

                # Re-read merged data
                db_trimester = trimester
                db_due_date = due_date.isoformat() if due_date else None
                db_clothing_size = ""
                if cust_id:
                    merged = db.table("customers").select("*").eq("id", cust_id).single().execute()
                    if merged.data:
                        db_trimester = merged.data.get("trimester")
                        db_due_date = merged.data.get("due_date")
                        db_clothing_size = merged.data.get("clothing_size", "")

                # Fetch survey results
                if cust_id and CRATEJOY_CLIENT_ID and CRATEJOY_CLIENT_SECRET:
                    cj_sub_id = payload.get("id", "")
                    if cj_sub_id:
                        try:
                            auth_str = base64.b64encode(f"{CRATEJOY_CLIENT_ID}:{CRATEJOY_CLIENT_SECRET}".encode()).decode()
                            cj_headers = {"Authorization": f"Basic {auth_str}", "Accept": "application/json"}
                            async with httpx.AsyncClient(timeout=15.0) as client:
                                survey_url = f"https://api.cratejoy.com/v1/product_survey_results/?subscription_id={cj_sub_id}"
                                logger.info(f"[WEBHOOK REPLAY] Fetching CJ survey: {survey_url}")
                                survey_resp = await client.get(survey_url, headers=cj_headers)
                                if survey_resp.status_code == 200:
                                    survey_data = survey_resp.json()
                                    update_from_survey = {}
                                    for sr in survey_data.get("results", []):
                                        for ans in sr.get("answers", []):
                                            field = ans.get("field", {})
                                            field_name = field.get("name", "").lower().strip() if isinstance(field, dict) else ""
                                            value = str(ans.get("value", "")).strip()
                                            if not value:
                                                continue
                                            if "due date" in field_name:
                                                parsed_due = parse_due_date(value)
                                                if parsed_due:
                                                    update_from_survey["due_date"] = parsed_due.isoformat()
                                                    update_from_survey["trimester"] = calculate_trimester(parsed_due, date.today())
                                                    db_due_date = update_from_survey["due_date"]
                                                    db_trimester = update_from_survey["trimester"]
                                            elif "clothing size" in field_name:
                                                norm = normalize_clothing_size(value)
                                                if norm:
                                                    update_from_survey["clothing_size"] = norm
                                                    db_clothing_size = norm
                                            elif "expecting" in field_name or "whom" in field_name:
                                                gl = value.lower()
                                                update_from_survey["baby_gender"] = "boy" if "boy" in gl else ("girl" if "girl" in gl else "unknown")
                                            elif "second parent" in field_name or "matching apparel" in field_name:
                                                update_from_survey["wants_daddy_item"] = value.lower() in ("yes", "y", "true", "sure")
                                    if update_from_survey:
                                        db.table("customers").update(update_from_survey).eq("id", cust_id).execute()
                                        logger.info(f"[WEBHOOK REPLAY] Updated CJ customer with survey: {list(update_from_survey.keys())}")
                                else:
                                    logger.warning(f"[WEBHOOK REPLAY] Survey API returned {survey_resp.status_code}")
                        except Exception as e:
                            logger.warning(f"[WEBHOOK REPLAY] Survey fetch failed (non-fatal): {e}")

                # Decision
                if cust_id:
                    if (is_cancelled and not is_prepaid) or is_expired:
                        db.table("decisions").insert({
                            "customer_id": cust_id, "kit_id": None, "kit_sku": None,
                            "decision_type": "skipped",
                            "reason": f"Subscription {sub_status_str} — no prepaid remaining",
                            "status": "rejected", "order_id": cj_order_id, "platform": "cratejoy",
                            "trimester": db_trimester, "ship_date": date.today().isoformat(),
                        }).execute()
                        logger.info(f"[WEBHOOK REPLAY] Saved skipped decision for cancelled CJ sub")
                    elif not db_trimester:
                        db.table("decisions").insert({
                            "customer_id": cust_id, "kit_id": None, "kit_sku": None,
                            "decision_type": "needs-data-entry",
                            "reason": "Missing quiz data — add due date & size manually, then re-curate",
                            "status": "pending", "order_id": cj_order_id, "platform": "cratejoy",
                            "trimester": None, "ship_date": date.today().isoformat(),
                        }).execute()
                        logger.info(f"[WEBHOOK REPLAY] Saved needs-data-entry decision for CJ customer")
                    else:
                        kit_decision = await assign_kit(cust_id, date.today())
                        logger.info(f"[WEBHOOK REPLAY] CJ Decision: {kit_decision['decision_type']} — {kit_decision.get('kit_sku', 'none')}")
                        db.table("decisions").insert({
                            "customer_id": cust_id,
                            "kit_id": kit_decision.get("kit_id"),
                            "kit_sku": kit_decision.get("kit_sku"),
                            "decision_type": kit_decision["decision_type"],
                            "reason": kit_decision["reason"],
                            "status": "pending", "order_id": cj_order_id, "platform": "cratejoy",
                            "trimester": db_trimester, "ship_date": date.today().isoformat(),
                        }).execute()
                        write_decision_to_sheet({
                            "date": date.today().isoformat(),
                            "customer_name": f"{first_name or ''} {last_name or ''}".strip(),
                            "email": email, "platform": "cratejoy", "trimester": db_trimester,
                            "order_type": event_type, "kit_sku": kit_decision.get("kit_sku", "—"),
                            "decision_type": kit_decision["decision_type"], "reason": kit_decision["reason"],
                            "order_id": str(cj_order_id), "due_date": db_due_date or "",
                            "clothing_size": db_clothing_size or "",
                        })

            # Mark replay as successful
            elapsed_ms = int((time.time() - start_time) * 1000)
            if replay_log_id:
                db.table("webhook_logs").update({"status": "processed", "processing_time_ms": elapsed_ms}).eq("id", replay_log_id).execute()
            # Mark original as replayed (isolated try/except — don't crash if CHECK constraint hasn't been updated yet)
            try:
                db.table("webhook_logs").update({"status": "replayed", "error_message": f"Replayed successfully → {replay_log_id}"}).eq("id", webhook_id).execute()
            except Exception as e:
                logger.warning(f"[WEBHOOK REPLAY] Could not mark original as 'replayed' (CHECK constraint?): {e}")
                try:
                    db.table("webhook_logs").update({"error_message": f"Replayed → {replay_log_id}"}).eq("id", webhook_id).execute()
                except Exception:
                    pass
            await log_activity("webhook", f"Replayed Cratejoy webhook (CJ ID: {cj_order_id})", f"Original: {webhook_id}, Replay: {replay_log_id}", "success")
            logger.info(f"[WEBHOOK REPLAY] Cratejoy replay complete in {elapsed_ms}ms")
            return RedirectResponse(f"/webhooks/{replay_log_id}", status_code=303)

        # ═══════════════════════════════════════════════════════
        # UNKNOWN SOURCE
        # ═══════════════════════════════════════════════════════
        else:
            logger.error(f"[WEBHOOK REPLAY] Unknown source '{source}' — cannot replay")
            if replay_log_id:
                db.table("webhook_logs").update({"status": "failed", "error_message": f"Unknown source: {source}"}).eq("id", replay_log_id).execute()
            return RedirectResponse(f"/webhooks/{webhook_id}", status_code=303)

    except Exception as e:
        logger.error(f"[WEBHOOK REPLAY] Error replaying webhook {webhook_id}: {e}", exc_info=True)
        elapsed_ms = int((time.time() - start_time) * 1000)
        if replay_log_id:
            db.table("webhook_logs").update({"status": "failed", "error_message": str(e), "processing_time_ms": elapsed_ms}).eq("id", replay_log_id).execute()
        await log_activity("webhook", f"Webhook replay failed: {e}", f"Original: {webhook_id}", "error")
        return RedirectResponse(f"/webhooks/{webhook_id}", status_code=303)


@app.get("/customers", response_class=HTMLResponse)
async def customers_page(request: Request):
    """View all customers. Supports filtering by trimester, platform, status, size, ?q= text search, and sorting."""
    try:
        db = get_supabase()
        q = request.query_params.get("q", "").strip().lower()
        msg = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")

        # Parse server-side filters
        f_trimester = request.query_params.get("trimester", "").strip()
        f_platform  = request.query_params.get("platform", "").strip()
        f_status    = request.query_params.get("status", "").strip()
        f_size      = request.query_params.get("size", "").strip()

        # Parse sort params (whitelist to prevent SQL injection)
        sort     = request.query_params.get("sort", "created_at")
        sort_dir = request.query_params.get("dir", "desc")
        if sort not in ALLOWED_SORTS_CUSTOMERS:
            sort     = "created_at"
            sort_dir = "desc"
        desc_order = (sort_dir == "desc")

        # True total count from DB (unfiltered) — not capped by 1000
        count_result    = db.table("customers").select("id", count="exact").execute()
        total_customers = count_result.count or 0
        logger.info(f"[CUSTOMERS PAGE] True DB total: {total_customers}")

        # Paginate all matching records to bypass Supabase 1000-row server cap
        all_customers: list = []
        offset    = 0
        PAGE_SIZE = 1000
        while True:
            q_obj = db.table("customers").select("*")
            if f_trimester:
                try:
                    q_obj = q_obj.eq("trimester", int(f_trimester))
                except ValueError:
                    pass
            if f_platform:
                q_obj = q_obj.eq("platform", f_platform)
            if f_status:
                q_obj = q_obj.eq("subscription_status", f_status)
            if f_size:
                q_obj = q_obj.eq("clothing_size", f_size)
            batch      = q_obj.order(sort, desc=desc_order).range(offset, offset + PAGE_SIZE - 1).execute()
            batch_data = batch.data or []
            all_customers.extend(batch_data)
            logger.debug(f"[CUSTOMERS PAGE] Batch offset={offset}, fetched={len(batch_data)}")
            if len(batch_data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        logger.info(f"[CUSTOMERS PAGE] After server filters: {len(all_customers)} customers")

        # Client-side text search on top of server-side filters
        if q:
            filtered = [
                c for c in all_customers
                if q in (c.get("email") or "").lower()
                or q in (c.get("first_name") or "").lower()
                or q in (c.get("last_name") or "").lower()
                or q in f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".lower()
            ]
            logger.info(f"[CUSTOMERS PAGE] Text search '{q}' matched {len(filtered)}/{len(all_customers)}")
        else:
            filtered = all_customers

        # Active vs Past split
        # Active: status active/cancelled-prepaid/paused AND (recent shipment within 3 months OR no due_date or recent due_date)
        # Past: everything else (cancelled-expired, or no activity in 3+ months)
        today  = date.today()
        shipment_cutoff = today - timedelta(days=90)  # 3 months
        due_date_cutoff = today - timedelta(days=150)  # fallback for customers without shipment data

        # Bulk-load latest shipment dates for all customer IDs in this page
        customer_ids_on_page = [c["id"] for c in filtered if c.get("id")]
        latest_shipment_map: dict = {}
        if customer_ids_on_page:
            # Load shipments in chunks (Supabase filter limit)
            for chunk_start in range(0, len(customer_ids_on_page), 200):
                chunk_ids = customer_ids_on_page[chunk_start:chunk_start + 200]
                ship_batch = db.table("shipments").select("customer_id, ship_date").in_("customer_id", chunk_ids).execute()
                for s in (ship_batch.data or []):
                    cid = s["customer_id"]
                    sd = s.get("ship_date") or ""
                    if sd and (cid not in latest_shipment_map or sd > latest_shipment_map[cid]):
                        latest_shipment_map[cid] = sd

        active_customers: list = []
        past_customers:   list = []
        for c in filtered:
            s = c.get("subscription_status", "")
            cid = c.get("id", "")

            # Cancelled-expired always goes to past
            if s == "cancelled-expired":
                past_customers.append(c)
                continue

            # Check subscription status first
            if s not in ("active", "cancelled-prepaid", "paused"):
                past_customers.append(c)
                continue

            # Check for recent shipment (primary signal)
            last_ship = latest_shipment_map.get(cid, "")
            if last_ship:
                try:
                    last_ship_dt = date.fromisoformat(str(last_ship)[:10])
                    if last_ship_dt >= shipment_cutoff:
                        active_customers.append(c)
                        continue
                except (ValueError, TypeError):
                    pass

            # Fallback to due date for customers without shipment history
            dd_raw = c.get("due_date")
            if dd_raw:
                try:
                    dd = date.fromisoformat(str(dd_raw))
                    if dd >= due_date_cutoff:
                        active_customers.append(c)
                        continue
                except (ValueError, TypeError):
                    pass

            # No due date and no recent shipment but active status — still show as active
            if not dd_raw and not last_ship:
                active_customers.append(c)
                continue

            past_customers.append(c)
        logger.info(f"[CUSTOMERS PAGE] Active={len(active_customers)}, Past={len(past_customers)}")

        # Build filter query string for sort links
        filter_qs_parts = []
        if f_trimester: filter_qs_parts.append(f"trimester={f_trimester}")
        if f_platform:  filter_qs_parts.append(f"platform={f_platform}")
        if f_status:    filter_qs_parts.append(f"status={f_status}")
        if f_size:      filter_qs_parts.append(f"size={f_size}")
        if q:           filter_qs_parts.append(f"q={q}")
        filter_qs         = "&".join(filter_qs_parts)
        any_filter_active = bool(f_trimester or f_platform or f_status or f_size or q)

        return templates.TemplateResponse("customers.html", {
            "request":          request,
            "customers":        filtered,
            "active_customers": active_customers,
            "past_customers":   past_customers,
            "total_customers":  total_customers,
            "search_query":     q,
            "msg":              msg,
            "msg_type":         msg_type,
            "page":             "customers",
            "filters": {
                "trimester": f_trimester,
                "platform":  f_platform,
                "status":    f_status,
                "size":      f_size,
            },
            "sort":             sort,
            "sort_dir":         sort_dir,
            "filter_qs":        filter_qs,
            "any_filter_active": any_filter_active,
        })
    except Exception as e:
        logger.error(f"[CUSTOMERS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("customers.html", {
            "request":          request,
            "customers":        [],
            "active_customers": [],
            "past_customers":   [],
            "total_customers":  0,
            "search_query":     "",
            "error":            str(e),
            "msg":              "",
            "msg_type":         "error",
            "page":             "customers",
            "filters":          {},
            "sort":             "created_at",
            "sort_dir":         "desc",
            "filter_qs":        "",
            "any_filter_active": False,
        })


@app.get("/customers/export-csv")
async def export_customers_csv(request: Request):
    """Export filtered customers as a downloadable CSV."""
    try:
        db = get_supabase()

        # Re-apply same filter params as customers page
        f_trimester = request.query_params.get("trimester", "").strip()
        f_platform  = request.query_params.get("platform", "").strip()
        f_status    = request.query_params.get("status", "").strip()
        f_size      = request.query_params.get("size", "").strip()
        q           = request.query_params.get("q", "").strip().lower()

        def _build_cust_q():
            qo = db.table("customers").select("*")
            if f_trimester:
                try:
                    qo = qo.eq("trimester", int(f_trimester))
                except ValueError:
                    pass
            if f_platform:
                qo = qo.eq("platform", f_platform)
            if f_status:
                qo = qo.eq("subscription_status", f_status)
            if f_size:
                qo = qo.eq("clothing_size", f_size)
            return qo

        rows: list = []
        offset = 0
        PAGE_SIZE = 1000
        while True:
            batch = _build_cust_q().order("created_at", desc=True).range(offset, offset + PAGE_SIZE - 1).execute()
            batch_data = batch.data or []
            rows.extend(batch_data)
            if len(batch_data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        # Client-side text search
        if q:
            rows = [
                c for c in rows
                if q in (c.get("email") or "").lower()
                or q in (c.get("first_name") or "").lower()
                or q in (c.get("last_name") or "").lower()
                or q in f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".lower()
            ]

        logger.info(f"[CUSTOMER EXPORT] Exporting {len(rows)} customers")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Name", "Email", "Platform", "Status",
            "Trimester", "Due Date", "Size", "Gender",
            "Phone", "Address", "City", "State", "Zip", "Country",
        ])
        for c in rows:
            name = f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".strip()
            writer.writerow([
                name,
                c.get("email", ""),
                c.get("platform", ""),
                c.get("subscription_status", ""),
                f"T{c.get('trimester', '')}" if c.get("trimester") else "",
                c.get("due_date", ""),
                c.get("clothing_size", ""),
                c.get("baby_gender", ""),
                c.get("phone") or "",
                c.get("address_line1") or "",
                c.get("city") or "",
                c.get("province") or "",
                c.get("zip") or "",
                c.get("country") or "US",
            ])

        output.seek(0)
        filename = f"customers_export_{date.today().isoformat()}.csv"
        await log_activity("export", f"Customer CSV export: {len(rows)} rows", "", "success")
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        logger.error(f"[CUSTOMER EXPORT] Error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/customers/{customer_id}", response_class=HTMLResponse)
async def customer_detail(request: Request, customer_id: str):
    """View a single customer's details and history."""
    try:
        db = get_supabase()
        cust = db.table("customers").select("*").eq("id", customer_id).single().execute()
        decisions = db.table("decisions").select("*").eq("customer_id", customer_id).order("created_at", desc=True).execute()
        shipments = load_customer_shipments_with_items(customer_id)
        msg = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")

        # Live trimester recalculation — stored value may be stale if due date passed a boundary
        stored_trimester = cust.data.get("trimester")
        due_date_raw = cust.data.get("due_date")
        live_trimester = stored_trimester
        trimester_changed = False
        if due_date_raw:
            try:
                due_dt = date.fromisoformat(str(due_date_raw))
                live_trimester = calculate_trimester(due_dt, date.today())
                if live_trimester != stored_trimester:
                    trimester_changed = True
                    logger.info(
                        f"[CUSTOMER DETAIL] Trimester mismatch for {customer_id}: "
                        f"stored T{stored_trimester} → live T{live_trimester} (due_date={due_date_raw})"
                    )
                    # Auto-save immediately — no need for user to Edit→Save
                    try:
                        db.table("customers").update({"trimester": live_trimester}).eq("id", customer_id).execute()
                        cust.data["trimester"] = live_trimester
                        logger.info(f"[CUSTOMER DETAIL] Auto-saved trimester T{live_trimester} for {customer_id}")
                    except Exception as save_err:
                        logger.error(f"[CUSTOMER DETAIL] Failed to auto-save trimester: {save_err}")
            except Exception as tri_err:
                logger.warning(f"[CUSTOMER DETAIL] Could not recalculate trimester: {tri_err}")

        # Get all kits for the shipment form dropdown and override form
        kits_list = db.table("kits").select("id, sku, trimester, is_welcome_kit").order("sku").execute()

        return templates.TemplateResponse("customer_detail.html", {
            "request": request,
            "customer": cust.data,
            "decisions": decisions.data or [],
            "shipments": shipments,
            "kits": kits_list.data or [],
            "stored_trimester": stored_trimester,
            "live_trimester": live_trimester,
            "trimester_changed": trimester_changed,
            "msg": msg,
            "msg_type": msg_type,
            "page": "customers",
        })
    except Exception as e:
        logger.error(f"[CUSTOMER DETAIL] Error: {e}", exc_info=True)
        return HTMLResponse(f"Customer not found: {e}", status_code=404)


@app.get("/decisions", response_class=HTMLResponse)
async def decisions_page(request: Request):
    """View kit assignment decisions. Supports filtering by trimester, status, type, platform, month, and sorting."""
    try:
        db = get_supabase()
        msg      = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")

        # Parse filters
        f_trimester = request.query_params.get("trimester", "").strip()
        f_status    = request.query_params.get("status", "").strip()
        f_type      = request.query_params.get("type", "").strip()
        f_platform  = request.query_params.get("platform", "").strip()
        f_month     = request.query_params.get("month", "").strip()
        q           = request.query_params.get("q", "").strip().lower()

        # Parse sort (whitelist)
        sort     = request.query_params.get("sort", "created_at")
        sort_dir = request.query_params.get("dir", "desc")
        if sort not in ALLOWED_SORTS_DECISIONS:
            sort     = "created_at"
            sort_dir = "desc"
        desc_order = (sort_dir == "desc")

        # True total count (unfiltered) for header display
        count_result     = db.table("decisions").select("id", count="exact").execute()
        total_decisions  = count_result.count or 0
        logger.info(f"[DECISIONS PAGE] True DB total: {total_decisions}")

        # Build Supabase query with server-side filters — helper to rebuild for pagination
        def _build_q():
            qo = db.table("decisions").select("*, customers(email, first_name, last_name)")
            if f_trimester:
                try:
                    qo = qo.eq("trimester", int(f_trimester))
                except ValueError:
                    pass
            if f_status:
                qo = qo.eq("status", f_status)
            if f_type:
                qo = qo.eq("decision_type", f_type)
            if f_platform:
                qo = qo.eq("platform", f_platform)
            if f_month:
                try:
                    y2, m2    = int(f_month[:4]), int(f_month[5:7])
                    start_dt2 = f"{y2}-{m2:02d}-01"
                    m2 += 1
                    if m2 > 12:
                        m2 = 1; y2 += 1
                    end_dt2  = f"{y2}-{m2:02d}-01"
                    qo = qo.gte("created_at", start_dt2).lt("created_at", end_dt2)
                except Exception as me:
                    logger.warning(f"[DECISIONS PAGE] Invalid month param '{f_month}': {me}")
            return qo

        # customer_name is a virtual sort (joined field) — use created_at for DB query, sort in Python after
        db_sort = sort if sort != "customer_name" else "created_at"

        # Paginate all matching records past Supabase 1000-row cap
        all_decisions: list = []
        offset    = 0
        PAGE_SIZE = 1000
        while True:
            batch = _build_q().order(db_sort, desc=desc_order).range(offset, offset + PAGE_SIZE - 1).execute()
            batch_data = batch.data or []
            all_decisions.extend(batch_data)
            logger.debug(f"[DECISIONS PAGE] Batch offset={offset}, fetched={len(batch_data)}")
            if len(batch_data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        # Python-side sort for customer_name (joined field, can't sort in Supabase)
        if sort == "customer_name":
            def _cust_sort_key(d):
                c = d.get("customers") or {}
                return f"{(c.get('first_name') or '').lower()} {(c.get('last_name') or '').lower()}"
            all_decisions.sort(key=_cust_sort_key, reverse=desc_order)

        logger.info(f"[DECISIONS PAGE] After server filters: {len(all_decisions)} decisions (t={f_trimester}, s={f_status}, type={f_type}, platform={f_platform}, month={f_month})")

        # Client-side text search on customer name/email/kit_sku (runs AFTER all rows fetched)
        if q:
            all_decisions = [
                d for d in all_decisions
                if (d.get("customers") and (
                    q in (d["customers"].get("email") or "").lower()
                    or q in (d["customers"].get("first_name") or "").lower()
                    or q in (d["customers"].get("last_name") or "").lower()
                ))
                or q in (d.get("kit_sku") or "").lower()
            ]
            logger.info(f"[DECISIONS PAGE] Text search '{q}' → {len(all_decisions)} decisions")

        # Build filter query string for sort links and export URLs
        filter_qs_parts = []
        if f_trimester: filter_qs_parts.append(f"trimester={f_trimester}")
        if f_status:    filter_qs_parts.append(f"status={f_status}")
        if f_type:      filter_qs_parts.append(f"type={f_type}")
        if f_platform:  filter_qs_parts.append(f"platform={f_platform}")
        if f_month:     filter_qs_parts.append(f"month={f_month}")
        if q:           filter_qs_parts.append(f"q={q}")
        filter_qs = "&".join(filter_qs_parts)

        any_filter_active = bool(f_trimester or f_status or f_type or f_platform or f_month or q)

        return templates.TemplateResponse("decisions.html", {
            "request":          request,
            "decisions":        all_decisions,
            "total_decisions":  total_decisions,
            "msg":              msg,
            "msg_type":         msg_type,
            "page":             "decisions",
            "filters": {
                "trimester": f_trimester,
                "status":    f_status,
                "type":      f_type,
                "platform":  f_platform,
                "month":     f_month,
                "q":         q,
            },
            "sort":             sort,
            "sort_dir":         sort_dir,
            "filter_qs":        filter_qs,
            "any_filter_active": any_filter_active,
        })
    except Exception as e:
        logger.error(f"[DECISIONS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("decisions.html", {
            "request":          request,
            "decisions":        [],
            "total_decisions":  0,
            "error":            str(e),
            "msg":              "",
            "msg_type":         "error",
            "page":             "decisions",
            "filters":          {},
            "sort":             "created_at",
            "sort_dir":         "desc",
            "filter_qs":        "",
            "any_filter_active": False,
        })


@app.get("/kits", response_class=HTMLResponse)
async def kits_page(request: Request):
    """View and manage kits inventory."""
    try:
        db = get_supabase()
        kits_data = db.table("kits").select("*").execute()
        msg = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")

        # Get item counts per kit
        kit_item_counts = {}
        if kits_data.data:
            all_kit_items = db.table("kit_items").select("kit_id").execute()
            for ki in (all_kit_items.data or []):
                kid = ki["kit_id"]
                kit_item_counts[kid] = kit_item_counts.get(kid, 0) + 1

        # Parse sort params for kits (Python sort — all kits fit in memory)
        kits_sort     = request.query_params.get("sort", "age_rank")
        kits_sort_dir = request.query_params.get("dir", "asc")
        if kits_sort not in ALLOWED_SORTS_KITS:
            kits_sort     = "age_rank"
            kits_sort_dir = "asc"
        kits_desc = (kits_sort_dir == "desc")

        def _kits_sort_key(k):
            val = k.get(kits_sort)
            if val is None:
                return "" if kits_sort in ("sku", "name") else 0
            return val

        # Split into welcome kits and regular kits, sorted by selected column
        all_kits = kits_data.data or []
        try:
            welcome_kits = sorted(
                [k for k in all_kits if k.get("is_welcome_kit")],
                key=_kits_sort_key, reverse=kits_desc
            )
            regular_kits = sorted(
                [k for k in all_kits if not k.get("is_welcome_kit")],
                key=_kits_sort_key, reverse=kits_desc
            )
        except TypeError:
            # Fallback for mixed-type values
            welcome_kits = sorted([k for k in all_kits if k.get("is_welcome_kit")],     key=lambda k: (k.get("trimester", 0), k.get("age_rank", 0)))
            regular_kits = sorted([k for k in all_kits if not k.get("is_welcome_kit")], key=lambda k: (k.get("trimester", 0), k.get("age_rank", 0)))
        logger.info(f"[KITS PAGE] {len(welcome_kits)} welcome kits, {len(regular_kits)} regular kits, sort={kits_sort} {kits_sort_dir}")

        # Get all items for the kit creation form
        all_items = db.table("items").select("*").order("name").execute()

        # Phase 3 — unresolved low-stock alerts from VeraCore inventory sync
        stock_alerts = []
        try:
            alerts_res = db.table("kit_stock_alerts").select(
                "id, kit_id, seen_qty, threshold, alerted_at, kits(sku, name)"
            ).eq("resolved", False).order("alerted_at", desc=True).execute()
            stock_alerts = alerts_res.data or []
        except Exception as alert_err:
            # Table may not exist if migration 012 hasn't been applied yet.
            logger.warning("[KITS PAGE] Could not load stock alerts (migration 012 applied?): %s", alert_err)

        return templates.TemplateResponse("kits.html", {
            "request": request,
            "kits": all_kits,           # kept for backwards compat
            "welcome_kits": welcome_kits,
            "regular_kits": regular_kits,
            "kit_item_counts": kit_item_counts,
            "all_items": all_items.data or [],
            "stock_alerts": stock_alerts,
            "msg": msg,
            "msg_type": msg_type,
            "page": "kits",
            "sort": kits_sort,
            "sort_dir": kits_sort_dir,
        })
    except Exception as e:
        logger.error(f"[KITS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("kits.html", {
            "request": request,
            "kits": [],
            "welcome_kits": [],
            "regular_kits": [],
            "kit_item_counts": {},
            "all_items": [],
            "stock_alerts": [],
            "error": str(e),
            "msg": "",
            "msg_type": "error",
            "page": "kits",
            "sort": "age_rank",
            "sort_dir": "asc",
        })


@app.post("/kits/add")
async def add_kit(
    request: Request,
    sku: str = Form(...),
    name: str = Form(""),
    trimester: int = Form(...),
    size_variant: int = Form(1),
    is_welcome_kit: str = Form(""),
    is_universal: str = Form(""),
    quantity_available: int = Form(0),
    age_rank: int = Form(0),
    cost_per_kit: float = Form(0),
):
    """Add a new kit with optional item selection."""
    try:
        db = get_supabase()
        welcome = is_welcome_kit.lower() in ("true", "on", "1", "yes") if is_welcome_kit else False
        universal = is_universal.lower() in ("true", "on", "1", "yes") if is_universal else False
        sku_clean = sku.strip().upper()
        # Auto-compute age_rank from SKU when not manually set (age_rank == 0)
        if age_rank == 0:
            age_rank = compute_age_rank_from_sku(sku_clean)
            logger.info(f"[KITS] Auto-computed age_rank={age_rank} from SKU '{sku_clean}'")
        logger.info(f"[KITS] Adding kit: sku={sku_clean}, T{trimester}, size={size_variant}, universal={universal}, welcome={welcome}, qty={quantity_available}, age_rank={age_rank} (source=auto)")
        kit_result = db.table("kits").insert({
            "sku": sku_clean,
            "name": name.strip() or None,
            "trimester": trimester,
            "size_variant": size_variant,
            "is_welcome_kit": welcome,
            "is_universal": universal,
            "quantity_available": quantity_available,
            "age_rank": age_rank,
            "age_rank_source": "auto",
            "cost_per_kit": cost_per_kit if cost_per_kit > 0 else None,
        }).execute()
        kit_id = kit_result.data[0]["id"] if kit_result.data else None
        logger.info(f"[KITS] ✅ Added kit: {sku_clean}, id={kit_id}")

        # Link selected items to the kit
        if kit_id:
            form_data = await request.form()
            selected_items = form_data.getlist("item_ids")
            logger.info(f"[KITS] Selected items for kit {sku_clean}: {selected_items}")
            for item_id in selected_items:
                if item_id and item_id.strip():
                    try:
                        db.table("kit_items").insert({
                            "kit_id": kit_id,
                            "item_id": item_id.strip(),
                            "quantity": 1,
                        }).execute()
                        logger.info(f"[KITS] Linked item {item_id} to kit {sku_clean}")
                    except Exception as link_err:
                        logger.warning(f"[KITS] Could not link item {item_id}: {link_err}")

            items_count = len(selected_items)
            await log_activity("kit", f"Kit {sku_clean} added with {items_count} items", f"T{trimester}, Qty: {quantity_available}, Welcome: {welcome}", "success")
            # Redirect to kit detail so user can see and edit items
            return RedirectResponse(f"/kits/{kit_id}", status_code=303)

        await log_activity("kit", f"Kit {sku_clean} added", f"T{trimester}, Qty: {quantity_available}, Welcome: {welcome}", "success")
    except Exception as e:
        logger.error(f"[KITS] Error adding kit: {e}", exc_info=True)
        await log_activity("kit", f"Failed to add kit {sku}: {e}", "", "error")
    return RedirectResponse("/kits", status_code=303)


@app.get("/items", response_class=HTMLResponse)
async def items_page(request: Request):
    """View and manage items."""
    try:
        db = get_supabase()
        items_data = db.table("items").select("*").order("name").execute()
        msg = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")
        return templates.TemplateResponse("items.html", {
            "request": request,
            "items": items_data.data or [],
            "msg": msg,
            "msg_type": msg_type,
            "page": "items",
            "today_str": date.today().isoformat(),
        })
    except Exception as e:
        logger.error(f"[ITEMS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("items.html", {
            "request": request,
            "items": [],
            "error": str(e),
            "msg": "",
            "msg_type": "error",
            "page": "items",
            "today_str": date.today().isoformat(),
        })


@app.post("/items/add")
async def add_item(
    request: Request,
    name: str = Form(...),
    sku: str = Form(""),
    category: str = Form(""),
    unit_cost: float = Form(0),
    is_therabox: str = Form(""),
    expiry_date: str = Form(""),
):
    """Add a new item."""
    try:
        db = get_supabase()
        therabox = is_therabox.lower() in ("true", "on", "1", "yes") if is_therabox else False
        name_clean = name.strip()
        sku_clean = sku.strip().upper() or None
        logger.info(f"[ITEMS] Adding item: name='{name_clean}', sku={sku_clean}, category={category}, therabox={therabox}")
        item_data = {
            "name": name_clean,
            "sku": sku_clean,
            "category": category.strip() or None,
            "unit_cost": unit_cost if unit_cost > 0 else None,
            "is_therabox": therabox,
        }
        if expiry_date and expiry_date.strip():
            item_data["expiry_date"] = expiry_date.strip()
        db.table("items").insert(item_data).execute()
        logger.info(f"[ITEMS] ✅ Added item: {name_clean}")
        await log_activity("item", f"Item '{name_clean}' added", f"SKU: {sku_clean}, TheraBox: {therabox}", "success")
    except Exception as e:
        logger.error(f"[ITEMS] Error adding item: {e}", exc_info=True)
        await log_activity("item", f"Failed to add item: {e}", "", "error")
    return RedirectResponse("/items", status_code=303)


# ─── Item Alternatives (Pipe Items) ───

@app.get("/item-alternatives", response_class=HTMLResponse)
async def item_alternatives_page(request: Request):
    """View and manage item alternatives (pipe items)."""
    try:
        db = get_supabase()
        msg = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")

        # Load all items for the dropdowns
        items_data = db.table("items").select("id, name, sku").order("name").execute()
        items_list = items_data.data or []
        item_lookup = {i["id"]: i for i in items_list}

        # Load existing alternatives
        alts_data = db.table("item_alternatives").select("item_id, alternative_item_id").execute()
        alternatives = []
        for a in (alts_data.data or []):
            item = item_lookup.get(a["item_id"], {})
            alt = item_lookup.get(a["alternative_item_id"], {})
            alternatives.append({
                "item_id": a["item_id"],
                "alternative_item_id": a["alternative_item_id"],
                "item_name": item.get("name", "Unknown"),
                "item_sku": item.get("sku"),
                "alt_name": alt.get("name", "Unknown"),
                "alt_sku": alt.get("sku"),
            })

        logger.info(f"[ITEM ALTS PAGE] {len(alternatives)} alternative pairs loaded")
        return templates.TemplateResponse("item_alternatives.html", {
            "request": request,
            "items": items_list,
            "alternatives": alternatives,
            "msg": msg,
            "msg_type": msg_type,
            "page": "item-alternatives",
        })
    except Exception as e:
        logger.error(f"[ITEM ALTS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("item_alternatives.html", {
            "request": request,
            "items": [],
            "alternatives": [],
            "error": str(e),
            "msg": "",
            "msg_type": "error",
            "page": "item-alternatives",
        })


@app.post("/item-alternatives/add")
async def add_item_alternative(
    item_id: str = Form(...),
    alternative_item_id: str = Form(...),
):
    """Add an item alternative pair."""
    try:
        db = get_supabase()
        item_id = item_id.strip()
        alternative_item_id = alternative_item_id.strip()

        if item_id == alternative_item_id:
            logger.warning(f"[ITEM ALTS] Cannot add self as alternative: {item_id}")
            return RedirectResponse("/item-alternatives?msg=Cannot+add+an+item+as+its+own+alternative&msg_type=error", status_code=303)

        # Insert both directions for easy lookup
        logger.info(f"[ITEM ALTS] Adding alternative pair: {item_id} ↔ {alternative_item_id}")
        try:
            db.table("item_alternatives").insert({"item_id": item_id, "alternative_item_id": alternative_item_id}).execute()
        except Exception as dup:
            if "duplicate" in str(dup).lower() or "23505" in str(dup):
                logger.info(f"[ITEM ALTS] Pair A→B already exists, skipping")
            else:
                raise
        try:
            db.table("item_alternatives").insert({"item_id": alternative_item_id, "alternative_item_id": item_id}).execute()
        except Exception as dup:
            if "duplicate" in str(dup).lower() or "23505" in str(dup):
                logger.info(f"[ITEM ALTS] Pair B→A already exists, skipping")
            else:
                raise

        # Get item names for logging
        item_a = db.table("items").select("name").eq("id", item_id).single().execute()
        item_b = db.table("items").select("name").eq("id", alternative_item_id).single().execute()
        name_a = item_a.data["name"] if item_a.data else item_id
        name_b = item_b.data["name"] if item_b.data else alternative_item_id
        logger.info(f"[ITEM ALTS] ✅ Added: {name_a} ↔ {name_b}")
        await log_activity("item_alternative", f"Added alternative: {name_a} ↔ {name_b}", "", "success")
        return RedirectResponse(f"/item-alternatives?msg=Added+alternative:+{quote(name_a)}+↔+{quote(name_b)}", status_code=303)
    except Exception as e:
        logger.error(f"[ITEM ALTS] Error adding alternative: {e}", exc_info=True)
        return RedirectResponse(f"/item-alternatives?msg=Error:+{quote(str(e))}&msg_type=error", status_code=303)


@app.post("/item-alternatives/remove")
async def remove_item_alternative(
    item_id: str = Form(...),
    alternative_item_id: str = Form(...),
):
    """Remove an item alternative pair (both directions)."""
    try:
        db = get_supabase()
        logger.info(f"[ITEM ALTS] Removing alternative pair: {item_id} ↔ {alternative_item_id}")
        db.table("item_alternatives").delete().eq("item_id", item_id).eq("alternative_item_id", alternative_item_id).execute()
        db.table("item_alternatives").delete().eq("item_id", alternative_item_id).eq("alternative_item_id", item_id).execute()
        logger.info(f"[ITEM ALTS] ✅ Removed alternative pair")
        await log_activity("item_alternative", "Removed alternative pair", "", "success")
        return RedirectResponse("/item-alternatives?msg=Alternative+pair+removed", status_code=303)
    except Exception as e:
        logger.error(f"[ITEM ALTS] Error removing: {e}", exc_info=True)
        return RedirectResponse(f"/item-alternatives?msg=Error:+{quote(str(e))}&msg_type=error", status_code=303)


# ═══════════════════════════════════════════════════════════════════
# CURATION REPORT ROUTES (Phase 2)
# ═══════════════════════════════════════════════════════════════════

from curation_report import run_monthly_report
from projection_engine import project_forward, load_committed_items
import json as json_module

# ─── In-memory job registry ───────────────────────────────────────
# Stores background job state for curation report / forward planner.
# Heroku has a 30-second request timeout (H12). Both operations take
# ~40s, so we return immediately, run in BackgroundTasks, and the
# client polls /job/{job_id}/status for completion.
#
# Schema: {job_id: {type, status, result, error, created_at, params}}
# type: "curation_report" | "forward_planner"
# status: "running" | "done" | "error"
_jobs: dict = {}
_jobs_lock = threading.Lock()


def _create_job(job_type: str, params: dict) -> str:
    """Create a new job entry and return its ID."""
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "type": job_type,
            "status": "running",
            "result": None,
            "error": None,
            "created_at": datetime.utcnow().isoformat(),
            "params": params,
        }
    logger.info(f"[JOBS] Created job {job_id} type={job_type}")
    return job_id


def _finish_job(job_id: str, result: dict):
    """Mark job as done with result."""
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["result"] = result
    logger.info(f"[JOBS] Job {job_id} finished successfully")


def _fail_job(job_id: str, error: str):
    """Mark job as failed with error message."""
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["error"] = error
    logger.error(f"[JOBS] Job {job_id} failed: {error}")


def _get_job(job_id: str) -> dict | None:
    with _jobs_lock:
        return _jobs.get(job_id)


@app.get("/curation-report", response_class=HTMLResponse)
async def curation_report_page(request: Request, msg: str = "", msg_type: str = ""):
    """Main curation report page — shows form to generate + previous runs."""
    try:
        db = get_supabase()
        logger.info("[CURATION PAGE] Loading curation report page")

        # Load previous runs
        runs = db.table("curation_runs").select("*").order("generated_at", desc=True).limit(20).execute()
        previous_runs = runs.data or []

        # Parse summary_json for display
        for run in previous_runs:
            if isinstance(run.get("summary_json"), str):
                try:
                    run["summary_json"] = json_module.loads(run["summary_json"])
                except Exception:
                    run["summary_json"] = None

        # Default month = next month
        from datetime import date, timedelta
        today = date.today()
        if today.month == 12:
            default_month = f"{today.year + 1}-01"
        else:
            default_month = f"{today.year}-{today.month + 1:02d}"

        return templates.TemplateResponse("curation_report.html", {
            "request": request,
            "page": "curation-report",
            "msg": msg,
            "msg_type": msg_type,
            "previous_runs": previous_runs,
            "report": None,
            "default_month": default_month,
        })
    except Exception as e:
        logger.error(f"[CURATION PAGE] Error loading page: {e}", exc_info=True)
        return templates.TemplateResponse("curation_report.html", {
            "request": request,
            "page": "curation-report",
            "msg": f"Error loading page: {e}",
            "msg_type": "error",
            "previous_runs": [],
            "report": None,
            "default_month": "",
        })


@app.post("/curation-report/{run_id}/delete")
async def delete_curation_run(run_id: str):
    """Delete a curation report run."""
    try:
        db = get_supabase()
        db.table("curation_runs").delete().eq("id", run_id).execute()
        logger.info(f"[CURATION DELETE] Deleted curation run {run_id}")
    except Exception as e:
        logger.error(f"[CURATION DELETE] Error deleting run {run_id}: {e}", exc_info=True)
    return RedirectResponse("/curation-report?msg=Report+deleted&msg_type=success", status_code=303)


@app.post("/curation-report/generate")
async def generate_curation_report(
    request: Request,
    background_tasks: BackgroundTasks,
    report_month: str = Form(...),
    ship_day: int = Form(14),
    warehouse_min: int = Form(100),
    lookback_months: int = Form(4),
    recency_months: int = Form(3),
    include_paused: str = Form(""),
):
    """Start curation report generation as a background job.
    Returns immediately (HTTP 303) to avoid Heroku's 30-second H12 timeout.
    The client polls /curation-report/job/{job_id}/status for completion.
    """
    paused = include_paused in ("1", "on", "true")
    recency = recency_months if recency_months > 0 else None
    params = {
        "report_month": report_month,
        "ship_day": ship_day,
        "warehouse_min": warehouse_min,
        "lookback_months": lookback_months,
        "recency_months": recency,
        "include_paused": paused,
    }
    job_id = _create_job("curation_report", params)
    logger.info(f"[CURATION GEN] Started background job {job_id} for {report_month}")
    background_tasks.add_task(_run_curation_report_job, job_id, params)
    return RedirectResponse(f"/curation-report/job/{job_id}", status_code=303)


def _run_curation_report_job(job_id: str, params: dict):
    """Background worker: runs the full curation report and stores result in job registry."""
    try:
        db = get_supabase()
        report_month = params["report_month"]
        ship_day = params["ship_day"]
        warehouse_min = params["warehouse_min"]
        lookback_months = params["lookback_months"]
        recency = params["recency_months"]
        paused = params["include_paused"]

        logger.info(f"[CURATION JOB] {job_id} — Generating report: month={report_month}, ship_day={ship_day}, wh_min={warehouse_min}, lookback={lookback_months}, recency={recency}")

        year, month_num = int(report_month.split("-")[0]), int(report_month.split("-")[1])
        ship_date = date(year, month_num, ship_day)

        # Create curation_run record (status=running)
        run_insert = db.table("curation_runs").insert({
            "report_month": report_month,
            "ship_date": str(ship_date),
            "warehouse_minimum": warehouse_min,
            "include_paused": paused,
            "lookback_months": lookback_months,
            "status": "running",
        }).execute()
        run_id = run_insert.data[0]["id"]
        logger.info(f"[CURATION JOB] {job_id} — Created curation_run {run_id}")

        # Run the report (the heavy part)
        report = run_monthly_report(
            db=db,
            report_month=report_month,
            ship_date=ship_date,
            warehouse_minimum=warehouse_min,
            include_paused=paused,
            lookback_months=lookback_months,
            recency_months=recency,
        )

        # Save detailed results to DB
        _save_report_details(db, run_id, report)

        # Mark curation_run completed
        db.table("curation_runs").update({
            "status": "completed",
            "summary_json": report["executive"],
            "completed_at": str(date.today()),
        }).eq("id", run_id).execute()

        logger.info(f"[CURATION JOB] {job_id} — Completed. run_id={run_id}")
        _finish_job(job_id, {"run_id": run_id, "report_month": report_month})

    except Exception as e:
        logger.error(f"[CURATION JOB] {job_id} — Error: {e}", exc_info=True)
        # Try to update DB run status
        try:
            if "run_id" in locals():
                db.table("curation_runs").update({
                    "status": "error",
                    "error_message": str(e)[:500],
                }).eq("id", run_id).execute()
        except Exception:
            pass
        _fail_job(job_id, str(e)[:500])


@app.get("/curation-report/job/{job_id}", response_class=HTMLResponse)
async def curation_report_job_page(request: Request, job_id: str):
    """Loading/result page for a curation report background job."""
    job = _get_job(job_id)
    if not job:
        return RedirectResponse("/curation-report?msg=Job+not+found&msg_type=error", status_code=303)

    if job["status"] == "done":
        run_id = job["result"]["run_id"]
        return RedirectResponse(f"/curation-report/{run_id}?msg=Report+generated+successfully", status_code=303)

    if job["status"] == "error":
        return templates.TemplateResponse("job_loading.html", {
            "request": request,
            "job_id": job_id,
            "job_type": "Curation Report",
            "status": "error",
            "error": job["error"],
            "cancel_url": "/curation-report",
            "page": "curation-report",
        })

    # Still running — show loading page
    return templates.TemplateResponse("job_loading.html", {
        "request": request,
        "job_id": job_id,
        "job_type": "Curation Report",
        "status": "running",
        "error": None,
        "cancel_url": "/curation-report",
        "page": "curation-report",
        "params": job["params"],
    })


@app.get("/curation-report/job/{job_id}/status")
async def curation_report_job_status(job_id: str):
    """JSON status endpoint for polling. Used by the loading page."""
    job = _get_job(job_id)
    if not job:
        return JSONResponse({"status": "not_found"}, status_code=404)
    return JSONResponse({
        "status": job["status"],
        "error": job["error"],
        "redirect_url": f"/curation-report/{job['result']['run_id']}" if job["status"] == "done" else None,
    })




def _save_report_details(db, run_id: str, report: dict):
    """Save detailed customer and item results to the database."""
    logger.info(f"[CURATION SAVE] Saving details for run {run_id}")

    customer_rows = []
    item_rows = []

    for tri_key, tri_data in report.get("trimester_reports", {}).items():
        trimester = int(tri_key) if isinstance(tri_key, str) else tri_key

        # Customer results
        for cust in tri_data.get("customers", []):
            customer_rows.append({
                "run_id": run_id,
                "customer_id": cust["customer_id"],
                "projected_trimester": trimester,
                "needs_new_curation": cust["needs_new_curation"],
                "recommended_kit_id": cust.get("recommended_kit_id"),
                "recommended_kit_sku": cust.get("recommended_kit_sku"),
                "alternative_kit_skus": cust.get("alternative_kit_skus", []),
                "reason": cust.get("reason", "")[:500],
                "blocking_item_count": cust.get("blocking_item_count", 0),
            })

        # DO NOT USE items
        for item in tri_data.get("do_not_use", []):
            item_rows.append({
                "run_id": run_id,
                "trimester": trimester,
                "item_id": item["item_id"],
                "blocked_count": item["blocked_count"],
                "group_size": item["group_size"],
                "blocked_pct": item["blocked_pct"],
                "risk_level": item["risk_level"],
            })

        # CAN USE items (LOW risk)
        for item in tri_data.get("can_use", []):
            item_rows.append({
                "run_id": run_id,
                "trimester": trimester,
                "item_id": item["item_id"],
                "blocked_count": item["blocked_count"],
                "group_size": item["group_size"],
                "blocked_pct": item["blocked_pct"],
                "risk_level": item["risk_level"],
            })

    # Batch insert customers (chunks of 500)
    for i in range(0, len(customer_rows), 500):
        batch = customer_rows[i:i + 500]
        db.table("curation_run_customers").insert(batch).execute()
    logger.info(f"[CURATION SAVE] Saved {len(customer_rows)} customer records")

    # Batch insert items (chunks of 500)
    for i in range(0, len(item_rows), 500):
        batch = item_rows[i:i + 500]
        db.table("curation_run_items").insert(batch).execute()
    logger.info(f"[CURATION SAVE] Saved {len(item_rows)} item records")


@app.get("/curation-report/{run_id}", response_class=HTMLResponse)
async def view_curation_report(request: Request, run_id: str, msg: str = "", msg_type: str = ""):
    """View a specific curation report by run ID."""
    try:
        db = get_supabase()
        logger.info(f"[CURATION VIEW] Loading report: run_id={run_id}")

        # Load the run record
        run = db.table("curation_runs").select("*").eq("id", run_id).single().execute()
        if not run.data:
            return RedirectResponse("/curation-report?msg=Report+not+found&msg_type=error", status_code=303)

        run_data = run.data
        report_month = run_data["report_month"]

        # Load executive summary from summary_json
        executive = run_data.get("summary_json", {})
        if isinstance(executive, str):
            try:
                executive = json_module.loads(executive)
            except Exception:
                executive = {}

        # Load customer results
        cust_rows = []
        offset = 0
        while True:
            batch = db.table("curation_run_customers").select("*, customers(email, first_name, last_name, clothing_size, platform)").eq("run_id", run_id).range(offset, offset + 999).execute()
            cust_rows.extend(batch.data or [])
            if len(batch.data or []) < 1000:
                break
            offset += 1000

        # Load item results
        item_rows = []
        offset = 0
        while True:
            batch = db.table("curation_run_items").select("*, items(name, sku, category, unit_cost)").eq("run_id", run_id).range(offset, offset + 999).execute()
            item_rows.extend(batch.data or [])
            if len(batch.data or []) < 1000:
                break
            offset += 1000

        logger.info(f"[CURATION VIEW] Loaded {len(cust_rows)} customers, {len(item_rows)} items")

        # Load inventory status (kits with stock)
        all_kits = db.table("kits").select("*").eq("is_welcome_kit", False).gt("quantity_available", 0).order("age_rank").execute()
        kits_by_tri = {}
        for k in (all_kits.data or []):
            tri = k["trimester"]
            if tri not in kits_by_tri:
                kits_by_tri[tri] = []
            kits_by_tri[tri].append({
                "sku": k["sku"],
                "quantity_available": k.get("quantity_available", 0),
                "age_rank": k.get("age_rank", 0),
                "is_universal": k.get("is_universal", False),
                "size_variant": k.get("size_variant"),
            })

        # Welcome kits
        welcome_kits = db.table("kits").select("sku, trimester, quantity_available, age_rank").eq("is_welcome_kit", True).gt("quantity_available", 0).order("age_rank").execute()

        # Build report structure for the template
        trimester_reports = {}
        for tri in [1, 2, 3, 4]:
            tri_customers = []
            for cr in cust_rows:
                if cr["projected_trimester"] == tri:
                    cust_info = cr.get("customers", {}) or {}
                    tri_customers.append({
                        "customer_id": cr["customer_id"],
                        "email": cust_info.get("email", ""),
                        "first_name": cust_info.get("first_name", ""),
                        "last_name": cust_info.get("last_name", ""),
                        "clothing_size": cust_info.get("clothing_size"),
                        "platform": cust_info.get("platform", ""),
                        "projected_trimester": tri,
                        "needs_new_curation": cr["needs_new_curation"],
                        "recommended_kit_id": cr.get("recommended_kit_id"),
                        "recommended_kit_sku": cr.get("recommended_kit_sku"),
                        "alternative_kit_skus": cr.get("alternative_kit_skus", []),
                        "reason": cr.get("reason", ""),
                        "blocking_item_count": cr.get("blocking_item_count", 0),
                    })

            tri_do_not_use = []
            tri_can_use = []
            for ir in item_rows:
                if ir["trimester"] == tri:
                    item_info = ir.get("items", {}) or {}
                    entry = {
                        "item_id": ir["item_id"],
                        "name": item_info.get("name", "Unknown"),
                        "sku": item_info.get("sku", ""),
                        "category": item_info.get("category"),
                        "unit_cost": item_info.get("unit_cost"),
                        "blocked_count": ir["blocked_count"],
                        "group_size": ir["group_size"],
                        "blocked_pct": ir["blocked_pct"],
                        "risk_level": ir["risk_level"],
                    }
                    if ir["risk_level"] in ("HIGH", "MEDIUM"):
                        tri_do_not_use.append(entry)
                    else:
                        tri_can_use.append(entry)

            # Sort DO NOT USE by blocked_pct desc, CAN USE by blocked_pct asc
            tri_do_not_use.sort(key=lambda x: -x["blocked_pct"])
            tri_can_use.sort(key=lambda x: (x["blocked_pct"], x["name"]))

            covered = sum(1 for c in tri_customers if not c["needs_new_curation"])
            needs_new = sum(1 for c in tri_customers if c["needs_new_curation"])

            trimester_reports[tri] = {
                "customers": tri_customers,
                "covered_count": covered,
                "needs_new_count": needs_new,
                "do_not_use": tri_do_not_use,
                "can_use": tri_can_use,
                "build_qty": {
                    "projected_customers": len(tri_customers),
                    "covered_by_existing": covered,
                    "need_new_curation": needs_new,
                    "recommended_build_qty": executive.get("trimesters", {}).get(str(tri), {}).get("recommended_build_qty", 0) if executive else 0,
                    "expected_leftover": executive.get("trimesters", {}).get(str(tri), {}).get("expected_leftover", 0) if executive else 0,
                },
                "inventory_status": kits_by_tri.get(tri, []),
            }

        report = {
            "executive": executive,
            "trimester_reports": trimester_reports,
            "welcome_watchlist": {
                "total_stock": sum(k.get("quantity_available", 0) for k in (welcome_kits.data or [])),
                "kits": welcome_kits.data or [],
                "new_customer_count": executive.get("total_new_customers", 0),
            },
        }

        return templates.TemplateResponse("curation_report.html", {
            "request": request,
            "page": "curation-report",
            "msg": msg,
            "msg_type": msg_type,
            "report": report,
            "run_id": run_id,
            "previous_runs": [],
            "default_month": "",
        })
    except Exception as e:
        logger.error(f"[CURATION VIEW] Error loading report: {e}", exc_info=True)
        return RedirectResponse(f"/curation-report?msg=Error:+{quote(str(e)[:200])}&msg_type=error", status_code=303)


@app.post("/customers/add")
async def add_customer(
    request: Request,
    email: str = Form(...),
    first_name: str = Form(""),
    last_name: str = Form(""),
    due_date_str: str = Form(""),
    clothing_size: str = Form(""),
    baby_gender: str = Form(""),
    platform: str = Form("shopify"),
    subscription_status: str = Form("active"),
    wants_daddy_item: str = Form(""),
    phone: str = Form(""),
    address_line1: str = Form(""),
    city: str = Form(""),
    province: str = Form(""),
    zip_code: str = Form(""),
):
    """Manually add a customer, optionally with shipment history."""
    try:
        db = get_supabase()
        email_clean = email.strip().lower()
        daddy = wants_daddy_item.lower() in ("true", "on", "1", "yes") if wants_daddy_item else False
        logger.info(f"[CUSTOMER ADD] Adding: email={email_clean}, platform={platform}, daddy={daddy}")
        if not email_clean:
            logger.error("[CUSTOMER ADD] Empty email")
            return RedirectResponse("/customers", status_code=303)

        # Check if customer already exists
        existing = db.table("customers").select("id").ilike("email", email_clean).execute()
        if existing.data:
            logger.warning(f"[CUSTOMER ADD] Customer {email_clean} already exists — redirecting to edit")
            return RedirectResponse(f"/customers/{existing.data[0]['id']}", status_code=303)

        due_date = parse_due_date(due_date_str) if due_date_str.strip() else None
        trimester = calculate_trimester(due_date, date.today()) if due_date else None
        size = normalize_clothing_size(clothing_size) if clothing_size.strip() else None

        record = {
            "email": email_clean,
            "first_name": first_name.strip() or None,
            "last_name": last_name.strip() or None,
            "due_date": due_date.isoformat() if due_date else None,
            "trimester": trimester,
            "clothing_size": size,
            "baby_gender": baby_gender.strip() or None,
            "platform": platform,
            "subscription_status": subscription_status,
            "wants_daddy_item": daddy,
            "phone": phone.strip() or None,
            "address_line1": address_line1.strip() or None,
            "city": city.strip() or None,
            "province": province.strip() or None,
            "zip": zip_code.strip() or None,
        }
        result = db.table("customers").insert(record).execute()
        cust_id = result.data[0]["id"] if result.data else None
        logger.info(f"[CUSTOMER ADD] Created customer: {email_clean}, id={cust_id}, T{trimester}")

        # Process shipment history entries from form
        if cust_id:
            form_data = await request.form()
            ship_skus = form_data.getlist("ship_kit_sku")
            ship_dates = form_data.getlist("ship_date")
            ship_trimesters = form_data.getlist("ship_trimester")
            ship_platforms = form_data.getlist("ship_platform")
            ship_items = form_data.getlist("ship_items")
            logger.info(f"[CUSTOMER ADD] Shipment history entries: {len(ship_skus)}")

            for i, ship_sku in enumerate(ship_skus):
                ship_sku_clean = ship_sku.strip().upper() if ship_sku else ""
                if not ship_sku_clean:
                    continue
                try:
                    s_date = parse_due_date(ship_dates[i]) if i < len(ship_dates) and ship_dates[i].strip() else None
                    s_tri = int(ship_trimesters[i]) if i < len(ship_trimesters) and ship_trimesters[i].strip() else None
                    s_plat = ship_platforms[i] if i < len(ship_platforms) and ship_platforms[i].strip() else platform
                    s_items = ship_items[i] if i < len(ship_items) else ""

                    # Try to find kit by SKU
                    kit_id = None
                    kit_result = db.table("kits").select("id").eq("sku", ship_sku_clean).execute()
                    if kit_result.data:
                        kit_id = kit_result.data[0]["id"]

                    ship_result = db.table("shipments").insert({
                        "customer_id": cust_id,
                        "kit_id": kit_id,
                        "kit_sku": ship_sku_clean,
                        "ship_date": s_date.isoformat() if s_date else None,
                        "trimester_at_ship": s_tri if s_tri and s_tri > 0 else None,
                        "platform": s_plat,
                        "notes": "Added during customer creation",
                    }).execute()
                    shipment_id = ship_result.data[0]["id"] if ship_result.data else None

                    item_count, unresolved_refs = populate_shipment_items(shipment_id, kit_id, s_items)
                    if shipment_id and item_count == 0:
                        db.table("shipments").update({
                            "notes": "Added during customer creation | WARNING: no item history recorded"
                        }).eq("id", shipment_id).execute()
                        logger.warning(
                            f"[CUSTOMER ADD] Shipment #{i+1} for {email_clean} has no recorded items. "
                            f"Duplicate checking will be incomplete until items are added."
                        )
                    if unresolved_refs:
                        logger.warning(f"[CUSTOMER ADD] Shipment #{i+1} unresolved item refs: {unresolved_refs}")

                    logger.info(
                        f"[CUSTOMER ADD] Added shipment #{i+1}: {ship_sku_clean} for {email_clean}; "
                        f"recorded_items={item_count}"
                    )
                except Exception as ship_err:
                    logger.warning(f"[CUSTOMER ADD] Error adding shipment #{i+1}: {ship_err}")

        await log_activity("customer", f"Manually added customer: {email_clean}", f"T{trimester}, {platform}, {len(ship_skus) if cust_id else 0} shipments", "success")
        if cust_id:
            return RedirectResponse(f"/customers/{cust_id}", status_code=303)
    except Exception as e:
        logger.error(f"[CUSTOMER ADD] Error: {e}", exc_info=True)
        await log_activity("customer", f"Failed to add customer: {e}", "", "error")
    return RedirectResponse("/customers", status_code=303)


@app.post("/customers/{customer_id}/edit")
async def edit_customer(
    request: Request,
    customer_id: str,
    first_name: str = Form(""),
    last_name: str = Form(""),
    due_date_str: str = Form(""),
    clothing_size: str = Form(""),
    baby_gender: str = Form(""),
    platform: str = Form("shopify"),
    subscription_status: str = Form("active"),
    wants_daddy_item: str = Form(""),
):
    """Edit an existing customer's details."""
    try:
        db = get_supabase()
        daddy = wants_daddy_item.lower() in ("true", "on", "1", "yes") if wants_daddy_item else False
        due_date = parse_due_date(due_date_str) if due_date_str.strip() else None
        trimester = calculate_trimester(due_date, date.today()) if due_date else None
        size = normalize_clothing_size(clothing_size) if clothing_size.strip() else None

        record = {
            "first_name": first_name.strip() or None,
            "last_name": last_name.strip() or None,
            "due_date": due_date.isoformat() if due_date else None,
            "trimester": trimester,
            "clothing_size": size,
            "baby_gender": baby_gender.strip() or None,
            "platform": platform,
            "subscription_status": subscription_status,
            "wants_daddy_item": daddy,
        }
        db.table("customers").update(record).eq("id", customer_id).execute()
        logger.info(f"[CUSTOMER EDIT] Updated customer {customer_id}: T{trimester}, {platform}, daddy={daddy}")
        await log_activity("customer", f"Edited customer {customer_id}", f"T{trimester}, {platform}", "success")
    except Exception as e:
        logger.error(f"[CUSTOMER EDIT] Error: {e}", exc_info=True)
    return RedirectResponse(f"/customers/{customer_id}", status_code=303)


@app.post("/customers/{customer_id}/shipments/add")
async def add_shipment_history(
    request: Request,
    customer_id: str,
    kit_sku: str = Form(...),
    ship_date_str: str = Form(""),
    trimester_at_ship: int = Form(0),
    platform: str = Form("shopify"),
    order_id: str = Form(""),
    item_refs: str = Form(""),
    notes: str = Form(""),
):
    """Manually add a shipment to a customer's history (for historical data entry)."""
    try:
        db = get_supabase()
        ship_date = None
        if ship_date_str.strip():
            ship_date = parse_due_date(ship_date_str)

        # Try to find the kit by SKU to link kit_id
        kit_id = None
        kit_sku_clean = kit_sku.strip().upper()
        kit_result = db.table("kits").select("id").eq("sku", kit_sku_clean).execute()
        if kit_result.data:
            kit_id = kit_result.data[0]["id"]
            logger.info(f"[SHIPMENT ADD] Found kit '{kit_sku_clean}' in DB: {kit_id}")
        else:
            logger.info(f"[SHIPMENT ADD] Kit '{kit_sku_clean}' not in DB — storing SKU only")

        shipment_record = {
            "customer_id": customer_id,
            "kit_id": kit_id,
            "kit_sku": kit_sku_clean,
            "ship_date": ship_date.isoformat() if ship_date else None,
            "trimester_at_ship": trimester_at_ship if trimester_at_ship > 0 else None,
            "platform": platform if platform else None,
            "order_id": order_id.strip() or None,
            "notes": notes.strip() or None,
        }
        ship_result = db.table("shipments").insert(shipment_record).execute()
        shipment_id = ship_result.data[0]["id"] if ship_result.data else None

        item_count, unresolved_refs = populate_shipment_items(shipment_id, kit_id, item_refs)
        if shipment_id and item_count == 0:
            warning_note = (notes.strip() + " | WARNING: no item history recorded").strip(" |")
            db.table("shipments").update({"notes": warning_note}).eq("id", shipment_id).execute()
            logger.warning(
                f"[SHIPMENT ADD] Shipment {shipment_id} for customer {customer_id} has no recorded items. "
                f"Duplicate checking will be incomplete until items are added."
            )
        if unresolved_refs:
            logger.warning(f"[SHIPMENT ADD] Unresolved manual item refs for {kit_sku_clean}: {unresolved_refs}")

        # Get customer email for logging
        cust = db.table("customers").select("email").eq("id", customer_id).single().execute()
        cust_email = cust.data["email"] if cust.data else customer_id
        logger.info(
            f"[SHIPMENT ADD] Added shipment for {cust_email}: {kit_sku_clean}, date={ship_date}, "
            f"recorded_items={item_count}"
        )
        await log_activity(
            "shipment",
            f"Manual shipment added for {cust_email}",
            f"Kit: {kit_sku_clean}, Date: {ship_date}, Items recorded: {item_count}",
            "success"
        )
    except Exception as e:
        logger.error(f"[SHIPMENT ADD] Error: {e}", exc_info=True)
        await log_activity("shipment", f"Failed to add shipment: {e}", "", "error")
    return RedirectResponse(f"/customers/{customer_id}", status_code=303)


@app.post("/customers/{customer_id}/shipments/{shipment_id}/edit")
async def edit_shipment_history(
    request: Request,
    customer_id: str,
    shipment_id: str,
    kit_sku: str = Form(...),
    ship_date_str: str = Form(""),
    trimester_at_ship: int = Form(0),
    platform: str = Form("shopify"),
    order_id: str = Form(""),
    item_refs: str = Form(""),
    notes: str = Form(""),
):
    """Edit a manually recorded shipment and rebuild its item history."""
    try:
        db = get_supabase()
        existing = db.table("shipments").select("id, kit_sku").eq("id", shipment_id).eq("customer_id", customer_id).single().execute()
        if not existing.data:
            return RedirectResponse(
                f"/customers/{customer_id}?msg={quote('Shipment not found')}&msg_type=error",
                status_code=303,
            )

        ship_date = parse_due_date(ship_date_str) if ship_date_str.strip() else None
        kit_sku_clean = kit_sku.strip().upper()
        kit_id = None
        kit_result = db.table("kits").select("id").eq("sku", kit_sku_clean).execute()
        if kit_result.data:
            kit_id = kit_result.data[0]["id"]

        db.table("shipments").update({
            "kit_id": kit_id,
            "kit_sku": kit_sku_clean,
            "ship_date": ship_date.isoformat() if ship_date else None,
            "trimester_at_ship": trimester_at_ship if trimester_at_ship > 0 else None,
            "platform": platform if platform else None,
            "order_id": order_id.strip() or None,
            "notes": notes.strip() or None,
        }).eq("id", shipment_id).eq("customer_id", customer_id).execute()

        db.table("shipment_items").delete().eq("shipment_id", shipment_id).execute()
        item_count, unresolved_refs = populate_shipment_items(shipment_id, kit_id, item_refs)

        final_notes = notes.strip()
        if item_count == 0:
            final_notes = (final_notes + " | WARNING: no item history recorded").strip(" |")
        db.table("shipments").update({"notes": final_notes or None}).eq("id", shipment_id).execute()

        if unresolved_refs:
            logger.warning(f"[SHIPMENT EDIT] Unresolved manual item refs for shipment {shipment_id}: {unresolved_refs}")

        logger.info(
            f"[SHIPMENT EDIT] Updated shipment {shipment_id} for customer {customer_id}: "
            f"kit={kit_sku_clean}, recorded_items={item_count}"
        )
        await log_activity(
            "shipment",
            f"Edited shipment {shipment_id[:8]}",
            f"Kit: {kit_sku_clean}, Items recorded: {item_count}",
            "success",
        )

        if unresolved_refs:
            msg = f"Shipment updated, but some items were not matched: {', '.join(unresolved_refs[:3])}"
            msg_type = "error"
        elif item_count == 0:
            msg = "Shipment updated, but no items were recorded. Duplicate checking will be incomplete."
            msg_type = "error"
        else:
            msg = f"Shipment updated. {item_count} item(s) recorded."
            msg_type = "success"

        return RedirectResponse(
            f"/customers/{customer_id}?msg={quote(msg)}&msg_type={msg_type}",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[SHIPMENT EDIT] Error: {e}", exc_info=True)
        await log_activity("shipment", f"Failed to edit shipment: {e}", "", "error")
        return RedirectResponse(
            f"/customers/{customer_id}?msg={quote('Failed to edit shipment')}&msg_type=error",
            status_code=303,
        )


@app.post("/customers/{customer_id}/shipments/{shipment_id}/remove")
async def remove_shipment_history(request: Request, customer_id: str, shipment_id: str):
    """Remove a shipment and its recorded shipment_items from customer history."""
    try:
        db = get_supabase()
        shipment = db.table("shipments").select("kit_sku").eq("id", shipment_id).eq("customer_id", customer_id).single().execute()
        if not shipment.data:
            return RedirectResponse(
                f"/customers/{customer_id}?msg={quote('Shipment not found')}&msg_type=error",
                status_code=303,
            )

        kit_sku = shipment.data.get("kit_sku") or shipment_id[:8]
        db.table("shipment_items").delete().eq("shipment_id", shipment_id).execute()
        db.table("shipments").delete().eq("id", shipment_id).eq("customer_id", customer_id).execute()

        logger.info(f"[SHIPMENT REMOVE] Removed shipment {shipment_id} (kit={kit_sku}) from customer {customer_id}")
        await log_activity("shipment", f"Removed shipment {shipment_id[:8]}", f"Kit: {kit_sku}", "info")
        return RedirectResponse(
            f"/customers/{customer_id}?msg={quote(f'Removed shipment {kit_sku}')}&msg_type=success",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[SHIPMENT REMOVE] Error: {e}", exc_info=True)
        await log_activity("shipment", f"Failed to remove shipment: {e}", "", "error")
        return RedirectResponse(
            f"/customers/{customer_id}?msg={quote('Failed to remove shipment')}&msg_type=error",
            status_code=303,
        )


@app.post("/customers/{customer_id}/remove")
async def remove_customer(request: Request, customer_id: str):
    """Delete a customer and all dependent history via DB cascade."""
    try:
        db = get_supabase()
        customer = db.table("customers").select("email, first_name, last_name").eq("id", customer_id).single().execute()
        if not customer.data:
            return RedirectResponse(f"/customers?msg={quote('Customer not found')}&msg_type=error", status_code=303)

        customer_name = f"{customer.data.get('first_name', '')} {customer.data.get('last_name', '')}".strip() or customer.data.get("email")
        db.table("customers").delete().eq("id", customer_id).execute()
        logger.info(f"[CUSTOMER REMOVE] Deleted customer {customer_id} ({customer_name})")
        await log_activity("customer", f"Removed customer {customer_name}", customer.data.get("email", ""), "warning")
        return RedirectResponse(
            f"/customers?msg={quote(f'Removed customer {customer_name}')}&msg_type=success",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[CUSTOMER REMOVE] Error: {e}", exc_info=True)
        await log_activity("customer", f"Failed to remove customer: {e}", "", "error")
        return RedirectResponse(f"/customers?msg={quote('Failed to remove customer')}&msg_type=error", status_code=303)


@app.post("/items/{item_id}/remove")
async def remove_item(request: Request, item_id: str):
    """Delete an item only if it has never been used in shipment history."""
    try:
        db = get_supabase()
        item = db.table("items").select("name, sku").eq("id", item_id).single().execute()
        if not item.data:
            return RedirectResponse(f"/items?msg={quote('Item not found')}&msg_type=error", status_code=303)

        item_name = item.data.get("name") or item.data.get("sku") or item_id[:8]
        shipment_refs = db.table("shipment_items").select("shipment_id").eq("item_id", item_id).limit(1).execute()
        if shipment_refs.data:
            return RedirectResponse(
                f"/items?msg={quote('Cannot delete an item that exists in shipment history')}&msg_type=error",
                status_code=303,
            )

        db.table("items").delete().eq("id", item_id).execute()
        logger.info(f"[ITEM REMOVE] Deleted item {item_id} ({item_name})")
        await log_activity("item", f"Removed item {item_name}", item.data.get("sku", ""), "warning")
        return RedirectResponse(
            f"/items?msg={quote(f'Removed item {item_name}')}&msg_type=success",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[ITEM REMOVE] Error: {e}", exc_info=True)
        await log_activity("item", f"Failed to remove item: {e}", "", "error")
        return RedirectResponse(f"/items?msg={quote('Failed to remove item')}&msg_type=error", status_code=303)


@app.post("/kits/{kit_id}/remove")
async def remove_kit(request: Request, kit_id: str):
    """Delete a kit only if it is not already used in history or decisions."""
    try:
        db = get_supabase()
        kit = db.table("kits").select("sku").eq("id", kit_id).single().execute()
        if not kit.data:
            return RedirectResponse(f"/kits?msg={quote('Kit not found')}&msg_type=error", status_code=303)

        kit_sku = kit.data.get("sku") or kit_id[:8]
        shipment_refs = db.table("shipments").select("id").eq("kit_id", kit_id).limit(1).execute()
        decision_refs = db.table("decisions").select("id").eq("kit_id", kit_id).limit(1).execute()
        if shipment_refs.data or decision_refs.data:
            return RedirectResponse(
                f"/kits?msg={quote('Cannot delete a kit that is already used in shipments or decisions')}&msg_type=error",
                status_code=303,
            )

        db.table("kits").delete().eq("id", kit_id).execute()
        logger.info(f"[KIT REMOVE] Deleted kit {kit_id} ({kit_sku})")
        await log_activity("kit", f"Removed kit {kit_sku}", "", "warning")
        return RedirectResponse(
            f"/kits?msg={quote(f'Removed kit {kit_sku}')}&msg_type=success",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[KIT REMOVE] Error: {e}", exc_info=True)
        await log_activity("kit", f"Failed to remove kit: {e}", "", "error")
        return RedirectResponse(f"/kits?msg={quote('Failed to remove kit')}&msg_type=error", status_code=303)


@app.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request):
    """View activity log."""
    try:
        db = get_supabase()
        logs = db.table("activity_log").select("*").order("created_at", desc=True).limit(100).execute()
        return templates.TemplateResponse("activity.html", {
            "request": request,
            "activities": logs.data or [],
            "page": "activity",
        })
    except Exception as e:
        logger.error(f"[ACTIVITY PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("activity.html", {
            "request": request,
            "activities": [],
            "error": str(e),
            "page": "activity",
        })


@app.get("/flow-diagram", response_class=HTMLResponse)
async def flow_diagram(request: Request):
    """Visual Mermaid.js diagram of the full application flow."""
    return templates.TemplateResponse("flow_diagram.html", {"request": request})


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """Settings and integration status."""
    shopify_connected = bool(SHOPIFY_WEBHOOK_SECRET)
    cratejoy_connected = bool(CRATEJOY_CLIENT_ID and CRATEJOY_CLIENT_SECRET)
    supabase_connected = bool(SUPABASE_URL and SUPABASE_KEY)

    # Check if Cratejoy webhooks are actually registered
    cratejoy_hooks_registered = False
    cratejoy_hook_count = 0
    if cratejoy_connected:
        try:
            auth_str = base64.b64encode(f"{CRATEJOY_CLIENT_ID}:{CRATEJOY_CLIENT_SECRET}".encode()).decode()
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get("https://api.cratejoy.com/v1/hooks/", headers={
                    "Authorization": f"Basic {auth_str}",
                    "Accept": "application/json",
                })
                if resp.status_code == 200:
                    hooks = resp.json().get("results", [])
                    target = f"{BASE_URL}/webhooks/cratejoy/order"
                    our_hooks = [h for h in hooks if h.get("target") == target and h.get("enabled")]
                    cratejoy_hook_count = len(our_hooks)
                    cratejoy_hooks_registered = cratejoy_hook_count >= 4
                    logger.info(f"[SETTINGS] Cratejoy hooks: {cratejoy_hook_count}/4 registered")
        except Exception as e:
            logger.error(f"[SETTINGS] Error checking Cratejoy hooks: {e}")

    # Check DB connectivity
    db_connected = False
    migration_run = False
    try:
        db = get_supabase()
        db.table("activity_log").select("id").limit(1).execute()
        db_connected = True
        migration_run = True
    except Exception as e:
        logger.error(f"[SETTINGS] DB check failed: {e}")

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "shopify_connected": shopify_connected,
        "cratejoy_connected": cratejoy_connected,
        "supabase_connected": supabase_connected,
        "db_connected": db_connected,
        "migration_run": migration_run,
        "cratejoy_hooks_registered": cratejoy_hooks_registered,
        "cratejoy_hook_count": cratejoy_hook_count,
        "shopify_domain": SHOPIFY_STORE_DOMAIN,
        "base_url": BASE_URL,
        "webhook_url_shopify": f"{BASE_URL}/webhooks/shopify/orders/create",
        "webhook_url_cratejoy": f"{BASE_URL}/webhooks/cratejoy/order",
        "page": "settings",
    })


# ─── Health check ───
# ─── Bulk recalculate trimesters for all customers ───
def _do_recalculate_trimesters():
    """Background worker — runs after the 202 response is sent to avoid H12 timeout."""
    try:
        db = get_supabase()
        today = date.today()

        # Paginate to bypass Supabase's 1000-row default max
        all_customers = []
        page_size = 1000
        offset = 0
        while True:
            batch = (
                db.table("customers")
                .select("id, due_date, trimester")
                .not_.is_("due_date", "null")
                .range(offset, offset + page_size - 1)
                .execute()
            ).data or []
            all_customers.extend(batch)
            logger.info(f"[RECALC TRIMESTER] Fetched page offset={offset}: {len(batch)} rows")
            if len(batch) < page_size:
                break
            offset += page_size

        customers = all_customers
        updated = 0
        skipped = 0
        errors = 0
        logger.info(f"[RECALC TRIMESTER] Starting bulk recalculation for {len(customers)} customers")
        for c in customers:
            try:
                due_dt = date.fromisoformat(str(c["due_date"]))
                new_t = calculate_trimester(due_dt, today)
                if new_t != c.get("trimester"):
                    db.table("customers").update({"trimester": new_t}).eq("id", c["id"]).execute()
                    logger.info(f"[RECALC TRIMESTER] {c['id']}: T{c.get('trimester')} → T{new_t}")
                    updated += 1
                else:
                    skipped += 1
            except Exception as row_err:
                logger.error(f"[RECALC TRIMESTER] Error on customer {c.get('id')}: {row_err}")
                errors += 1
        logger.info(f"[RECALC TRIMESTER] Done. updated={updated}, skipped={skipped}, errors={errors}")
        db.table("activity_log").insert({
            "type": "admin",
            "summary": f"Bulk trimester recalculation: {updated} updated",
            "detail": f"updated={updated}, skipped={skipped}, errors={errors}",
            "result": "success" if errors == 0 else "warning",
        }).execute()
    except Exception as e:
        logger.error(f"[RECALC TRIMESTER] Fatal error: {e}", exc_info=True)


@app.post("/api/recalculate-all-trimesters")
async def api_recalculate_all_trimesters(background_tasks: BackgroundTasks):
    """
    Kick off bulk trimester recalculation in the background.
    Returns 202 immediately so Heroku doesn't H12-timeout on 900+ customers.
    Check Activity log for results when done.
    """
    background_tasks.add_task(_do_recalculate_trimesters)
    logger.info("[RECALC TRIMESTER] Job queued in background")
    return JSONResponse({"status": "ok", "message": "Running in background — check Activity log for results in ~30 seconds."}, status_code=202)


@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        db = get_supabase()
        db.table("activity_log").select("id").limit(1).execute()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}


# ─── Fix Google Sheets headers ───
@app.post("/api/fix-gsheet-headers")
async def api_fix_gsheet_headers():
    """Update Google Sheet header row to match current column format (12 columns)."""
    result = fix_gsheet_headers()
    if result:
        await log_activity("gsheets", "Fixed Google Sheet headers", "Updated to 12-column format", "success")
        return JSONResponse({"status": "ok", "message": "Headers updated successfully"})
    return JSONResponse({"status": "error", "message": "Failed to update headers — check logs"}, status_code=500)


# ─── API: Backfill age ranks from SKU ───
@app.post("/api/backfill-age-ranks")
async def api_backfill_age_ranks():
    """
    Compute age_rank from SKU for every kit that is NOT manually overridden.
    Safe to re-run — only skips kits where age_rank_source='manual' (user explicitly set).
    All other kits (auto or unknown) get recomputed.
    """
    try:
        db = get_supabase()
        kits_result = db.table("kits").select("id, sku, age_rank, age_rank_source").execute()
        all_kits = kits_result.data or []
        logger.info(f"[BACKFILL] Starting age_rank backfill for {len(all_kits)} total kits")

        updated = []
        skipped_manual = []
        skipped_unresolved = []

        for kit in all_kits:
            kit_id = kit["id"]
            sku = kit["sku"]
            source = kit.get("age_rank_source") or "auto"

            # Only skip kits the user has manually locked
            if source == "manual":
                skipped_manual.append(sku)
                logger.info(f"[BACKFILL] Skipping {sku} — age_rank_source=manual (user override preserved)")
                continue

            computed = compute_age_rank_from_sku(sku)
            if computed == 0:
                skipped_unresolved.append(sku)
                logger.warning(f"[BACKFILL] Could not compute age_rank for SKU '{sku}' — leaving as-is")
                continue

            db.table("kits").update({"age_rank": computed, "age_rank_source": "auto"}).eq("id", kit_id).execute()
            updated.append({"sku": sku, "age_rank": computed})
            logger.info(f"[BACKFILL] ✅ {sku} → age_rank={computed} (source=auto)")

        logger.info(
            f"[BACKFILL] Done. Updated={len(updated)}, "
            f"SkippedManual={len(skipped_manual)}, Unresolved={len(skipped_unresolved)}"
        )
        await log_activity(
            "kit",
            f"Age rank backfill: {len(updated)} updated",
            f"Skipped manual overrides: {len(skipped_manual)}, Unresolved: {len(skipped_unresolved)}",
            "success",
        )
        return JSONResponse({
            "status": "ok",
            "updated": len(updated),
            "skipped_manual_overrides": len(skipped_manual),
            "unresolved": len(skipped_unresolved),
            "details": updated,
            "unresolved_skus": skipped_unresolved,
        })
    except Exception as e:
        logger.error(f"[BACKFILL] Error during age_rank backfill: {e}", exc_info=True)
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ─── API: Test webhook (manual trigger for testing) ───
@app.post("/api/test-webhook")
async def test_webhook(request: Request):
    """
    Send a test payload through the full Shopify processing pipeline.
    Accepts optional JSON body with custom fields:
      { email, first_name, last_name, due_date, size, expecting, second_parent }
    """
    # Accept custom fields from JSON body
    custom = {}
    try:
        body = await request.body()
        if body:
            custom = json.loads(body)
    except Exception:
        pass

    test_email = custom.get("email", "test@example.com")
    test_first = custom.get("first_name", "Test")
    test_last = custom.get("last_name", "Customer")
    test_due = custom.get("due_date", "2026-07-15")
    test_size = custom.get("size", "med")
    test_expecting = custom.get("expecting", "girl")
    test_second_parent = custom.get("second_parent", "no")
    test_platform = custom.get("platform", "shopify")

    test_order_id = f"test_{int(time.time())}"
    test_payload = {
        "id": test_order_id,
        "email": test_email,
        "total_price": "44.99",
        "financial_status": "paid",
        "source_name": "web",
        "customer": {
            "id": 999999,
            "email": test_email,
            "first_name": test_first,
            "last_name": test_last,
        },
        "shipping_address": {
            "address1": "123 Test St",
            "city": "Test City",
            "province": "CA",
            "zip": "90210",
            "country_code": "US",
        },
        "line_items": [{
            "title": "OBB Subscription Box",
            "sku": "OBB-SUBPLAN-1",
            "properties": [],
        }],
        "note_attributes": [
            {"name": "q_due_date", "value": test_due},
            {"name": "q_size", "value": test_size},
            {"name": "q_expecting", "value": test_expecting},
            {"name": "q_second_parent", "value": test_second_parent},
            {"name": "q_past_experience", "value": "no"},
        ],
    }

    logger.info(f"[TEST WEBHOOK] Starting test with order_id: {test_order_id}")

    try:
        db = get_supabase()

        # Log the webhook
        webhook_log = db.table("webhook_logs").insert({
            "source": "shopify",
            "event_type": "orders/create",
            "event_id": test_order_id,
            "payload": test_payload,
            "headers": {"X-Shopify-Topic": "orders/create", "test": True},
            "status": "received",
        }).execute()
        webhook_log_id = webhook_log.data[0]["id"] if webhook_log.data else None

        # Extract quiz data
        quiz = extract_quiz_data(test_payload["note_attributes"], test_payload["line_items"])
        due_date = parse_due_date(quiz["due_date_str"])
        trimester = calculate_trimester(due_date, date.today()) if due_date else None

        # Upsert customer
        existing = db.table("customers").select("*").ilike("email", test_email).execute()
        customer_record = {
            "email": test_email,
            "first_name": test_first,
            "last_name": test_last,
            "shopify_customer_id": "999999" if test_platform == "shopify" else None,
            "cratejoy_customer_id": "999999" if test_platform == "cratejoy" else None,
            "platform": test_platform,
            "subscription_status": "active",
            "due_date": due_date.isoformat() if due_date else None,
            "trimester": trimester,
            "clothing_size": quiz["clothing_size"],
            "baby_gender": quiz["baby_gender"],
            "wants_daddy_item": quiz["wants_daddy"],
        }
        if existing.data:
            cust_id = existing.data[0]["id"]
            db.table("customers").update(customer_record).eq("id", cust_id).execute()
        else:
            result = db.table("customers").insert(customer_record).execute()
            cust_id = result.data[0]["id"] if result.data else None

        # Run decision engine
        kit_decision = await assign_kit(cust_id, date.today()) if cust_id else {
            "decision_type": "incomplete-data", "reason": "Failed to create test customer",
            "kit_id": None, "kit_sku": None,
        }

        # Save decision
        if cust_id:
            db.table("decisions").insert({
                "customer_id": cust_id,
                "kit_id": kit_decision.get("kit_id"),
                "kit_sku": kit_decision.get("kit_sku"),
                "decision_type": kit_decision["decision_type"],
                "reason": kit_decision["reason"],
                "status": "pending",
                "order_id": test_order_id,
                "platform": test_platform,
                "trimester": trimester,
                "ship_date": date.today().isoformat(),
            }).execute()

            # ─── Write to Google Sheets ───
            write_decision_to_sheet({
                "date": date.today().isoformat(),
                "customer_name": f"{test_first} {test_last}".strip(),
                "email": test_email,
                "platform": f"test-{test_platform}",
                "trimester": trimester,
                "order_type": "test",
                "kit_sku": kit_decision.get("kit_sku", "—"),
                "decision_type": kit_decision["decision_type"],
                "reason": kit_decision["reason"],
                "order_id": test_order_id,
                "due_date": due_date.isoformat() if due_date else "",
                "clothing_size": quiz.get("clothing_size", "") or "",
            })

        # Update webhook log
        if webhook_log_id:
            db.table("webhook_logs").update({"status": "processed"}).eq("id", webhook_log_id).execute()

        await log_activity("test", f"Test webhook processed: {kit_decision['decision_type']}", 
                          f"Kit: {kit_decision.get('kit_sku', 'none')}, Trimester: T{trimester}", "info")

        logger.info(f"[TEST WEBHOOK] Complete. Decision: {kit_decision['decision_type']}, Kit: {kit_decision.get('kit_sku', 'none')}")
        return JSONResponse({
            "status": "ok",
            "decision": kit_decision["decision_type"],
            "kit_assigned": kit_decision.get("kit_sku"),
            "trimester": trimester,
            "reason": kit_decision["reason"],
        })
    except Exception as e:
        logger.error(f"[TEST WEBHOOK] Error: {e}", exc_info=True)
        await log_activity("test", f"Test webhook failed: {e}", "", "error")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ═══════════════════════════════════════════════════════════
# KIT DETAIL & KIT-ITEM MANAGEMENT
# ═══════════════════════════════════════════════════════════

@app.get("/kits/{kit_id}", response_class=HTMLResponse)
async def kit_detail(request: Request, kit_id: str):
    """View a kit's details and manage its items."""
    try:
        db = get_supabase()
        kit = db.table("kits").select("*").eq("id", kit_id).single().execute()
        if not kit.data:
            return HTMLResponse("Kit not found", status_code=404)

        # Get items linked to this kit
        kit_items = db.table("kit_items").select("*, items(*)").eq("kit_id", kit_id).execute()
        linked_items = kit_items.data or []
        logger.info(f"[KIT DETAIL] Kit {kit.data['sku']}: {len(linked_items)} items linked")

        # Get all items for the add dropdown (exclude already linked)
        linked_item_ids = {ki["item_id"] for ki in linked_items}
        all_items = db.table("items").select("*").order("name").execute()
        available_items = [i for i in (all_items.data or []) if i["id"] not in linked_item_ids]

        # Read optional flash messages from query params
        msg = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")

        return templates.TemplateResponse("kit_detail.html", {
            "request": request,
            "kit": kit.data,
            "kit_items": linked_items,
            "available_items": available_items,
            "msg": msg,
            "msg_type": msg_type,
            "page": "kits",
        })
    except Exception as e:
        logger.error(f"[KIT DETAIL] Error: {e}", exc_info=True)
        return HTMLResponse(f"Error loading kit: {e}", status_code=500)


@app.post("/kits/{kit_id}/items/add")
async def add_item_to_kit(
    request: Request,
    kit_id: str,
    item_id: str = Form(...),
    quantity: int = Form(1),
):
    """Link an item to a kit."""
    from urllib.parse import quote
    try:
        db = get_supabase()
        logger.info(f"[KIT-ITEM] Attempting to add item_id={item_id} to kit_id={kit_id} qty={quantity}")

        if not item_id or item_id.strip() == "":
            logger.warning(f"[KIT-ITEM] No item selected")
            return RedirectResponse(f"/kits/{kit_id}?msg={quote('Please select an item')}&msg_type=error", status_code=303)

        # Check if already linked
        existing = db.table("kit_items").select("item_id").eq("kit_id", kit_id).eq("item_id", item_id).execute()
        if existing.data:
            logger.info(f"[KIT-ITEM] Item {item_id} already linked to kit {kit_id}")
            return RedirectResponse(f"/kits/{kit_id}?msg={quote('Item already in this kit')}&msg_type=error", status_code=303)

        db.table("kit_items").insert({
            "kit_id": kit_id,
            "item_id": item_id,
            "quantity": quantity,
        }).execute()

        # Get names for logging
        kit = db.table("kits").select("sku").eq("id", kit_id).single().execute()
        item = db.table("items").select("name").eq("id", item_id).single().execute()
        kit_sku = kit.data["sku"] if kit.data else kit_id
        item_name = item.data["name"] if item.data else item_id
        logger.info(f"[KIT-ITEM] ✅ Added item '{item_name}' to kit {kit_sku} (qty: {quantity})")
        await log_activity("kit", f"Added item '{item_name}' to kit {kit_sku}", f"Qty: {quantity}", "success")
        return RedirectResponse(f"/kits/{kit_id}?msg={quote(f'Added {item_name} to kit')}&msg_type=success", status_code=303)
    except Exception as e:
        logger.error(f"[KIT-ITEM] Error adding item to kit: {e}", exc_info=True)
        return RedirectResponse(f"/kits/{kit_id}?msg={quote(f'Error adding item: {str(e)[:80]}')}&msg_type=error", status_code=303)


@app.post("/kits/{kit_id}/items/{item_id}/remove")
async def remove_item_from_kit(request: Request, kit_id: str, item_id: str):
    """Unlink an item from a kit."""
    try:
        db = get_supabase()
        db.table("kit_items").delete().eq("kit_id", kit_id).eq("item_id", item_id).execute()

        kit = db.table("kits").select("sku").eq("id", kit_id).single().execute()
        item = db.table("items").select("name").eq("id", item_id).single().execute()
        kit_sku = kit.data["sku"] if kit.data else kit_id
        item_name = item.data["name"] if item.data else item_id
        logger.info(f"[KIT-ITEM] Removed item '{item_name}' from kit {kit_sku}")
        await log_activity("kit", f"Removed item '{item_name}' from kit {kit_sku}", "", "info")
    except Exception as e:
        logger.error(f"[KIT-ITEM] Error removing item from kit: {e}", exc_info=True)
    return RedirectResponse(f"/kits/{kit_id}", status_code=303)


@app.post("/kits/{kit_id}/items/quick-add")
async def quick_add_item_to_kit(
    request: Request,
    kit_id: str,
    name: str = Form(...),
    sku: str = Form(""),
    category: str = Form(""),
    unit_cost: float = Form(0),
    is_therabox: str = Form(""),
    quantity: int = Form(1),
):
    """Create a new item AND link it to this kit in one step."""
    from urllib.parse import quote
    try:
        db = get_supabase()
        therabox = is_therabox.lower() in ("true", "on", "1", "yes") if is_therabox else False
        name_clean = name.strip()
        sku_clean = sku.strip().upper() or None
        if not name_clean:
            return RedirectResponse(f"/kits/{kit_id}?msg={quote('Item name is required')}&msg_type=error", status_code=303)

        logger.info(f"[KIT-ITEM QUICK] Creating item '{name_clean}' and linking to kit {kit_id}")

        # Create the item
        item_result = db.table("items").insert({
            "name": name_clean,
            "sku": sku_clean,
            "category": category.strip() or None,
            "unit_cost": unit_cost if unit_cost > 0 else None,
            "is_therabox": therabox,
        }).execute()
        item_id = item_result.data[0]["id"] if item_result.data else None
        if not item_id:
            return RedirectResponse(f"/kits/{kit_id}?msg={quote('Failed to create item')}&msg_type=error", status_code=303)

        # Link to kit
        db.table("kit_items").insert({
            "kit_id": kit_id,
            "item_id": item_id,
            "quantity": quantity,
        }).execute()

        kit = db.table("kits").select("sku").eq("id", kit_id).single().execute()
        kit_sku = kit.data["sku"] if kit.data else kit_id
        logger.info(f"[KIT-ITEM QUICK] ✅ Created '{name_clean}' and added to kit {kit_sku}")
        await log_activity("kit", f"Quick-added item '{name_clean}' to kit {kit_sku}", f"SKU: {sku_clean}, Qty: {quantity}", "success")
        return RedirectResponse(f"/kits/{kit_id}?msg={quote(f'Created and added {name_clean}')}&msg_type=success", status_code=303)
    except Exception as e:
        logger.error(f"[KIT-ITEM QUICK] Error: {e}", exc_info=True)
        return RedirectResponse(f"/kits/{kit_id}?msg={quote(f'Error: {str(e)[:80]}')}&msg_type=error", status_code=303)


# ═══════════════════════════════════════════════════════════
# CRATEJOY TEST WEBHOOK
# ═══════════════════════════════════════════════════════════

@app.post("/api/test-webhook-cratejoy")
async def test_webhook_cratejoy(request: Request):
    """
    Send a test payload through the Cratejoy processing pipeline.
    Accepts optional JSON body with custom fields.
    """
    custom = {}
    try:
        body = await request.body()
        if body:
            custom = json.loads(body)
    except Exception:
        pass

    test_email = custom.get("email", "cjtest@example.com")
    test_first = custom.get("first_name", "CJ")
    test_last = custom.get("last_name", "Tester")
    test_due = custom.get("due_date", "2026-07-15")
    test_size = custom.get("size", "med")

    cj_order_id = f"cj_test_{int(time.time())}"
    logger.info(f"[CJ TEST] Starting Cratejoy test: {test_email}, due={test_due}")

    try:
        db = get_supabase()

        # Log as webhook
        webhook_log = db.table("webhook_logs").insert({
            "source": "cratejoy",
            "event_type": "order_new",
            "event_id": cj_order_id,
            "payload": {"test": True, "email": test_email, "due_date": test_due},
            "headers": {"test": True},
            "status": "received",
        }).execute()
        webhook_log_id = webhook_log.data[0]["id"] if webhook_log.data else None

        # Parse due date & trimester
        due_date = parse_due_date(test_due)
        trimester = calculate_trimester(due_date, date.today()) if due_date else None
        clothing_size = normalize_clothing_size(test_size)

        # Upsert customer
        existing = db.table("customers").select("*").ilike("email", test_email).execute()
        customer_record = {
            "email": test_email,
            "first_name": test_first,
            "last_name": test_last,
            "cratejoy_customer_id": "999999",
            "platform": "cratejoy",
            "subscription_status": "active",
            "due_date": due_date.isoformat() if due_date else None,
            "trimester": trimester,
            "clothing_size": clothing_size,
        }

        if existing.data:
            cust_id = existing.data[0]["id"]
            if existing.data[0].get("shopify_customer_id"):
                customer_record["platform"] = "both"
            db.table("customers").update(customer_record).eq("id", cust_id).execute()
            logger.info(f"[CJ TEST] Updated customer: {cust_id}")
        else:
            result = db.table("customers").insert(customer_record).execute()
            cust_id = result.data[0]["id"] if result.data else None
            logger.info(f"[CJ TEST] Created customer: {cust_id}")

        # Run decision engine
        kit_decision = await assign_kit(cust_id, date.today()) if cust_id else {
            "decision_type": "incomplete-data", "reason": "Failed to create test customer",
            "kit_id": None, "kit_sku": None,
        }

        # Save decision
        if cust_id:
            db.table("decisions").insert({
                "customer_id": cust_id,
                "kit_id": kit_decision.get("kit_id"),
                "kit_sku": kit_decision.get("kit_sku"),
                "decision_type": kit_decision["decision_type"],
                "reason": kit_decision["reason"],
                "status": "pending",
                "order_id": cj_order_id,
                "platform": "cratejoy",
                "trimester": trimester,
                "ship_date": date.today().isoformat(),
            }).execute()

            # Write to Google Sheets
            write_decision_to_sheet({
                "date": date.today().isoformat(),
                "customer_name": f"{test_first} {test_last}".strip(),
                "email": test_email,
                "platform": "test-cratejoy",
                "trimester": trimester,
                "order_type": "test",
                "kit_sku": kit_decision.get("kit_sku", "—"),
                "decision_type": kit_decision["decision_type"],
                "reason": kit_decision["reason"],
                "order_id": cj_order_id,
                "due_date": due_date.isoformat() if due_date else "",
                "clothing_size": clothing_size or "",
            })

        if webhook_log_id:
            db.table("webhook_logs").update({"status": "processed"}).eq("id", webhook_log_id).execute()

        await log_activity("test", f"Cratejoy test processed: {kit_decision['decision_type']}",
                          f"Kit: {kit_decision.get('kit_sku', 'none')}, T{trimester}", "info")

        logger.info(f"[CJ TEST] Complete. Decision: {kit_decision['decision_type']}, Kit: {kit_decision.get('kit_sku', 'none')}")
        return JSONResponse({
            "status": "ok",
            "decision": kit_decision["decision_type"],
            "kit_assigned": kit_decision.get("kit_sku"),
            "trimester": trimester,
            "reason": kit_decision["reason"],
            "platform": "cratejoy",
        })
    except Exception as e:
        logger.error(f"[CJ TEST] Error: {e}", exc_info=True)
        await log_activity("test", f"Cratejoy test failed: {e}", "", "error")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ═══════════════════════════════════════════════════════════
# DECISION APPROVAL / REJECT / SHIP
# ═══════════════════════════════════════════════════════════

# ─── VeraCore submission helper (Phase 3) ───────────────────

def _build_ship_to_from_customer(c: dict) -> dict:
    """Extract a VeraCore-shaped ship_to block from a customers row."""
    from veracore_client import normalize_country
    name = f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".strip() or c.get("email", "")
    return {
        "name":     name,
        "address1": c.get("address_line1", "") or "",
        "address2": c.get("address_line2", "") or "",
        "city":     c.get("city", "") or "",
        "state":    c.get("province", "") or "",
        "zip":      c.get("zip") or c.get("zip_code", "") or "",
        "country":  normalize_country(c.get("country")),
        "phone":    c.get("phone", "") or "",
    }


def submit_to_veracore(decision_id: str) -> dict:
    """
    Push an approved decision to VeraCore as a warehouse order.

    Guarantees:
      - No-op when VERACORE_BASE_URL is empty → CSV fallback picks it up later.
      - Idempotent: if decisions.veracore_order_id is already set, we skip.
      - Always writes a row to veracore_sync_log (success OR failure).
      - Never raises — callers can trust a return dict.

    Returns: {status: 'skipped'|'noop'|'submitted'|'failed'|'already_submitted',
              order_id: str|None, error: str|None}
    """
    db = get_supabase()

    # Fetch decision + customer + kit in one round-trip (Supabase embed).
    try:
        d_res = db.table("decisions").select(
            "*, customers(*), kits(id, sku, veracore_sku, cost_per_kit, name)"
        ).eq("id", decision_id).single().execute()
    except Exception as e:
        logger.error("[VERACORE SUBMIT] Could not load decision %s: %s", decision_id, e)
        return {"status": "failed", "order_id": None, "error": f"load failed: {e}"}

    d = d_res.data
    if not d:
        logger.warning("[VERACORE SUBMIT] Decision %s not found", decision_id)
        return {"status": "failed", "order_id": None, "error": "decision not found"}

    # ── Short-circuits ──────────────────────────────────────
    if d.get("veracore_order_id"):
        logger.info("[VERACORE SUBMIT] %s already has veracore_order_id=%s — skipping (idempotent)",
                    decision_id, d["veracore_order_id"])
        return {"status": "already_submitted", "order_id": d["veracore_order_id"], "error": None}

    if not veracore_enabled():
        logger.info("[VERACORE SUBMIT] VERACORE_BASE_URL empty — no-op (CSV fallback will handle)")
        return {"status": "noop", "order_id": None, "error": None}

    try:
        from veracore_client import VeraCoreError, build_customs, pick_shipping_method
        from veracore_sync import log_sync
    except ImportError as e:
        logger.warning("[VERACORE SUBMIT] veracore_client not yet installed — no-op: %s", e)
        return {"status": "noop", "order_id": None, "error": None}

    kit      = d.get("kits") or {}
    customer = d.get("customers") or {}
    if not kit:
        logger.warning("[VERACORE SUBMIT] %s has no kit — cannot submit", decision_id)
        return {"status": "skipped", "order_id": None, "error": "no kit"}
    if not customer:
        logger.warning("[VERACORE SUBMIT] %s has no customer — cannot submit", decision_id)
        return {"status": "skipped", "order_id": None, "error": "no customer"}

    vc = get_veracore_client()
    if vc is None:
        return {"status": "noop", "order_id": None, "error": None}

    ship_to         = _build_ship_to_from_customer(customer)
    is_intl         = ship_to["country"] != "US"
    shipping_method = pick_shipping_method(ship_to["country"])
    offer_id        = kit.get("veracore_sku") or kit.get("sku")
    order_public_id = d.get("order_id") or f"OBB-{decision_id[:8]}"
    comments        = f"Kit {kit.get('sku','?')} | T{d.get('trimester','?')} | decision {decision_id[:8]}"

    request_payload = {
        "order_id":         order_public_id,
        "ship_to":          ship_to,
        "line_items":       [{"offer_id": offer_id, "quantity": 1}],
        "shipping_method":  shipping_method,
        "comments":         comments,
        "is_international": is_intl,
    }

    try:
        resp = vc.add_order(
            order_id=order_public_id,
            ship_to=ship_to,
            line_items=[{"offer_id": offer_id, "quantity": 1}],
            shipping_method=shipping_method,
            comments=comments,
            customs=build_customs(kit) if is_intl else None,
        )
        internal_id = resp.get("veracore_internal_id") or order_public_id
        db.table("decisions").update({
            "veracore_order_id":     internal_id,
            "veracore_status":       "submitted",
            "veracore_submitted_at": datetime.utcnow().isoformat(),
            "veracore_last_error":   None,
        }).eq("id", decision_id).execute()
        log_sync(db, "order_submit", decision_id, request_payload, resp, "ok")
        logger.info("[VERACORE SUBMIT] ✅ decision=%s VC id=%s", decision_id, internal_id)
        return {"status": "submitted", "order_id": internal_id, "error": None}

    except VeraCoreError as e:
        err = f"{e} (HTTP {e.status_code})" if e.status_code else str(e)
        logger.error("[VERACORE SUBMIT] ❌ decision=%s: %s", decision_id, err)
        try:
            db.table("decisions").update({
                "veracore_status":     "failed",
                "veracore_last_error": err[:500],
            }).eq("id", decision_id).execute()
        except Exception as db_err:
            logger.error("[VERACORE SUBMIT] Failed to mark decision failed: %s", db_err)
        log_sync(db, "order_submit", decision_id, request_payload,
                 {"body": e.response_body} if e.response_body else None,
                 "fail", err)
        return {"status": "failed", "order_id": None, "error": err}

    except Exception as e:
        err = f"unexpected: {e}"
        logger.error("[VERACORE SUBMIT] ❌ decision=%s: %s", decision_id, err, exc_info=True)
        try:
            db.table("decisions").update({
                "veracore_status":     "failed",
                "veracore_last_error": err[:500],
            }).eq("id", decision_id).execute()
        except Exception:
            pass
        log_sync(db, "order_submit", decision_id, request_payload, None, "fail", err)
        return {"status": "failed", "order_id": None, "error": err}


@app.post("/decisions/{decision_id}/approve")
async def approve_decision(request: Request, decision_id: str):
    """
    Approve a pending decision.
    - Changes status from 'pending' to 'approved'
    - Decrements kit quantity_available by 1 (stock sync)
    """
    try:
        db = get_supabase()
        decision = db.table("decisions").select("*, customers(email, first_name, last_name)").eq("id", decision_id).single().execute()
        if not decision.data:
            logger.error(f"[APPROVE] Decision {decision_id} not found")
            return JSONResponse({"error": "Decision not found"}, status_code=404)

        d = decision.data
        current_status = d.get("status")
        if current_status != "pending":
            logger.warning(f"[APPROVE] Decision {decision_id} is '{current_status}', not 'pending' — skipping")
            return RedirectResponse("/decisions", status_code=303)

        # Update decision status
        db.table("decisions").update({"status": "approved"}).eq("id", decision_id).execute()
        logger.info(f"[APPROVE] Decision {decision_id} approved. Kit: {d.get('kit_sku', 'none')}")

        # Decrement kit stock if kit was assigned
        kit_id = d.get("kit_id")
        if kit_id:
            kit = db.table("kits").select("sku, quantity_available").eq("id", kit_id).single().execute()
            if kit.data:
                new_qty = max(0, kit.data["quantity_available"] - 1)
                db.table("kits").update({"quantity_available": new_qty}).eq("id", kit_id).execute()
                logger.info(f"[APPROVE] Kit {kit.data['sku']} stock: {kit.data['quantity_available']} → {new_qty}")

        # Create a draft shipment NOW so duplicate checking works immediately.
        # ship_date is None until staff click 📦 Ship — at that point we just stamp the date.
        approve_ship_record = {
            "customer_id": d["customer_id"],
            "kit_id": d.get("kit_id"),
            "kit_sku": d.get("kit_sku"),
            "ship_date": None,
            "trimester_at_ship": d.get("trimester"),
            "platform": d.get("platform"),
            "order_id": d.get("order_id"),
            "notes": f"Auto-created from decision {decision_id[:8]}",
        }
        approve_ship_result = db.table("shipments").insert(approve_ship_record).execute()
        approve_shipment_id = approve_ship_result.data[0]["id"] if approve_ship_result.data else None
        logger.info(f"[APPROVE] Draft shipment {approve_shipment_id} created — items now visible to duplicate engine")
        if d.get("kit_id") and approve_shipment_id:
            approve_kit_items = db.table("kit_items").select("item_id").eq("kit_id", d["kit_id"]).execute()
            for ki in (approve_kit_items.data or []):
                try:
                    db.table("shipment_items").insert({
                        "shipment_id": approve_shipment_id,
                        "item_id": ki["item_id"],
                    }).execute()
                except Exception as si_err:
                    logger.warning(f"[APPROVE] Could not add shipment_item: {si_err}")
            logger.info(f"[APPROVE] Populated {len(approve_kit_items.data or [])} item(s) into draft shipment")

        cust_email = d.get("customers", {}).get("email", decision_id[:8]) if d.get("customers") else decision_id[:8]
        cust_name = ""
        if d.get("customers"):
            cust_name = f"{d['customers'].get('first_name', '')} {d['customers'].get('last_name', '')}".strip()
        await log_activity("decision", f"Approved decision for {cust_email}", f"Kit: {d.get('kit_sku', '—')}", "success")

        # Update existing row in Google Sheets (don't create duplicate)
        update_decision_status_in_sheet(
            email=cust_email,
            order_id=d.get("order_id", ""),
            new_status="approved",
            reason_prefix="Approved",
        )

        # Phase 3 — Push to VeraCore (no-op when creds missing → CSV fallback).
        # Idempotent on retry; always writes to veracore_sync_log.
        # Staff can explicitly route to Pirate Ship CSV by adding ?via=pirateship
        # to the approve URL — in that case we skip VC and mark status='manual'
        # so the CSV export picks it up and the VeraCore column shows it clearly.
        via = (request.query_params.get("via") or "").lower()
        if via == "pirateship":
            try:
                db.table("decisions").update({
                    "veracore_status": "manual",
                    "veracore_last_error": None,
                }).eq("id", decision_id).execute()
                logger.info("[APPROVE] decision=%s routed to Pirate Ship (VC push skipped)", decision_id)
                await log_activity("veracore",
                                   f"Approved via Pirate Ship for {cust_email}",
                                   f"Kit: {d.get('kit_sku', '—')} — VC push skipped",
                                   "info")
            except Exception as ps_err:
                logger.error("[APPROVE] Failed to mark manual route: %s", ps_err)
        else:
            try:
                vc_result = submit_to_veracore(decision_id)
                logger.info("[APPROVE] VeraCore push: %s", vc_result)
            except Exception as vc_err:
                # Never let VeraCore failure break the approve request — helper catches
                # everything, but double-guard just in case.
                logger.error("[APPROVE] VeraCore push raised unexpectedly: %s", vc_err, exc_info=True)
    except Exception as e:
        logger.error(f"[APPROVE] Error: {e}", exc_info=True)
        await log_activity("decision", f"Failed to approve decision: {e}", "", "error")
    return RedirectResponse("/decisions", status_code=303)


@app.post("/decisions/{decision_id}/reject")
async def reject_decision(request: Request, decision_id: str):
    """Reject a pending decision."""
    try:
        db = get_supabase()
        decision = db.table("decisions").select("*, customers(email)").eq("id", decision_id).single().execute()
        if not decision.data:
            return JSONResponse({"error": "Decision not found"}, status_code=404)

        d = decision.data
        if d.get("status") != "pending":
            logger.warning(f"[REJECT] Decision {decision_id} is '{d.get('status')}', not 'pending'")
            return RedirectResponse("/decisions", status_code=303)

        db.table("decisions").update({"status": "rejected"}).eq("id", decision_id).execute()
        cust_email = d.get("customers", {}).get("email", decision_id[:8]) if d.get("customers") else decision_id[:8]
        logger.info(f"[REJECT] Decision {decision_id} rejected for {cust_email}")
        await log_activity("decision", f"Rejected decision for {cust_email}", f"Kit: {d.get('kit_sku', '—')}", "info")
        # Sync rejection to Google Sheets
        update_decision_status_in_sheet(
            email=cust_email,
            order_id=d.get("order_id", ""),
            new_status="rejected",
            reason_prefix="Rejected",
        )
    except Exception as e:
        logger.error(f"[REJECT] Error: {e}", exc_info=True)
    return RedirectResponse("/decisions", status_code=303)


@app.post("/decisions/{decision_id}/veracore-retry")
async def veracore_retry_decision(request: Request, decision_id: str):
    """
    Retry a failed VeraCore submission for a single decision.
    Clears veracore_last_error, then re-runs submit_to_veracore (which is idempotent
    — if a VC order ID is already set, it skips cleanly, preventing duplicates).
    """
    try:
        db = get_supabase()
        d_res = db.table("decisions").select(
            "id, veracore_status, veracore_last_error, veracore_order_id"
        ).eq("id", decision_id).single().execute()
        if not d_res.data:
            return JSONResponse({"error": "Decision not found"}, status_code=404)

        logger.info("[VC RETRY] decision=%s current_status=%s order_id=%s",
                    decision_id, d_res.data.get("veracore_status"),
                    d_res.data.get("veracore_order_id"))

        # Clear previous error so UI shows a fresh attempt.
        db.table("decisions").update({"veracore_last_error": None}).eq("id", decision_id).execute()

        result = submit_to_veracore(decision_id)
        logger.info("[VC RETRY] decision=%s result=%s", decision_id, result)
        await log_activity(
            "veracore",
            f"VeraCore retry for decision {decision_id[:8]}: {result['status']}",
            (result.get("error") or "")[:200],
            "success" if result["status"] in ("submitted", "already_submitted") else "warning",
        )
    except Exception as e:
        logger.error("[VC RETRY] Error: %s", e, exc_info=True)
        await log_activity("veracore", f"VeraCore retry failed: {e}", "", "error")
    return RedirectResponse("/decisions", status_code=303)


# ═══════════════════════════════════════════════════════════
# VERACORE OPS PAGE + MANUAL TRIGGERS (Phase 3 UI)
# ═══════════════════════════════════════════════════════════

@app.get("/veracore", response_class=HTMLResponse)
async def veracore_ops_page(request: Request):
    """
    VeraCore ops dashboard — the staff's one-stop view of what the fulfillment
    integration is doing.  Shows connection status, recent syncs, failed submits
    (with retry buttons), and manual trigger buttons.
    """
    try:
        db = get_supabase()
        msg      = request.query_params.get("msg", "")
        msg_type = request.query_params.get("msg_type", "success")

        # Counts for the status tiles.
        approved_count      = 0
        pending_push_count  = 0
        submitted_count     = 0
        failed_count        = 0
        shipped_count       = 0
        try:
            status_rows = db.table("decisions").select("status, veracore_status").execute().data or []
            for r in status_rows:
                s  = r.get("status")
                vs = r.get("veracore_status")
                if s == "approved" and not vs:
                    pending_push_count += 1
                if vs == "submitted":
                    submitted_count += 1
                elif vs == "failed":
                    failed_count += 1
                elif vs == "shipped":
                    shipped_count += 1
                if s == "approved":
                    approved_count += 1
        except Exception as e:
            logger.warning("[VC OPS] counts failed: %s", e)

        # Recent sync log (last 50).
        recent_syncs = []
        try:
            recent_syncs = (db.table("veracore_sync_log")
                              .select("id, run_at, sync_type, status, error, request, response, decision_id")
                              .order("run_at", desc=True)
                              .limit(50)
                              .execute().data or [])
        except Exception as e:
            logger.warning("[VC OPS] sync_log fetch failed (migration 012 applied?): %s", e)

        # Last successful inventory + shipment sync timestamps.
        last_inventory = None
        last_shipment  = None
        for s in recent_syncs:
            if s["status"] == "ok" and s["sync_type"] == "inventory" and last_inventory is None:
                last_inventory = s["run_at"]
            if s["status"] == "ok" and s["sync_type"] == "shipment_poll" and last_shipment is None:
                last_shipment = s["run_at"]
            if last_inventory and last_shipment:
                break

        # Failed submit list w/ retry buttons.
        failed_decisions = []
        try:
            fd = (db.table("decisions")
                    .select("id, order_id, kit_sku, veracore_last_error, veracore_submitted_at, customers(email, first_name, last_name)")
                    .eq("veracore_status", "failed")
                    .order("veracore_submitted_at", desc=True, nullsfirst=False)
                    .limit(30)
                    .execute().data or [])
            failed_decisions = fd
        except Exception as e:
            logger.warning("[VC OPS] failed decisions fetch failed: %s", e)

        return templates.TemplateResponse("veracore.html", {
            "request":          request,
            "page":             "veracore",
            "enabled":          veracore_enabled(),
            "base_url":         VERACORE_BASE_URL,
            "auth_mode":        VERACORE_AUTH_MODE,
            "system_id":        VERACORE_SYSTEM_ID,
            "last_inventory":   last_inventory,
            "last_shipment":    last_shipment,
            "approved_count":   approved_count,
            "pending_push_count": pending_push_count,
            "submitted_count":  submitted_count,
            "failed_count":     failed_count,
            "shipped_count":    shipped_count,
            "recent_syncs":     recent_syncs,
            "failed_decisions": failed_decisions,
            "msg":              msg,
            "msg_type":         msg_type,
        })
    except Exception as e:
        logger.error("[VC OPS] Error: %s", e, exc_info=True)
        return templates.TemplateResponse("veracore.html", {
            "request": request, "page": "veracore",
            "enabled": False, "base_url": "", "auth_mode": "", "system_id": "",
            "last_inventory": None, "last_shipment": None,
            "approved_count": 0, "pending_push_count": 0, "submitted_count": 0,
            "failed_count": 0, "shipped_count": 0,
            "recent_syncs": [], "failed_decisions": [],
            "error": str(e), "msg": "", "msg_type": "error",
        })


@app.post("/veracore/sync-inventory-now")
async def veracore_sync_inventory_now(request: Request):
    """Manually trigger a VeraCore inventory sync (off-schedule)."""
    try:
        if not veracore_enabled():
            return RedirectResponse("/veracore?msg=VeraCore+not+configured+(env+vars+missing)&msg_type=error",
                                    status_code=303)
        db = get_supabase()
        vc = get_veracore_client()
        if vc is None:
            return RedirectResponse("/veracore?msg=VeraCore+client+unavailable&msg_type=error",
                                    status_code=303)
        from veracore_sync import run_inventory_sync
        r = run_inventory_sync(db, vc)
        await log_activity("veracore",
                           f"Manual inventory sync: synced={r['synced']} skipped={r['skipped']} alerts={r['alerts_raised']}",
                           (r.get("error") or "")[:200],
                           "error" if r.get("error") else "success")
        if r.get("error"):
            return RedirectResponse(f"/veracore?msg=Inventory+sync+failed:+{r['error'][:100]}&msg_type=error",
                                    status_code=303)
        return RedirectResponse(
            f"/veracore?msg=Inventory+synced:+{r['synced']}+kits,+{r['alerts_raised']}+alerts+raised&msg_type=success",
            status_code=303)
    except Exception as e:
        logger.error("[VC SYNC NOW] %s", e, exc_info=True)
        return RedirectResponse(f"/veracore?msg=Error:+{str(e)[:100]}&msg_type=error", status_code=303)


@app.post("/veracore/poll-shipments-now")
async def veracore_poll_shipments_now(request: Request):
    """Manually poll VeraCore for shipment/tracking updates."""
    try:
        if not veracore_enabled():
            return RedirectResponse("/veracore?msg=VeraCore+not+configured&msg_type=error", status_code=303)
        db = get_supabase()
        vc = get_veracore_client()
        if vc is None:
            return RedirectResponse("/veracore?msg=VeraCore+client+unavailable&msg_type=error", status_code=303)
        from veracore_sync import run_shipment_poll
        r = run_shipment_poll(db, vc)
        await log_activity("veracore",
                           f"Manual shipment poll: matched={r['matched']} updated={r['updated']} unmatched={r['unmatched']}",
                           (r.get("error") or "")[:200],
                           "error" if r.get("error") else "success")
        if r.get("error"):
            return RedirectResponse(f"/veracore?msg=Poll+failed:+{r['error'][:100]}&msg_type=error", status_code=303)
        return RedirectResponse(
            f"/veracore?msg=Poll+done:+{r['updated']}+decisions+updated,+{r['unmatched']}+unmatched&msg_type=success",
            status_code=303)
    except Exception as e:
        logger.error("[VC POLL NOW] %s", e, exc_info=True)
        return RedirectResponse(f"/veracore?msg=Error:+{str(e)[:100]}&msg_type=error", status_code=303)


@app.post("/decisions/{decision_id}/ship")
async def ship_decision(request: Request, decision_id: str):
    """
    Mark an approved decision as shipped.
    - Creates a shipment record in shipments table
    - Populates shipment_items from kit_items
    """
    try:
        db = get_supabase()
        decision = db.table("decisions").select("*, customers(email, first_name, last_name)").eq("id", decision_id).single().execute()
        if not decision.data:
            return JSONResponse({"error": "Decision not found"}, status_code=404)

        d = decision.data
        if d.get("status") not in ("approved", "pending"):
            logger.warning(f"[SHIP] Decision {decision_id} is '{d.get('status')}', cannot ship")
            return RedirectResponse("/decisions", status_code=303)

        # If shipping from pending, also decrement stock
        if d.get("status") == "pending" and d.get("kit_id"):
            kit = db.table("kits").select("sku, quantity_available").eq("id", d["kit_id"]).single().execute()
            if kit.data:
                new_qty = max(0, kit.data["quantity_available"] - 1)
                db.table("kits").update({"quantity_available": new_qty}).eq("id", d["kit_id"]).execute()
                logger.info(f"[SHIP] Kit {kit.data['sku']} stock: {kit.data['quantity_available']} → {new_qty}")

        # Update decision status to shipped
        db.table("decisions").update({"status": "shipped"}).eq("id", decision_id).execute()

        # Check if draft shipment was already created at Approve time
        existing_ship = db.table("shipments").select("id").eq("customer_id", d["customer_id"]).ilike("notes", f"%decision {decision_id[:8]}%").execute()
        if existing_ship.data:
            # Already created on Approve — just stamp ship_date
            shipment_id = existing_ship.data[0]["id"]
            db.table("shipments").update({"ship_date": date.today().isoformat()}).eq("id", shipment_id).execute()
            logger.info(f"[SHIP] Stamped ship_date on existing draft shipment {shipment_id}")
        else:
            # Backward compat: decision was approved before this fix — create shipment here
            shipment_record = {
                "customer_id": d["customer_id"],
                "kit_id": d.get("kit_id"),
                "kit_sku": d.get("kit_sku"),
                "ship_date": date.today().isoformat(),
                "trimester_at_ship": d.get("trimester"),
                "platform": d.get("platform"),
                "order_id": d.get("order_id"),
                "notes": f"Auto-created from decision {decision_id[:8]}",
            }
            ship_result = db.table("shipments").insert(shipment_record).execute()
            shipment_id = ship_result.data[0]["id"] if ship_result.data else None
            logger.info(f"[SHIP] Created new shipment {shipment_id} for decision {decision_id[:8]}")
            if d.get("kit_id") and shipment_id:
                kit_items = db.table("kit_items").select("item_id").eq("kit_id", d["kit_id"]).execute()
                for ki in (kit_items.data or []):
                    try:
                        db.table("shipment_items").insert({
                            "shipment_id": shipment_id,
                            "item_id": ki["item_id"],
                        }).execute()
                    except Exception as si_err:
                        logger.warning(f"[SHIP] Could not add shipment_item: {si_err}")
                logger.info(f"[SHIP] Added {len(kit_items.data or [])} items to shipment")

        cust_email = d.get("customers", {}).get("email", d["customer_id"][:8]) if d.get("customers") else d["customer_id"][:8]
        cust_name = ""
        if d.get("customers"):
            cust_name = f"{d['customers'].get('first_name', '')} {d['customers'].get('last_name', '')}".strip()
        await log_activity("shipment", f"Shipped {d.get('kit_sku', '—')} to {cust_email}",
                          f"Decision: {decision_id[:8]}, Shipment: {shipment_id[:8] if shipment_id else '?'}", "success")

        # Update existing row in Google Sheets (don't create duplicate)
        update_decision_status_in_sheet(
            email=cust_email,
            order_id=d.get("order_id", ""),
            new_status="shipped",
            reason_prefix="Shipped",
        )
    except Exception as e:
        logger.error(f"[SHIP] Error: {e}", exc_info=True)
        await log_activity("decision", f"Failed to ship decision: {e}", "", "error")
    return RedirectResponse("/decisions", status_code=303)


# ═══════════════════════════════════════════════════════════
# RE-CURATE (Re-run decision engine for a customer)
# ═══════════════════════════════════════════════════════════

@app.post("/customers/{customer_id}/recurate")
async def recurate_customer(request: Request, customer_id: str, background_tasks: BackgroundTasks):
    """
    Re-run the decision engine for a customer.
    Useful when:
    - New kits have been added
    - Customer data was updated (due date, size)
    - Previous decision was 'needs-curation' and new kits are now available
    """
    try:
        db = get_supabase()
        cust = db.table("customers").select("email, first_name, last_name, due_date, clothing_size, trimester").eq("id", customer_id).single().execute()
        if not cust.data:
            logger.error(f"[RECURATE] Customer {customer_id} not found")
            return RedirectResponse(f"/customers/{customer_id}", status_code=303)

        email = cust.data["email"]
        logger.info(f"[RECURATE] Re-running decision engine for {email} (customer_id={customer_id})")

        # Guard: reject if there are already pending decisions to avoid stacking
        existing_pending = db.table("decisions").select("id").eq("customer_id", customer_id).eq("status", "pending").execute()
        if existing_pending.data:
            pending_count = len(existing_pending.data)
            logger.warning(f"[RECURATE] {pending_count} pending decision(s) already exist for {email} — blocked stacking")
            return RedirectResponse(
                f"/customers/{customer_id}?msg={quote(f'{pending_count} pending decision(s) already exist. Reject or approve them first, then re-curate.')}&msg_type=error",
                status_code=303,
            )

        # Run the decision engine
        kit_decision = await assign_kit(customer_id, date.today())
        logger.info(f"[RECURATE] Result: {kit_decision['decision_type']} — Kit: {kit_decision.get('kit_sku', 'none')}")

        # Early return for incomplete-data — do NOT insert a dangling pending decision
        if kit_decision["decision_type"] == "incomplete-data":
            reason = kit_decision.get("reason", "Missing customer data")
            logger.warning(f"[RECURATE] Skipping insert — incomplete-data for {email}: {reason}")
            return RedirectResponse(
                f"/customers/{customer_id}?msg={quote(f'Cannot curate: {reason}. Edit the customer to add missing data first.')}&msg_type=warning",
                status_code=303,
            )

        # Save new decision
        trimester = cust.data.get("trimester")

        # Find old decision's order_id so we can update its sheet row
        old_order_id = ""
        try:
            old_decisions = db.table("decisions").select("order_id").eq("customer_id", customer_id).eq("status", "rejected").order("created_at", desc=True).limit(1).execute()
            if old_decisions.data:
                old_order_id = old_decisions.data[0].get("order_id") or ""
        except Exception:
            pass

        # Update old decision's sheet row to show "re-curated"
        if email:
            update_decision_status_in_sheet(
                email=email,
                order_id=old_order_id,
                new_status="re-curated",
                reason_prefix="Re-curated",
            )

        decision_record = {
            "customer_id": customer_id,
            "kit_id": kit_decision.get("kit_id"),
            "kit_sku": kit_decision.get("kit_sku"),
            "decision_type": kit_decision["decision_type"],
            "reason": f"[Re-curated] {kit_decision['reason']}",
            "status": "pending",
            "order_id": None,
            "platform": None,
            "trimester": trimester,
            "ship_date": date.today().isoformat(),
        }
        db.table("decisions").insert(decision_record).execute()
        logger.info(f"[RECURATE] Decision inserted for {email} ({kit_decision['decision_type']})")

        # Write to Google Sheets in background — non-blocking so Heroku H12 never fires
        sheet_payload = {
            "date": date.today().isoformat(),
            "customer_name": f"{cust.data.get('first_name', '')} {cust.data.get('last_name', '')}".strip(),
            "email": email,
            "platform": "re-curate",
            "trimester": trimester,
            "order_type": "re-curate",
            "kit_sku": kit_decision.get("kit_sku", "—"),
            "decision_type": kit_decision["decision_type"],
            "reason": f"[Re-curated] {kit_decision['reason']}",
            "order_id": old_order_id or "",
            "due_date": cust.data.get("due_date", "") or "",
            "clothing_size": cust.data.get("clothing_size", "") or "",
        }
        def _write_sheet_safe(payload: dict):
            try:
                write_decision_to_sheet(payload)
                logger.info(f"[RECURATE] Google Sheets write succeeded for {email}")
            except Exception as sheet_err:
                logger.error(f"[RECURATE] Google Sheets write failed (non-fatal, decision already saved): {sheet_err}")
        background_tasks.add_task(_write_sheet_safe, sheet_payload)

        await log_activity("decision", f"Re-curated {email}: {kit_decision['decision_type']}",
                          f"Kit: {kit_decision.get('kit_sku', '—')}, T{trimester}", "success")
    except Exception as e:
        logger.error(f"[RECURATE] Error: {e}", exc_info=True)
        try:
            await log_activity("decision", f"Failed to re-curate: {e}", "", "error")
        except Exception as log_err:
            logger.error(f"[RECURATE] log_activity also failed: {log_err}")
    return RedirectResponse(f"/customers/{customer_id}", status_code=303)


# ═══════════════════════════════════════════════════════════
# KIT EDIT
# ═══════════════════════════════════════════════════════════

@app.post("/kits/{kit_id}/edit")
async def edit_kit(
    request: Request,
    kit_id: str,
    sku: str = Form(None),
    name: str = Form(""),
    trimester: int = Form(...),
    size_variant: int = Form(1),
    is_welcome_kit: str = Form(""),
    is_universal: str = Form(""),
    quantity_available: int = Form(0),
    age_rank: int = Form(0),
    cost_per_kit: float = Form(0),
):
    """Edit an existing kit's details, including SKU."""
    try:
        db = get_supabase()
        welcome = is_welcome_kit.lower() in ("true", "on", "1", "yes") if is_welcome_kit else False
        universal = is_universal.lower() in ("true", "on", "1", "yes") if is_universal else False

        # Fetch current SKU + source so we can detect changes
        current = db.table("kits").select("sku, age_rank_source").eq("id", kit_id).single().execute()
        current_sku = current.data["sku"] if current.data else ""
        current_source = current.data.get("age_rank_source") or "auto" if current.data else "auto"
        sku_clean = sku.strip().upper() if sku and sku.strip() else current_sku
        sku_changed = sku_clean != current_sku

        # Age rank source logic:
        # 1. SKU changed → always recompute from new SKU, reset to auto
        # 2. SKU same, age_rank==0 → compute from SKU, auto
        # 3. SKU same, age_rank matches formula → auto
        # 4. SKU same, age_rank differs from formula → manual (user explicitly overrode)
        auto_rank = compute_age_rank_from_sku(sku_clean)

        if sku_changed:
            # SKU changed — check if staff also explicitly set a different age rank
            if age_rank != 0 and age_rank != auto_rank:
                # Staff changed SKU AND typed a custom rank → respect it as manual
                age_rank_source = "manual"
                logger.info(f"[KIT EDIT] SKU changed {current_sku}→{sku_clean} AND age_rank={age_rank} differs from formula={auto_rank} — source=manual (staff override kept)")
            else:
                # Staff changed SKU but left age_rank matching formula (or 0) → recompute auto
                age_rank = auto_rank if auto_rank != 0 else age_rank
                age_rank_source = "auto"
                logger.info(f"[KIT EDIT] SKU changed {current_sku}→{sku_clean}: recomputed age_rank={age_rank} (source=auto)")
        elif age_rank == 0:
            age_rank = auto_rank
            age_rank_source = "auto"
            logger.info(f"[KIT EDIT] age_rank=0 submitted, auto-computed={age_rank} from SKU '{sku_clean}'")
        elif age_rank == auto_rank:
            age_rank_source = "auto"
            logger.info(f"[KIT EDIT] age_rank={age_rank} matches formula — source=auto")
        else:
            # Value differs from formula AND SKU didn't change — this is an explicit manual override
            age_rank_source = "manual"
            logger.info(f"[KIT EDIT] age_rank={age_rank} differs from auto={auto_rank} — source=manual (locked from backfill)")

        record = {
            "sku": sku_clean,
            "name": name.strip() or None,
            "trimester": trimester,
            "size_variant": size_variant,
            "is_welcome_kit": welcome,
            "is_universal": universal,
            "quantity_available": quantity_available,
            "age_rank": age_rank,
            "age_rank_source": age_rank_source,
            "cost_per_kit": cost_per_kit if cost_per_kit > 0 else None,
        }
        db.table("kits").update(record).eq("id", kit_id).execute()
        logger.info(f"[KIT EDIT] Updated kit {sku_clean}: T{trimester}, qty={quantity_available}, welcome={welcome}, universal={universal}, age_rank={age_rank}, source={age_rank_source}")
        await log_activity("kit", f"Edited kit {sku_clean}", f"T{trimester}, Qty: {quantity_available}, Age Rank: {age_rank} ({age_rank_source})", "success")
    except Exception as e:
        logger.error(f"[KIT EDIT] Error: {e}", exc_info=True)
    return RedirectResponse(f"/kits/{kit_id}", status_code=303)


# ═══════════════════════════════════════════════════════════
# ITEM EDIT
# ═══════════════════════════════════════════════════════════

@app.post("/items/{item_id}/edit")
async def edit_item(
    request: Request,
    item_id: str,
    name: str = Form(...),
    sku: str = Form(""),
    category: str = Form(""),
    unit_cost: float = Form(0),
    is_therabox: str = Form(""),
    expiry_date: str = Form(""),
):
    """Edit an existing item's details."""
    try:
        db = get_supabase()
        therabox = is_therabox.lower() in ("true", "on", "1", "yes") if is_therabox else False
        name_clean = name.strip()
        sku_clean = sku.strip().upper() or None
        if not name_clean:
            return RedirectResponse(f"/items?msg=Name+is+required&msg_type=error", status_code=303)
        record = {
            "name": name_clean,
            "sku": sku_clean,
            "category": category.strip() or None,
            "unit_cost": unit_cost if unit_cost > 0 else None,
            "is_therabox": therabox,
            "expiry_date": expiry_date.strip() if expiry_date and expiry_date.strip() else None,
        }
        db.table("items").update(record).eq("id", item_id).execute()
        logger.info(f"[ITEM EDIT] Updated item {item_id}: name='{name_clean}', sku={sku_clean}, therabox={therabox}, expiry={expiry_date}")
        await log_activity("item", f"Edited item '{name_clean}'", f"SKU: {sku_clean}, TheraBox: {therabox}", "success")
        return RedirectResponse(f"/items?msg={quote(f'Updated {name_clean}')}&msg_type=success", status_code=303)
    except Exception as e:
        logger.error(f"[ITEM EDIT] Error: {e}", exc_info=True)
        await log_activity("item", f"Failed to edit item: {e}", "", "error")
        return RedirectResponse(f"/items?msg={quote('Failed to update item')}&msg_type=error", status_code=303)


# ═══════════════════════════════════════════════════════════
# MANUAL KIT OVERRIDE
# ═══════════════════════════════════════════════════════════

@app.post("/customers/{customer_id}/override-kit")
async def manual_override_kit(
    request: Request,
    customer_id: str,
    kit_id: str = Form(...),
    reason: str = Form(""),
):
    """Manually override kit assignment for a customer, bypassing the auto-engine."""
    try:
        db = get_supabase()
        if not kit_id or kit_id.strip() == "":
            return RedirectResponse(
                f"/customers/{customer_id}?msg={quote('Please select a kit')}&msg_type=error",
                status_code=303,
            )

        kit = db.table("kits").select("sku, trimester").eq("id", kit_id).single().execute()
        if not kit.data:
            return RedirectResponse(
                f"/customers/{customer_id}?msg={quote('Kit not found')}&msg_type=error",
                status_code=303,
            )

        cust = db.table("customers").select("email, trimester").eq("id", customer_id).single().execute()
        if not cust.data:
            return RedirectResponse(f"/customers?msg={quote('Customer not found')}&msg_type=error", status_code=303)

        kit_sku = kit.data["sku"]
        override_reason = reason.strip() or f"Manual override — staff selected {kit_sku}"

        decision_record = {
            "customer_id": customer_id,
            "kit_id": kit_id,
            "kit_sku": kit_sku,
            "decision_type": "manual-override",
            "reason": override_reason,
            "status": "pending",
            "order_id": None,
            "platform": None,
            "trimester": cust.data.get("trimester"),
            "ship_date": date.today().isoformat(),
        }
        db.table("decisions").insert(decision_record).execute()
        logger.info(f"[OVERRIDE] Manual override for customer {customer_id}: kit={kit_sku}, reason='{override_reason}'")
        await log_activity(
            "decision",
            f"Manual kit override for {cust.data['email']}: {kit_sku}",
            override_reason,
            "info",
        )
        return RedirectResponse(
            f"/customers/{customer_id}?msg={quote(f'Override created — {kit_sku} is now pending approval')}&msg_type=success",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[OVERRIDE] Error: {e}", exc_info=True)
        await log_activity("decision", f"Failed to create manual override: {e}", "", "error")
        return RedirectResponse(
            f"/customers/{customer_id}?msg={quote('Failed to create override')}&msg_type=error",
            status_code=303,
        )




# ═══════════════════════════════════════════════════════════
# BULK DECISION ACTIONS
# ═══════════════════════════════════════════════════════════

@app.post("/decisions/bulk-action")
async def bulk_decision_action(request: Request):
    """Bulk approve or ship multiple decisions at once."""
    try:
        db          = get_supabase()
        form        = await request.form()
        action      = form.get("action", "").strip()
        decision_ids = form.getlist("decision_ids")
        redirect_qs = form.get("redirect_qs", "")
        logger.info(f"[BULK ACTION] action={action}, count={len(decision_ids)}, ids={decision_ids[:5]}")

        if action not in ("approve", "ship", "reject", "recurate"):
            logger.warning(f"[BULK ACTION] Invalid action: '{action}'")
            sep = "&" if redirect_qs else ""
            return RedirectResponse(f"/decisions?{redirect_qs}{sep}msg=Invalid+action&msg_type=error", status_code=303)

        if not decision_ids:
            sep = "&" if redirect_qs else ""
            return RedirectResponse(f"/decisions?{redirect_qs}{sep}msg=No+decisions+selected&msg_type=error", status_code=303)

        success = failed = skipped = 0

        for did in decision_ids:
            try:
                decision = db.table("decisions").select("*, customers(email, first_name, last_name)").eq("id", did).single().execute()
                if not decision.data:
                    logger.warning(f"[BULK ACTION] Decision {did} not found")
                    failed += 1
                    continue

                d              = decision.data
                current_status = d.get("status")

                if action == "approve":
                    if current_status != "pending":
                        logger.debug(f"[BULK ACTION] Skip approve on {did[:8]} — status={current_status}")
                        skipped += 1
                        continue
                    db.table("decisions").update({"status": "approved"}).eq("id", did).execute()
                    # Decrement kit stock
                    kit_id = d.get("kit_id")
                    if kit_id:
                        kit = db.table("kits").select("sku, quantity_available").eq("id", kit_id).single().execute()
                        if kit.data:
                            new_qty = max(0, kit.data["quantity_available"] - 1)
                            db.table("kits").update({"quantity_available": new_qty}).eq("id", kit_id).execute()
                            logger.debug(f"[BULK ACTION] Kit {kit.data['sku']} stock → {new_qty}")
                    # Create draft shipment + shipment_items
                    draft_ship = {
                        "customer_id":      d["customer_id"],
                        "kit_id":           d.get("kit_id"),
                        "kit_sku":          d.get("kit_sku"),
                        "ship_date":        None,
                        "trimester_at_ship": d.get("trimester"),
                        "platform":         d.get("platform"),
                        "order_id":         d.get("order_id"),
                        "notes":            f"Bulk-approved from decision {did[:8]}",
                    }
                    ship_result = db.table("shipments").insert(draft_ship).execute()
                    ship_id     = ship_result.data[0]["id"] if ship_result.data else None
                    if kit_id and ship_id:
                        kit_items = db.table("kit_items").select("item_id").eq("kit_id", kit_id).execute()
                        for ki in (kit_items.data or []):
                            try:
                                db.table("shipment_items").insert({"shipment_id": ship_id, "item_id": ki["item_id"]}).execute()
                            except Exception:
                                pass
                    logger.info(f"[BULK ACTION] Approved {did[:8]}, draft shipment={ship_id[:8] if ship_id else '?'}")
                    # Sync status to Google Sheet (matches single approve flow)
                    cust_email_bulk = (d.get("customers") or {}).get("email", did[:8])
                    update_decision_status_in_sheet(
                        email=cust_email_bulk,
                        order_id=d.get("order_id", ""),
                        new_status="approved",
                        reason_prefix="Bulk-approved",
                    )
                    # Phase 3 — Push to VeraCore (partial-failure safe).
                    # Succeeded rows stay submitted; failed rows surface in UI for retry.
                    try:
                        vc_bulk_result = submit_to_veracore(did)
                        logger.debug("[BULK ACTION] VeraCore push for %s: %s", did[:8], vc_bulk_result)
                    except Exception as vc_err:
                        logger.error("[BULK ACTION] VeraCore push raised for %s: %s", did[:8], vc_err)
                    success += 1

                elif action == "ship":
                    if current_status not in ("pending", "approved"):
                        logger.debug(f"[BULK ACTION] Skip ship on {did[:8]} — status={current_status}")
                        skipped += 1
                        continue
                    # Decrement stock if still pending
                    if current_status == "pending" and d.get("kit_id"):
                        kit = db.table("kits").select("sku, quantity_available").eq("id", d["kit_id"]).single().execute()
                        if kit.data:
                            new_qty = max(0, kit.data["quantity_available"] - 1)
                            db.table("kits").update({"quantity_available": new_qty}).eq("id", d["kit_id"]).execute()
                    db.table("decisions").update({"status": "shipped"}).eq("id", did).execute()
                    # Stamp ship_date on existing draft shipment or create new
                    existing = db.table("shipments").select("id").eq("customer_id", d["customer_id"]).ilike("notes", f"%decision {did[:8]}%").execute()
                    if existing.data:
                        db.table("shipments").update({"ship_date": date.today().isoformat()}).eq("id", existing.data[0]["id"]).execute()
                        logger.info(f"[BULK ACTION] Stamped ship_date on existing draft shipment {existing.data[0]['id'][:8]}")
                    else:
                        ship_res = db.table("shipments").insert({
                            "customer_id":      d["customer_id"],
                            "kit_id":           d.get("kit_id"),
                            "kit_sku":          d.get("kit_sku"),
                            "ship_date":        date.today().isoformat(),
                            "trimester_at_ship": d.get("trimester"),
                            "platform":         d.get("platform"),
                            "order_id":         d.get("order_id"),
                            "notes":            f"Bulk-shipped from decision {did[:8]}",
                        }).execute()
                        bulk_ship_id = ship_res.data[0]["id"] if ship_res.data else None
                        if d.get("kit_id") and bulk_ship_id:
                            bulk_kit_items = db.table("kit_items").select("item_id").eq("kit_id", d["kit_id"]).execute()
                            for ki in (bulk_kit_items.data or []):
                                try:
                                    db.table("shipment_items").insert({"shipment_id": bulk_ship_id, "item_id": ki["item_id"]}).execute()
                                except Exception:
                                    pass
                            logger.info(f"[BULK ACTION] Created shipment {bulk_ship_id[:8]} with {len(bulk_kit_items.data or [])} items")
                        else:
                            logger.info(f"[BULK ACTION] Created shipment (no kit_id to populate items)")
                    # Sync status to Google Sheet (matches single ship flow)
                    cust_email_bulk = (d.get("customers") or {}).get("email", did[:8])
                    update_decision_status_in_sheet(
                        email=cust_email_bulk,
                        order_id=d.get("order_id", ""),
                        new_status="shipped",
                        reason_prefix="Bulk-shipped",
                    )
                    logger.info(f"[BULK ACTION] Shipped {did[:8]}")
                    success += 1

                elif action == "reject":
                    if current_status not in ("pending", "approved"):
                        logger.debug(f"[BULK ACTION] Skip reject on {did[:8]} — status={current_status}")
                        skipped += 1
                        continue
                    db.table("decisions").update({"status": "rejected"}).eq("id", did).execute()
                    cust_email_bulk = (d.get("customers") or {}).get("email", did[:8])
                    update_decision_status_in_sheet(
                        email=cust_email_bulk,
                        order_id=d.get("order_id", ""),
                        new_status="rejected",
                        reason_prefix="Bulk-rejected",
                    )
                    logger.info(f"[BULK ACTION] Rejected {did[:8]}")
                    success += 1

                elif action == "recurate":
                    if current_status not in ("pending", "rejected"):
                        logger.debug(f"[BULK ACTION] Skip recurate on {did[:8]} — status={current_status}")
                        skipped += 1
                        continue
                    # Guard: check for OTHER pending decisions for this customer (prevent stacking)
                    cust_id_rc = d["customer_id"]
                    other_pending = db.table("decisions").select("id").eq("customer_id", cust_id_rc).eq("status", "pending").neq("id", did).execute()
                    if other_pending.data:
                        logger.warning(f"[BULK ACTION] Skipping recurate for {did[:8]} — {len(other_pending.data)} other pending decision(s) exist for customer {cust_id_rc[:8]}")
                        skipped += 1
                        continue
                    # Reject the old decision first
                    db.table("decisions").update({"status": "rejected"}).eq("id", did).execute()
                    # Run the decision engine fresh
                    kit_decision = await assign_kit(cust_id_rc, date.today())
                    if kit_decision["decision_type"] == "incomplete-data":
                        logger.warning(f"[BULK ACTION] Recurate incomplete-data for {did[:8]}")
                        skipped += 1
                        continue
                    trimester_rc = d.get("trimester")
                    new_decision = {
                        "customer_id": cust_id_rc,
                        "kit_id": kit_decision.get("kit_id"),
                        "kit_sku": kit_decision.get("kit_sku"),
                        "decision_type": kit_decision["decision_type"],
                        "reason": f"Bulk re-curate: {kit_decision['reason']}",
                        "status": "pending",
                        "order_id": d.get("order_id"),
                        "platform": d.get("platform"),
                        "trimester": trimester_rc,
                        "ship_date": date.today().isoformat(),
                    }
                    db.table("decisions").insert(new_decision).execute()
                    logger.info(f"[BULK ACTION] Re-curated {did[:8]} → {kit_decision['decision_type']}")
                    # Sync to Google Sheets: update old row + write new row
                    cust_data_rc = d.get("customers", {}) or {}
                    cust_email_rc = cust_data_rc.get("email", "")
                    cust_name_rc = f"{cust_data_rc.get('first_name', '')} {cust_data_rc.get('last_name', '')}".strip()
                    old_order_id_rc = d.get("order_id", "")
                    update_decision_status_in_sheet(
                        email=cust_email_rc,
                        order_id=old_order_id_rc,
                        new_status="re-curated",
                        reason_prefix="Bulk-re-curated",
                    )
                    write_decision_to_sheet({
                        "date": date.today().isoformat(),
                        "customer_name": cust_name_rc,
                        "email": cust_email_rc,
                        "platform": "re-curate",
                        "trimester": trimester_rc,
                        "order_type": "re-curate",
                        "kit_sku": kit_decision.get("kit_sku", "—"),
                        "decision_type": kit_decision["decision_type"],
                        "reason": f"[Bulk-re-curated] {kit_decision['reason']}",
                        "order_id": old_order_id_rc or "",
                        "due_date": "",
                        "clothing_size": "",
                    })
                    success += 1

            except Exception as row_err:
                logger.error(f"[BULK ACTION] Error on decision {did}: {row_err}", exc_info=True)
                failed += 1

        await log_activity(
            "decision",
            f"Bulk {action}: {success} succeeded, {skipped} skipped, {failed} failed",
            f"{len(decision_ids)} total selected",
            "success" if failed == 0 else "warning",
        )
        parts = [f"{success} {action}d"]
        if skipped: parts.append(f"{skipped} skipped")
        if failed:  parts.append(f"{failed} failed")
        result_msg  = quote(f"Bulk {action}: " + ", ".join(parts))
        result_type = "success" if failed == 0 else "warning"
        sep         = "&" if redirect_qs else ""
        return RedirectResponse(
            f"/decisions?{redirect_qs}{sep}msg={result_msg}&msg_type={result_type}",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[BULK ACTION] Fatal error: {e}", exc_info=True)
        return RedirectResponse(f"/decisions?msg={quote('Bulk action failed: ' + str(e))}&msg_type=error", status_code=303)


# ═══════════════════════════════════════════════════════════
# PIRATE SHIP CSV EXPORT + GOOGLE SHEET EXPORT
# ═══════════════════════════════════════════════════════════

@app.get("/decisions/export-csv")
async def export_decisions_csv(request: Request):
    """Export filtered decisions as a downloadable Pirate Ship CSV."""
    try:
        db = get_supabase()

        # Re-apply same filter params as decisions page
        f_trimester = request.query_params.get("trimester", "").strip()
        f_status    = request.query_params.get("status", "approved").strip()  # default: approved only
        f_type      = request.query_params.get("type", "").strip()
        f_platform  = request.query_params.get("platform", "").strip()
        f_month     = request.query_params.get("month", "").strip()

        def _build_export_q():
            qo = db.table("decisions").select("*, customers(*)")
            if f_trimester:
                try:
                    qo = qo.eq("trimester", int(f_trimester))
                except ValueError:
                    pass
            if f_status:
                qo = qo.eq("status", f_status)
            if f_type:
                qo = qo.eq("decision_type", f_type)
            if f_platform:
                qo = qo.eq("platform", f_platform)
            if f_month:
                try:
                    y, m     = int(f_month[:4]), int(f_month[5:7])
                    start_dt = f"{y}-{m:02d}-01"
                    m += 1
                    if m > 12:
                        m = 1; y += 1
                    end_dt  = f"{y}-{m:02d}-01"
                    qo = qo.gte("created_at", start_dt).lt("created_at", end_dt)
                except Exception as me:
                    logger.warning(f"[EXPORT CSV] Invalid month '{f_month}': {me}")
            return qo

        # Paginate past Supabase 1000-row cap
        rows: list = []
        offset    = 0
        PAGE_SIZE = 1000
        while True:
            batch = _build_export_q().order("created_at", desc=True).range(offset, offset + PAGE_SIZE - 1).execute()
            batch_data = batch.data or []
            rows.extend(batch_data)
            if len(batch_data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        logger.info(f"[EXPORT CSV] Exporting {len(rows)} decisions (status={f_status})")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Name", "Email", "Address Line 1", "Address Line 2",
            "City", "State/Province", "Zip", "Country", "Phone",
            "Order Number", "Kit SKU", "Trimester",
            "Weight (oz)", "Length (in)", "Width (in)", "Height (in)",
            # Phase 3 — international customs cols (appended; Pirate Ship ignores unknown cols)
            "Customs Description", "Customs Value", "Customs Quantity", "Customs Country of Origin",
        ])
        # Defaults confirmed by Sheena on Apr 15 call: ~3 lb (48 oz), 10 × 7.5 × 4 inches
        DEFAULT_WEIGHT_OZ = 48
        DEFAULT_LENGTH_IN = 10
        DEFAULT_WIDTH_IN = 7.5
        DEFAULT_HEIGHT_IN = 4
        # Customs defaults — applied only when country != 'US'
        from veracore_client import normalize_country as _norm_country, build_customs as _build_customs
        for d in rows:
            c    = d.get("customers") or {}
            name = f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".strip()
            country_iso = _norm_country(c.get("country") or "US")
            is_intl = country_iso != "US"
            if is_intl:
                # Use kit cost_per_kit if available; else fallback in build_customs defaults.
                # We only have kit_sku on the decision — grab cost from kits in one shot would be nice,
                # but for a CSV export we keep it cheap: use the generic default value.
                customs = _build_customs({"cost_per_kit": None})
                customs_desc  = customs["description"]
                customs_value = f"{customs['declared_value']:.2f}"
                customs_qty   = "1"
                customs_origin = customs["country_of_origin"]
            else:
                customs_desc = customs_value = customs_qty = customs_origin = ""
            writer.writerow([
                name,
                c.get("email", ""),
                c.get("address_line1", ""),
                c.get("address_line2") or "",
                c.get("city", ""),
                c.get("province", ""),
                c.get("zip") or c.get("zip_code", ""),
                country_iso,
                c.get("phone") or "",
                d.get("order_id", ""),
                d.get("kit_sku", ""),
                f"T{d.get('trimester', '')}",
                DEFAULT_WEIGHT_OZ,
                DEFAULT_LENGTH_IN,
                DEFAULT_WIDTH_IN,
                DEFAULT_HEIGHT_IN,
                customs_desc,
                customs_value,
                customs_qty,
                customs_origin,
            ])

        output.seek(0)
        filename = f"pirateship_export_{date.today().isoformat()}.csv"
        await log_activity("export", f"Pirate Ship CSV export: {len(rows)} rows", f"status={f_status}", "success")
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        logger.error(f"[EXPORT CSV] Error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/decisions/export-veracore-csv")
async def export_decisions_veracore_csv(request: Request):
    """
    Phase 3 fallback — download approved+unsubmitted decisions as a VeraCore-import CSV.
    Used when VeraCore API is down or creds haven't arrived yet.
    Column order matches VeraCore's standard order-import utility.
    """
    try:
        db = get_supabase()
        # Default: rows that are approved in OBB but NOT yet pushed to VeraCore.
        only_unsubmitted = request.query_params.get("only_unsubmitted", "1").strip() != "0"

        q = db.table("decisions").select("*, customers(*), kits(sku, veracore_sku, cost_per_kit)").eq("status", "approved")
        if only_unsubmitted:
            q = q.is_("veracore_order_id", "null")

        # Paginate past Supabase 1000-row cap
        rows: list = []
        offset    = 0
        PAGE_SIZE = 1000
        while True:
            batch = q.order("created_at", desc=True).range(offset, offset + PAGE_SIZE - 1).execute()
            batch_data = batch.data or []
            rows.extend(batch_data)
            if len(batch_data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        logger.info(f"[EXPORT VC CSV] Exporting {len(rows)} decisions (only_unsubmitted={only_unsubmitted})")

        output = io.StringIO()
        writer = csv.writer(output)
        # Column order matches VeraCore's documented import template.
        writer.writerow([
            "OrderID", "ShipToName", "ShipToAddress1", "ShipToAddress2",
            "ShipToCity", "ShipToState", "ShipToZip", "ShipToCountry", "ShipToPhone",
            "OfferID", "Quantity", "ShipMethod", "Comments",
            "CustomsDescription", "CustomsValue",
        ])

        from veracore_client import normalize_country as _norm_country, pick_shipping_method as _pick_ship, build_customs as _build_customs
        for d in rows:
            c = d.get("customers") or {}
            k = d.get("kits") or {}
            name = f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".strip() or c.get("email", "")
            country_iso = _norm_country(c.get("country") or "US")
            is_intl = country_iso != "US"
            offer_id = k.get("veracore_sku") or k.get("sku") or d.get("kit_sku", "")
            order_public_id = d.get("order_id") or f"OBB-{str(d.get('id', ''))[:8]}"
            if is_intl:
                customs = _build_customs(k)
                customs_desc  = customs["description"]
                customs_value = f"{customs['declared_value']:.2f}"
            else:
                customs_desc = ""
                customs_value = ""
            writer.writerow([
                order_public_id,
                name,
                c.get("address_line1", ""),
                c.get("address_line2") or "",
                c.get("city", ""),
                c.get("province", ""),
                c.get("zip") or c.get("zip_code", ""),
                country_iso,
                c.get("phone") or "",
                offer_id,
                1,
                _pick_ship(country_iso),
                f"Kit {k.get('sku','?')} | T{d.get('trimester','?')} | decision {str(d.get('id',''))[:8]}",
                customs_desc,
                customs_value,
            ])

        output.seek(0)
        filename = f"veracore_fallback_{date.today().isoformat()}.csv"
        await log_activity("export", f"VeraCore CSV fallback export: {len(rows)} rows",
                           f"only_unsubmitted={only_unsubmitted}", "success")
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        logger.error(f"[EXPORT VC CSV] Error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/decisions/export-sheet")
async def export_decisions_sheet(request: Request, background_tasks: BackgroundTasks):
    """Push filtered decisions to Google Sheets in Pirate Ship format."""
    try:
        db          = get_supabase()
        form        = await request.form()
        f_status    = form.get("status", "approved")
        f_trimester = form.get("trimester", "")
        f_type      = form.get("type", "")
        f_platform  = form.get("platform", "")
        f_month     = form.get("month", "")
        redirect_qs = form.get("redirect_qs", "")

        def _build_sheet_q():
            qo = db.table("decisions").select("*, customers(*)")
            if f_status:
                qo = qo.eq("status", f_status)
            if f_trimester:
                try:
                    qo = qo.eq("trimester", int(f_trimester))
                except ValueError:
                    pass
            if f_type:
                qo = qo.eq("decision_type", f_type)
            if f_platform:
                qo = qo.eq("platform", f_platform)
            if f_month:
                try:
                    y, m     = int(f_month[:4]), int(f_month[5:7])
                    start_dt = f"{y}-{m:02d}-01"
                    m += 1
                    if m > 12:
                        m = 1; y += 1
                    end_dt  = f"{y}-{m:02d}-01"
                    qo = qo.gte("created_at", start_dt).lt("created_at", end_dt)
                except Exception:
                    pass
            return qo

        # Paginate past Supabase 1000-row cap
        rows: list = []
        offset    = 0
        PAGE_SIZE = 1000
        while True:
            batch = _build_sheet_q().order("created_at", desc=True).range(offset, offset + PAGE_SIZE - 1).execute()
            batch_data = batch.data or []
            rows.extend(batch_data)
            if len(batch_data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        logger.info(f"[EXPORT SHEET] Queued {len(rows)} rows for Google Sheet push")

        def _push_to_sheet(rows_data: list):
            try:
                ws          = get_gsheet()
                tab_name    = f"PS Export {date.today().isoformat()}"
                spreadsheet = ws.spreadsheet
                try:
                    export_ws = spreadsheet.worksheet(tab_name)
                    export_ws.clear()
                except Exception:
                    export_ws = spreadsheet.add_worksheet(title=tab_name, rows=len(rows_data) + 5, cols=12)
                header = [
                    "Name", "Email", "Address Line 1", "Address Line 2",
                    "City", "State", "Zip", "Country", "Phone",
                    "Order Number", "Kit SKU", "Trimester",
                ]
                sheet_rows = [header]
                for d in rows_data:
                    c    = d.get("customers") or {}
                    name = f"{(c.get('first_name') or '')} {(c.get('last_name') or '')}".strip()
                    sheet_rows.append([
                        name,
                        c.get("email", ""),
                        c.get("address_line1", ""),
                        c.get("address_line2") or "",
                        c.get("city", ""),
                        c.get("province", ""),
                        c.get("zip") or c.get("zip_code", ""),
                        c.get("country") or "US",
                        c.get("phone") or "",
                        d.get("order_id", ""),
                        d.get("kit_sku", ""),
                        f"T{d.get('trimester', '')}",
                    ])
                export_ws.update(sheet_rows)
                logger.info(f"[EXPORT SHEET] Pushed {len(rows_data)} rows to tab '{tab_name}'")
            except Exception as se:
                logger.error(f"[EXPORT SHEET] Background push failed: {se}", exc_info=True)

        background_tasks.add_task(_push_to_sheet, rows)
        await log_activity("export", f"Google Sheet export queued: {len(rows)} rows", f"status={f_status}", "success")
        sep         = "&" if redirect_qs else ""
        result_msg  = quote(f"Sheet export queued — {len(rows)} rows pushing to Google Sheets")
        return RedirectResponse(f"/decisions?{redirect_qs}{sep}msg={result_msg}&msg_type=success", status_code=303)
    except Exception as e:
        logger.error(f"[EXPORT SHEET] Error: {e}", exc_info=True)
        return RedirectResponse(f"/decisions?msg={quote('Sheet export failed: ' + str(e))}&msg_type=error", status_code=303)


# ═══════════════════════════════════════════════════════════
# CURATION REPORT — GOOGLE SHEETS EXPORT
# ═══════════════════════════════════════════════════════════

@app.post("/curation-report/{run_id}/export-sheet")
async def export_curation_report_sheet(request: Request, run_id: str, background_tasks: BackgroundTasks):
    """Push curation report to Google Sheets — creates tabs for overview, needs-new, existing-safe, items."""
    try:
        db = get_supabase()

        # Load run data
        run = db.table("curation_runs").select("*").eq("id", run_id).single().execute()
        if not run.data:
            return RedirectResponse(f"/curation-report?msg=Report+not+found&msg_type=error", status_code=303)

        run_data = run.data
        report_month = run_data["report_month"]
        executive = run_data.get("summary_json", {})
        if isinstance(executive, str):
            try:
                executive = json_module.loads(executive)
            except Exception:
                executive = {}

        # Load customer results
        cust_rows = []
        offset = 0
        while True:
            batch = db.table("curation_run_customers").select("*, customers(email, first_name, last_name, clothing_size, platform)").eq("run_id", run_id).range(offset, offset + 999).execute()
            cust_rows.extend(batch.data or [])
            if len(batch.data or []) < 1000:
                break
            offset += 1000

        # Load item results
        item_rows = []
        offset = 0
        while True:
            batch = db.table("curation_run_items").select("*, items(name, sku, category)").eq("run_id", run_id).range(offset, offset + 999).execute()
            item_rows.extend(batch.data or [])
            if len(batch.data or []) < 1000:
                break
            offset += 1000

        logger.info(f"[CURATION SHEET] Exporting report {report_month}: {len(cust_rows)} customers, {len(item_rows)} items")

        def _push_curation_to_sheet():
            try:
                ws = get_gsheet()
                if not ws:
                    logger.error("[CURATION SHEET] Google Sheets not configured")
                    return
                spreadsheet = ws.spreadsheet

                # ── Tab 1: Monthly Report Overview ──
                tab1_name = f"Monthly Report - {report_month}"
                try:
                    tab1 = spreadsheet.worksheet(tab1_name)
                    tab1.clear()
                except Exception:
                    tab1 = spreadsheet.add_worksheet(title=tab1_name, rows=50, cols=10)

                overview_rows = [
                    ["OBB Monthly Curation Report", report_month],
                    ["Ship Date", executive.get("ship_date", "")],
                    ["Generated", executive.get("generated_at", "")],
                    ["Lookback Months", executive.get("lookback_months", 4)],
                    [],
                    ["Trimester", "Projected", "Covered", "Needs New", "Build Qty", "Leftover"],
                ]
                tri_data = executive.get("trimesters", {})
                for tri_key in ["1", "2", "3", "4"]:
                    td = tri_data.get(tri_key) or tri_data.get(int(tri_key), {})
                    if td:
                        overview_rows.append([
                            f"T{tri_key}",
                            td.get("projected_customers", 0),
                            td.get("covered_by_existing", 0),
                            td.get("needs_new_curation", 0),
                            td.get("recommended_build_qty", 0),
                            td.get("expected_leftover", 0),
                        ])
                tab1.update(overview_rows, "A1")

                # ── Tab 2: Needs New Curation ──
                tab2_name = f"Needs New - {report_month}"
                needs_new = [c for c in cust_rows if c.get("needs_new_curation")]
                try:
                    tab2 = spreadsheet.worksheet(tab2_name)
                    tab2.clear()
                except Exception:
                    tab2 = spreadsheet.add_worksheet(title=tab2_name, rows=max(len(needs_new) + 5, 10), cols=8)
                nn_header = ["Name", "Email", "Platform", "Trimester", "Size", "Reason"]
                nn_rows = [nn_header]
                for c in needs_new:
                    ci = c.get("customers", {}) or {}
                    nn_rows.append([
                        f"{ci.get('first_name', '')} {ci.get('last_name', '')}".strip(),
                        ci.get("email", ""),
                        ci.get("platform", ""),
                        f"T{c.get('projected_trimester', '')}",
                        ci.get("clothing_size", ""),
                        c.get("reason", ""),
                    ])
                tab2.update(nn_rows, "A1")

                # ── Tab 3: Existing Kit Safe ──
                tab3_name = f"Existing Safe - {report_month}"
                safe = [c for c in cust_rows if not c.get("needs_new_curation")]
                try:
                    tab3 = spreadsheet.worksheet(tab3_name)
                    tab3.clear()
                except Exception:
                    tab3 = spreadsheet.add_worksheet(title=tab3_name, rows=max(len(safe) + 5, 10), cols=8)
                safe_header = ["Name", "Email", "Platform", "Trimester", "Size", "Recommended Kit", "Alternatives"]
                safe_rows = [safe_header]
                for c in safe:
                    ci = c.get("customers", {}) or {}
                    alts = c.get("alternative_kit_skus", [])
                    alt_str = ", ".join(alts) if isinstance(alts, list) else str(alts or "")
                    safe_rows.append([
                        f"{ci.get('first_name', '')} {ci.get('last_name', '')}".strip(),
                        ci.get("email", ""),
                        ci.get("platform", ""),
                        f"T{c.get('projected_trimester', '')}",
                        ci.get("clothing_size", ""),
                        c.get("recommended_kit_sku", ""),
                        alt_str,
                    ])
                tab3.update(safe_rows, "A1")

                # ── Tab 4: Item Analysis ──
                tab4_name = f"Items - {report_month}"
                try:
                    tab4 = spreadsheet.worksheet(tab4_name)
                    tab4.clear()
                except Exception:
                    tab4 = spreadsheet.add_worksheet(title=tab4_name, rows=max(len(item_rows) + 5, 10), cols=8)
                items_header = ["Item Name", "SKU", "Category", "Trimester", "Blocked Count", "Group Size", "Blocked %", "Risk Level"]
                items_sheet_rows = [items_header]
                for ir in item_rows:
                    ii = ir.get("items", {}) or {}
                    items_sheet_rows.append([
                        ii.get("name", ""),
                        ii.get("sku", ""),
                        ii.get("category", ""),
                        f"T{ir.get('trimester', '')}",
                        ir.get("blocked_count", 0),
                        ir.get("group_size", 0),
                        ir.get("blocked_pct", 0),
                        ir.get("risk_level", ""),
                    ])
                tab4.update(items_sheet_rows, "A1")

                logger.info(f"[CURATION SHEET] Successfully exported 4 tabs for {report_month}")
            except Exception as e:
                logger.error(f"[CURATION SHEET] Error pushing to sheet: {e}", exc_info=True)

        background_tasks.add_task(_push_curation_to_sheet)
        await log_activity("export", f"Curation report {report_month} pushed to Google Sheets", f"run_id={run_id}", "success")
        return RedirectResponse(
            f"/curation-report/{run_id}?msg={quote(f'Exporting {report_month} report to Google Sheets...')}&msg_type=success",
            status_code=303,
        )
    except Exception as e:
        logger.error(f"[CURATION SHEET] Error: {e}", exc_info=True)
        return RedirectResponse(f"/curation-report/{run_id}?msg={quote('Sheet export failed: ' + str(e))}&msg_type=error", status_code=303)


# ═══════════════════════════════════════════════════════════════════
# FORWARD PLANNER ROUTES (Phase 2 — Milestone 6)
# ═══════════════════════════════════════════════════════════════════


@app.get("/forward-planner", response_class=HTMLResponse)
async def forward_planner_page(request: Request, msg: str = "", msg_type: str = ""):
    """Forward Planner page — shows form to generate projection + history of past runs."""
    try:
        logger.info("[PLANNER PAGE] Loading forward planner page")
        from datetime import date as date_cls
        today = date_cls.today()
        if today.month == 12:
            default_month = f"{today.year + 1}-01"
        else:
            default_month = f"{today.year}-{today.month + 1:02d}"

        db = get_supabase()
        all_items_res = db.table("items").select("id, name, sku, category").order("name").execute()
        all_items = all_items_res.data or []

        # Load projection history from DB
        try:
            runs_res = db.table("projection_runs").select("id, base_month, params, created_at, status").order("created_at", desc=True).limit(20).execute()
            projection_runs = runs_res.data or []
        except Exception:
            projection_runs = []

        return templates.TemplateResponse("forward_planner.html", {
            "request": request,
            "page": "forward-planner",
            "msg": msg,
            "msg_type": msg_type,
            "projection": None,
            "default_month": default_month,
            "all_items": all_items,
            "projection_runs": projection_runs,
        })
    except Exception as e:
        logger.error(f"[PLANNER PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("forward_planner.html", {
            "request": request,
            "page": "forward-planner",
            "msg": f"Error: {e}",
            "msg_type": "error",
            "projection": None,
            "default_month": "",
            "all_items": [],
            "projection_runs": [],
        })


@app.post("/forward-planner/generate")
async def generate_forward_projection(
    request: Request,
    background_tasks: BackgroundTasks,
    base_month: str = Form(...),
    horizon_months: int = Form(3),
    ship_day: int = Form(14),
    warehouse_min: int = Form(100),
    recency_months: int = Form(3),
    include_paused: str = Form(""),
):
    """Start forward projection as a background job.
    Returns immediately (HTTP 303) to avoid Heroku's 30-second H12 timeout.
    The client polls /forward-planner/job/{job_id}/status for completion.
    """
    paused = include_paused in ("1", "on", "true")
    recency = recency_months if recency_months > 0 else None
    params = {
        "base_month": base_month,
        "horizon_months": horizon_months,
        "ship_day": ship_day,
        "warehouse_min": warehouse_min,
        "recency_months": recency,
        "include_paused": paused,
    }
    job_id = _create_job("forward_planner", params)
    logger.info(f"[PLANNER GEN] Started background job {job_id} for {base_month} horizon={horizon_months}")
    background_tasks.add_task(_run_forward_planner_job, job_id, params)
    return RedirectResponse(f"/forward-planner/job/{job_id}", status_code=303)


def _run_forward_planner_job(job_id: str, params: dict):
    """Background worker: runs the full forward projection and stores result in job registry and DB."""
    try:
        db = get_supabase()
        base_month = params["base_month"]
        logger.info(
            f"[PLANNER JOB] {job_id} — Generating projection: base={base_month}, "
            f"horizon={params['horizon_months']}, wh_min={params['warehouse_min']}, "
            f"recency={params['recency_months']}"
        )

        projection = project_forward(
            db=db,
            base_month=base_month,
            ship_day=params["ship_day"],
            horizon_months=params["horizon_months"],
            warehouse_minimum=params["warehouse_min"],
            include_paused=params["include_paused"],
            recency_months=params["recency_months"],
        )

        months_count = len(projection.get("months", {}))
        warnings_count = len(projection.get("warnings", []))
        logger.info(f"[PLANNER JOB] {job_id} — Completed: {months_count} months, {warnings_count} warnings")
        _finish_job(job_id, {"projection": projection, "base_month": base_month})

        # Persist to DB so history survives server restarts
        try:
            db.table("projection_runs").insert({
                "id": job_id,
                "base_month": base_month,
                "params": params,
                "result": {"projection": projection, "base_month": base_month},
                "status": "done",
            }).execute()
            logger.info(f"[PLANNER JOB] {job_id} — Saved to projection_runs")
        except Exception as db_err:
            logger.warning(f"[PLANNER JOB] {job_id} — Could not save to DB (table may not exist yet): {db_err}")

    except Exception as e:
        logger.error(f"[PLANNER JOB] {job_id} — Error: {e}", exc_info=True)
        _fail_job(job_id, str(e)[:500])


@app.get("/forward-planner/job/{job_id}", response_class=HTMLResponse)
async def forward_planner_job_page(request: Request, job_id: str):
    """Loading/result page for a forward planner background job."""
    job = _get_job(job_id)

    # If not in memory (server restarted), try loading from DB
    if not job:
        try:
            db = get_supabase()
            run_res = db.table("projection_runs").select("*").eq("id", job_id).single().execute()
            if run_res.data:
                run = run_res.data
                projection = run["result"]["projection"]
                base_month = run["result"]["base_month"]
                try:
                    all_items_res = db.table("items").select("id, name, sku, category").order("name").execute()
                    all_items = all_items_res.data or []
                except Exception:
                    all_items = []
                return templates.TemplateResponse("forward_planner.html", {
                    "request": request,
                    "page": "forward-planner",
                    "msg": f"Projection for {base_month} (loaded from history)",
                    "msg_type": "success",
                    "projection": projection,
                    "default_month": base_month,
                    "all_items": all_items,
                    "projection_runs": [],
                })
        except Exception:
            pass
        return RedirectResponse("/forward-planner?msg=Job+not+found&msg_type=error", status_code=303)

    if job["status"] == "done":
        projection = job["result"]["projection"]
        base_month = job["result"]["base_month"]
        try:
            db = get_supabase()
            all_items_res = db.table("items").select("id, name, sku, category").order("name").execute()
            all_items = all_items_res.data or []
        except Exception:
            all_items = []
        return templates.TemplateResponse("forward_planner.html", {
            "request": request,
            "page": "forward-planner",
            "msg": "Projection generated successfully",
            "msg_type": "success",
            "projection": projection,
            "default_month": base_month,
            "all_items": all_items,
            "projection_runs": [],
        })

    if job["status"] == "error":
        return templates.TemplateResponse("job_loading.html", {
            "request": request,
            "job_id": job_id,
            "job_type": "Forward Planner",
            "status": "error",
            "error": job["error"],
            "cancel_url": "/forward-planner",
            "page": "forward-planner",
        })

    # Still running — show loading page
    return templates.TemplateResponse("job_loading.html", {
        "request": request,
        "job_id": job_id,
        "job_type": "Forward Planner",
        "status": "running",
        "error": None,
        "cancel_url": "/forward-planner",
        "page": "forward-planner",
        "params": job["params"],
    })


@app.get("/forward-planner/job/{job_id}/status")
async def forward_planner_job_status(job_id: str):
    """JSON status endpoint for polling. Used by the loading page."""
    job = _get_job(job_id)
    if not job:
        return JSONResponse({"status": "not_found"}, status_code=404)
    return JSONResponse({
        "status": job["status"],
        "error": job["error"],
        "redirect_url": f"/forward-planner/job/{job_id}" if job["status"] == "done" else None,
    })


@app.post("/forward-planner/{run_id}/delete")
async def delete_projection_run(run_id: str):
    """Delete a projection run from history."""
    try:
        db = get_supabase()
        db.table("projection_runs").delete().eq("id", run_id).execute()
        logger.info(f"[PLANNER DELETE] Deleted projection run {run_id}")
    except Exception as e:
        logger.error(f"[PLANNER DELETE] Error deleting run {run_id}: {e}", exc_info=True)
    return RedirectResponse("/forward-planner?msg=Projection+deleted&msg_type=success", status_code=303)




@app.post("/forward-planner/commit-items")
async def commit_items(
    request: Request,
    report_month: str = Form(...),
    trimester: int = Form(...),
    item_ids: str = Form(""),
):
    """Commit curated items for a specific month/trimester."""
    try:
        db = get_supabase()
        # Parse comma-separated UUIDs
        raw_ids = [x.strip() for x in item_ids.split(",") if x.strip()]
        if not raw_ids:
            return RedirectResponse(
                f"/forward-planner?msg={quote('No item IDs provided')}&msg_type=error",
                status_code=303,
            )

        logger.info(f"[PLANNER COMMIT] Committing {len(raw_ids)} items for {report_month} T{trimester}")

        # Validate item IDs exist
        valid_items = db.table("items").select("id").in_("id", raw_ids).execute()
        valid_ids = {i["id"] for i in (valid_items.data or [])}
        invalid = [x for x in raw_ids if x not in valid_ids]
        if invalid:
            logger.warning(f"[PLANNER COMMIT] Invalid item IDs: {invalid}")
            return RedirectResponse(
                f"/forward-planner?msg={quote(f'Invalid item IDs: {invalid[:3]}')}&msg_type=error",
                status_code=303,
            )

        # Upsert committed items
        rows = [
            {
                "report_month": report_month,
                "trimester": trimester,
                "item_id": iid,
            }
            for iid in valid_ids
        ]
        db.table("curation_committed_items").upsert(
            rows, on_conflict="report_month,trimester,item_id"
        ).execute()

        logger.info(f"[PLANNER COMMIT] Committed {len(rows)} items for {report_month} T{trimester}")
        await log_activity("forward_planner", f"Committed {len(rows)} items for {report_month} T{trimester}", "", "success")

        return RedirectResponse(
            f"/forward-planner?msg={quote(f'Committed {len(rows)} items for {report_month} T{trimester}')}&msg_type=success",
            status_code=303,
        )

    except Exception as e:
        logger.error(f"[PLANNER COMMIT] Error: {e}", exc_info=True)
        return RedirectResponse(
            f"/forward-planner?msg={quote(f'Error: {e}')}&msg_type=error",
            status_code=303,
        )


@app.post("/forward-planner/clear-committed")
async def clear_committed_items(
    request: Request,
    report_month: str = Form(...),
):
    """Clear all committed items for a specific month."""
    try:
        db = get_supabase()
        logger.info(f"[PLANNER CLEAR] Clearing committed items for {report_month}")

        result = db.table("curation_committed_items") \
            .delete() \
            .eq("report_month", report_month) \
            .execute()

        deleted = len(result.data or [])
        logger.info(f"[PLANNER CLEAR] Cleared {deleted} committed items for {report_month}")
        await log_activity("forward_planner", f"Cleared {deleted} committed items for {report_month}", "", "success")

        return RedirectResponse(
            f"/forward-planner?msg={quote(f'Cleared committed items for {report_month}')}&msg_type=success",
            status_code=303,
        )

    except Exception as e:
        logger.error(f"[PLANNER CLEAR] Error: {e}", exc_info=True)
        return RedirectResponse(
            f"/forward-planner?msg={quote(f'Error: {e}')}&msg_type=error",
            status_code=303,
        )


# ─── Start the monthly scheduler ───
_start_scheduler()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
