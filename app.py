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
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
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

# ─── App config ───
BASE_URL = os.getenv("BASE_URL", "https://obb-real-d4e16a8bb2ff.herokuapp.com")

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
        # Column order: received_at, platform, customer_name, email, trimester,
        #               order_type, assigned_kit, decision_status, reason,
        #               external_order_id, due_date, clothing_size
        row = [
            decision_data.get("date", date.today().isoformat()),
            decision_data.get("platform", ""),
            decision_data.get("customer_name", ""),
            decision_data.get("email", ""),
            f"T{decision_data.get('trimester', '?')}",
            decision_data.get("order_type", "renewal"),
            decision_data.get("kit_sku", "—"),
            decision_data.get("decision_type", ""),
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
    Finds the row by email (col D) + order_id (col J), then updates:
      - Column F (order_type) → new_status
      - Column H (decision_status) → new_status
      - Column I (reason) → prepend prefix to existing reason
    Falls back to logging a warning if the row is not found.
    """
    try:
        ws = get_gsheet()
        if ws is None:
            logger.info("[GSHEETS] Skipping update — Google Sheets not configured")
            return

        # Find the row by email + order_id
        all_values = ws.get_all_values()
        target_row = None
        for idx, row in enumerate(all_values):
            if idx == 0:
                continue  # skip header
            # Col D = email (index 3), Col J = order_id (index 9)
            row_email = (row[3] if len(row) > 3 else "").strip().lower()
            row_order_id = (row[9] if len(row) > 9 else "").strip()
            if row_email == email.strip().lower() and row_order_id == str(order_id or "").strip():
                target_row = idx + 1  # gspread is 1-indexed
                break

        if target_row:
            # Update status columns in-place: F (col 6), H (col 8), I (col 9)
            ws.update_cell(target_row, 6, new_status)  # order_type
            ws.update_cell(target_row, 8, new_status)  # decision_status
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
            "received_at", "platform", "customer_name", "email",
            "trimester", "order_type", "assigned_kit", "decision_status",
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
        name = (attr.get("name") or "").strip().lower()
        value = (attr.get("value") or "").strip()
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
    logger.info(f"[DECISION ENGINE] Customer: {cust.get('email')}, T{trimester}, Size: {clothing_size or 'universal'}")

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

    # 4. Filter by size variant (1=universal, 2=S/M, 3=L, 4=XL)
    if clothing_size:
        size_to_variant = {"S": 2, "M": 2, "L": 3, "XL": 4}
        customer_variant = size_to_variant.get(clothing_size, 1)
        filtered = [k for k in available_kits if k["size_variant"] == 1 or k["size_variant"] == customer_variant]
    else:
        filtered = [k for k in available_kits if k["size_variant"] == 1]

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
            f"size: {'universal' if chosen['size_variant'] == 1 else clothing_size}. "
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
        shipping = payload.get("shipping_address", {})
        address_line1 = shipping.get("address1", "")
        city = shipping.get("city", "")
        province = shipping.get("province", "")
        zip_code = shipping.get("zip", "")
        country = shipping.get("country_code", "US")

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

        logger.info(f"[SHOPIFY WEBHOOK] Customer: {email}, Name: {first_name} {last_name}")
        logger.info(f"[SHOPIFY WEBHOOK] Price: ${total_price}, Source: {source_name}, Is Renewal: {is_renewal}")
        logger.info(f"[SHOPIFY WEBHOOK] Due Date: {due_date}, Size: {clothing_size}, Gender: {baby_gender}, Daddy: {wants_daddy}")

        # ─── Upsert customer ───
        if email:
            existing_customer = db.table("customers").select("*").ilike("email", email).execute()

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
                db.table("customers").update(customer_record).eq("id", cust_id).execute()
                logger.info(f"[SHOPIFY WEBHOOK] Updated existing customer: {cust_id}")
            else:
                customer_record["platform"] = "shopify"
                customer_record["subscription_status"] = "active"
                result = db.table("customers").insert(customer_record).execute()
                cust_id = result.data[0]["id"] if result.data else None
                logger.info(f"[SHOPIFY WEBHOOK] Created new customer: {cust_id}")

            # ─── Run Decision Engine ───
            if cust_id:
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
            f"Customer: {email}, Trimester: T{trimester or '?'}, Type: {'Renewal' if is_renewal else 'New'}",
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
                cj_sub_id = payload.get("id", "")
                if cj_sub_id:
                    logger.info(f"[CRATEJOY WEBHOOK] Fetching survey results for subscription {cj_sub_id}")
                    try:
                        auth_str = base64.b64encode(f"{CRATEJOY_CLIENT_ID}:{CRATEJOY_CLIENT_SECRET}".encode()).decode()
                        cj_headers = {"Authorization": f"Basic {auth_str}", "Accept": "application/json"}

                        survey_due_date = None
                        survey_size = None
                        survey_gender = None
                        survey_daddy = None
                        survey_past_subscriber = None

                        async with httpx.AsyncClient(timeout=15.0) as client:
                            # Merchant API: GET /v1/product_survey_results/?subscription_id={id}
                            survey_url = f"https://api.cratejoy.com/v1/product_survey_results/?subscription_id={cj_sub_id}"
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


@app.get("/customers", response_class=HTMLResponse)
async def customers_page(request: Request):
    """View all customers."""
    try:
        db = get_supabase()
        custs = db.table("customers").select("*").order("created_at", desc=True).limit(100).execute()
        return templates.TemplateResponse("customers.html", {
            "request": request,
            "customers": custs.data or [],
            "page": "customers",
        })
    except Exception as e:
        logger.error(f"[CUSTOMERS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("customers.html", {
            "request": request,
            "customers": [],
            "error": str(e),
            "page": "customers",
        })


@app.get("/customers/{customer_id}", response_class=HTMLResponse)
async def customer_detail(request: Request, customer_id: str):
    """View a single customer's details and history."""
    try:
        db = get_supabase()
        cust = db.table("customers").select("*").eq("id", customer_id).single().execute()
        decisions = db.table("decisions").select("*").eq("customer_id", customer_id).order("created_at", desc=True).execute()
        shipments = db.table("shipments").select("*").eq("customer_id", customer_id).order("created_at", desc=True).execute()

        # Get all kits for the shipment form dropdown
        kits_list = db.table("kits").select("id, sku, trimester, is_welcome_kit").order("sku").execute()

        return templates.TemplateResponse("customer_detail.html", {
            "request": request,
            "customer": cust.data,
            "decisions": decisions.data or [],
            "shipments": shipments.data or [],
            "kits": kits_list.data or [],
            "page": "customers",
        })
    except Exception as e:
        logger.error(f"[CUSTOMER DETAIL] Error: {e}", exc_info=True)
        return HTMLResponse(f"Customer not found: {e}", status_code=404)


@app.get("/decisions", response_class=HTMLResponse)
async def decisions_page(request: Request):
    """View all kit assignment decisions."""
    try:
        db = get_supabase()
        decs = db.table("decisions").select("*, customers(email, first_name, last_name)").order("created_at", desc=True).limit(100).execute()
        return templates.TemplateResponse("decisions.html", {
            "request": request,
            "decisions": decs.data or [],
            "page": "decisions",
        })
    except Exception as e:
        logger.error(f"[DECISIONS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("decisions.html", {
            "request": request,
            "decisions": [],
            "error": str(e),
            "page": "decisions",
        })


@app.get("/kits", response_class=HTMLResponse)
async def kits_page(request: Request):
    """View and manage kits inventory."""
    try:
        db = get_supabase()
        kits_data = db.table("kits").select("*").order("trimester").order("age_rank").execute()

        # Get item counts per kit
        kit_item_counts = {}
        if kits_data.data:
            all_kit_items = db.table("kit_items").select("kit_id").execute()
            for ki in (all_kit_items.data or []):
                kid = ki["kit_id"]
                kit_item_counts[kid] = kit_item_counts.get(kid, 0) + 1

        # Get all items for the kit creation form
        all_items = db.table("items").select("*").order("name").execute()

        return templates.TemplateResponse("kits.html", {
            "request": request,
            "kits": kits_data.data or [],
            "kit_item_counts": kit_item_counts,
            "all_items": all_items.data or [],
            "page": "kits",
        })
    except Exception as e:
        logger.error(f"[KITS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("kits.html", {
            "request": request,
            "kits": [],
            "kit_item_counts": {},
            "all_items": [],
            "error": str(e),
            "page": "kits",
        })


@app.post("/kits/add")
async def add_kit(
    request: Request,
    sku: str = Form(...),
    name: str = Form(""),
    trimester: int = Form(...),
    size_variant: int = Form(1),
    is_welcome_kit: str = Form(""),
    quantity_available: int = Form(0),
    age_rank: int = Form(0),
    cost_per_kit: float = Form(0),
):
    """Add a new kit with optional item selection."""
    try:
        db = get_supabase()
        welcome = is_welcome_kit.lower() in ("true", "on", "1", "yes") if is_welcome_kit else False
        sku_clean = sku.strip().upper()
        logger.info(f"[KITS] Adding kit: sku={sku_clean}, T{trimester}, size={size_variant}, welcome={welcome}, qty={quantity_available}, age_rank={age_rank}")
        kit_result = db.table("kits").insert({
            "sku": sku_clean,
            "name": name.strip() or None,
            "trimester": trimester,
            "size_variant": size_variant,
            "is_welcome_kit": welcome,
            "quantity_available": quantity_available,
            "age_rank": age_rank,
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
        return templates.TemplateResponse("items.html", {
            "request": request,
            "items": items_data.data or [],
            "page": "items",
        })
    except Exception as e:
        logger.error(f"[ITEMS PAGE] Error: {e}", exc_info=True)
        return templates.TemplateResponse("items.html", {
            "request": request,
            "items": [],
            "error": str(e),
            "page": "items",
        })


@app.post("/items/add")
async def add_item(
    request: Request,
    name: str = Form(...),
    sku: str = Form(""),
    category: str = Form(""),
    unit_cost: float = Form(0),
    is_therabox: str = Form(""),
):
    """Add a new item."""
    try:
        db = get_supabase()
        therabox = is_therabox.lower() in ("true", "on", "1", "yes") if is_therabox else False
        name_clean = name.strip()
        sku_clean = sku.strip().upper() or None
        logger.info(f"[ITEMS] Adding item: name='{name_clean}', sku={sku_clean}, category={category}, therabox={therabox}")
        db.table("items").insert({
            "name": name_clean,
            "sku": sku_clean,
            "category": category.strip() or None,
            "unit_cost": unit_cost if unit_cost > 0 else None,
            "is_therabox": therabox,
        }).execute()
        logger.info(f"[ITEMS] ✅ Added item: {name_clean}")
        await log_activity("item", f"Item '{name_clean}' added", f"SKU: {sku_clean}, TheraBox: {therabox}", "success")
    except Exception as e:
        logger.error(f"[ITEMS] Error adding item: {e}", exc_info=True)
        await log_activity("item", f"Failed to add item: {e}", "", "error")
    return RedirectResponse("/items", status_code=303)


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
            logger.info(f"[CUSTOMER ADD] Shipment history entries: {len(ship_skus)}")

            for i, ship_sku in enumerate(ship_skus):
                ship_sku_clean = ship_sku.strip().upper() if ship_sku else ""
                if not ship_sku_clean:
                    continue
                try:
                    s_date = parse_due_date(ship_dates[i]) if i < len(ship_dates) and ship_dates[i].strip() else None
                    s_tri = int(ship_trimesters[i]) if i < len(ship_trimesters) and ship_trimesters[i].strip() else None
                    s_plat = ship_platforms[i] if i < len(ship_platforms) and ship_platforms[i].strip() else platform

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

                    # Populate shipment_items from kit_items if kit exists
                    if kit_id and shipment_id:
                        kit_items = db.table("kit_items").select("item_id").eq("kit_id", kit_id).execute()
                        for ki in (kit_items.data or []):
                            try:
                                db.table("shipment_items").insert({
                                    "shipment_id": shipment_id,
                                    "item_id": ki["item_id"],
                                }).execute()
                            except Exception:
                                pass

                    logger.info(f"[CUSTOMER ADD] Added shipment #{i+1}: {ship_sku_clean} for {email_clean}")
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

        # If kit exists in DB, also populate shipment_items from kit_items
        if kit_id and shipment_id:
            kit_items = db.table("kit_items").select("item_id").eq("kit_id", kit_id).execute()
            for ki in (kit_items.data or []):
                try:
                    db.table("shipment_items").insert({
                        "shipment_id": shipment_id,
                        "item_id": ki["item_id"],
                    }).execute()
                except Exception as si_err:
                    logger.warning(f"[SHIPMENT ADD] Could not add shipment_item: {si_err}")
            logger.info(f"[SHIPMENT ADD] Added {len(kit_items.data or [])} items to shipment from kit {kit_sku_clean}")

        # Get customer email for logging
        cust = db.table("customers").select("email").eq("id", customer_id).single().execute()
        cust_email = cust.data["email"] if cust.data else customer_id
        logger.info(f"[SHIPMENT ADD] Added shipment for {cust_email}: {kit_sku_clean}, date={ship_date}")
        await log_activity("shipment", f"Manual shipment added for {cust_email}", f"Kit: {kit_sku_clean}, Date: {ship_date}", "success")
    except Exception as e:
        logger.error(f"[SHIPMENT ADD] Error: {e}", exc_info=True)
        await log_activity("shipment", f"Failed to add shipment: {e}", "", "error")
    return RedirectResponse(f"/customers/{customer_id}", status_code=303)


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
        existing = db.table("kit_items").select("id").eq("kit_id", kit_id).eq("item_id", item_id).execute()
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
    except Exception as e:
        logger.error(f"[REJECT] Error: {e}", exc_info=True)
    return RedirectResponse("/decisions", status_code=303)


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

        # Create shipment record
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
        logger.info(f"[SHIP] Created shipment {shipment_id} for decision {decision_id[:8]}")

        # Populate shipment_items from kit_items
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
async def recurate_customer(request: Request, customer_id: str):
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

        # Run the decision engine
        kit_decision = await assign_kit(customer_id, date.today())
        logger.info(f"[RECURATE] Result: {kit_decision['decision_type']} — Kit: {kit_decision.get('kit_sku', 'none')}")

        # Save new decision
        trimester = cust.data.get("trimester")
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

        # Write to Google Sheets
        write_decision_to_sheet({
            "date": date.today().isoformat(),
            "customer_name": f"{cust.data.get('first_name', '')} {cust.data.get('last_name', '')}".strip(),
            "email": email,
            "platform": "re-curate",
            "trimester": trimester,
            "order_type": "re-curate",
            "kit_sku": kit_decision.get("kit_sku", "—"),
            "decision_type": kit_decision["decision_type"],
            "reason": f"[Re-curated] {kit_decision['reason']}",
            "order_id": "",
            "due_date": cust.data.get("due_date", "") or "",
            "clothing_size": cust.data.get("clothing_size", "") or "",
        })

        await log_activity("decision", f"Re-curated {email}: {kit_decision['decision_type']}",
                          f"Kit: {kit_decision.get('kit_sku', '—')}, T{trimester}", "success")
    except Exception as e:
        logger.error(f"[RECURATE] Error: {e}", exc_info=True)
        await log_activity("decision", f"Failed to re-curate: {e}", "", "error")
    return RedirectResponse(f"/customers/{customer_id}", status_code=303)


# ═══════════════════════════════════════════════════════════
# KIT EDIT
# ═══════════════════════════════════════════════════════════

@app.post("/kits/{kit_id}/edit")
async def edit_kit(
    request: Request,
    kit_id: str,
    name: str = Form(""),
    trimester: int = Form(...),
    size_variant: int = Form(1),
    is_welcome_kit: str = Form(""),
    quantity_available: int = Form(0),
    age_rank: int = Form(0),
    cost_per_kit: float = Form(0),
):
    """Edit an existing kit's details."""
    try:
        db = get_supabase()
        welcome = is_welcome_kit.lower() in ("true", "on", "1", "yes") if is_welcome_kit else False
        record = {
            "name": name.strip() or None,
            "trimester": trimester,
            "size_variant": size_variant,
            "is_welcome_kit": welcome,
            "quantity_available": quantity_available,
            "age_rank": age_rank,
            "cost_per_kit": cost_per_kit if cost_per_kit > 0 else None,
        }
        db.table("kits").update(record).eq("id", kit_id).execute()
        kit = db.table("kits").select("sku").eq("id", kit_id).single().execute()
        sku = kit.data["sku"] if kit.data else kit_id[:8]
        logger.info(f"[KIT EDIT] Updated kit {sku}: T{trimester}, qty={quantity_available}, welcome={welcome}")
        await log_activity("kit", f"Edited kit {sku}", f"T{trimester}, Qty: {quantity_available}", "success")
    except Exception as e:
        logger.error(f"[KIT EDIT] Error: {e}", exc_info=True)
    return RedirectResponse(f"/kits/{kit_id}", status_code=303)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
