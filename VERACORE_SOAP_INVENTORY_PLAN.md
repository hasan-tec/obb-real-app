# VeraCore Inventory — Move Item Sync to SOAP `GetProductAvailabilities`

**Status:** IMPLEMENTED AND LIVE-VERIFIED — Steps 1-4 executed and verified 2026-07-30.
Pending: final full pytest run (§8), commit.
**Date:** 2026-07-30
**Written for:** a developer who has never seen this codebase. Follow top to bottom. Do not improvise.

---

## ⚠️ Real bug found DURING implementation (not anticipated by the original plan)

While executing Step 2's verification, one item wrote its `quantity_available`
**twice in the same sync run with two different values** (`0 → 120` then
`120 → 0`). Root cause: SOAP's fuller catalog (1,326 products vs REST's 992)
contains near-duplicate real products that REST's narrower feed never
exposed. Specifically:

- `OBB-Toetalk+MantraGripSocks(MixedDesigns)` — real product, 120 in stock
- `OBB-ToeTalk+MantraGripSocks` — a *different* real product, 0 in stock

Our DB item `Toe Talk - Mantra Grip Socks (Mixed Designs)` had legacy `.sku`
matching the first (case-insensitively) and an **incorrectly assigned**
`veracore_sku` matching the second — a mapping mistake made earlier in this
project, invisible until SOAP revealed the collision. Because the old
matching logic tried `items_by_vc_sku` **or** `items_by_sku` per incoming
row, and iterated all incoming rows, the item got updated by whichever
matching row happened to be processed last — non-deterministic per run.

**Fixed two ways:**
1. **Code** (`veracore_sync.py`, dict construction): `items_by_sku` /
   `kits_by_sku` (the legacy fallback) now only include rows that have **no**
   `veracore_sku` set. Once `veracore_sku` is set, it is the sole match key
   for that row — matching Sheena's own rule that VeraCore Product ID is
   always the source of truth. This closes the entire collision class, not
   just this one item.
2. **Data**: `Toe Talk - Mantra Grip Socks (Mixed Designs)`'s `veracore_sku`
   corrected to `OBB-Toetalk+MantraGripSocks(MixedDesigns)` (120 in stock).
   Verified stable at `120` after a second sync run, no oscillation.

**Lesson for whoever reads this next:** switching to a broader data source
can surface pre-existing bad mappings that a narrower source was
accidentally masking. Don't assume "it matched before" means "it matched
correctly before" — re-verify collisions explicitly, as done here (see the
scan script in the implementation notes below).

---

## 0. TL;DR

Our inventory sync reads `GET /api/GetInventory`. That endpoint returns **only active offers**, not products. Products whose offer is inactive — or that have no offer at all — are invisible to us even though they have real stock in the warehouse. We fix it by reading product-level stock over SOAP instead, using an endpoint and an auth mechanism we **already use in production** for `AddOrder`.

**No new API permissions. No changes needed from VeraCore or the warehouse team.**

---

## 1. Root cause (already proven — do not re-investigate)

Measured live on the OhBaby tenant, 2026-07-30:

| Source | Rows |
|---|---:|
| `/api/Offers` | 1,135 |
| `/api/GetInventory` | 992 |
| `/api/GetInventoryDetails` | 992 (same filter — **not** an alternative) |
| **SOAP `GetProductAvailabilities` (`partNumber=%`)** | **1,326** |

The 1,135 → 992 gap is explained perfectly by one field: **143 of 143** missing offers have `Status.Inactive.Indicator == 1`; **992 of 992** returned offers have `0`. Zero exceptions. Requesting an inactive SKU explicitly via `?offerIds=<sku>` still returns 0 rows — the filter cannot be bypassed from REST.

**Why nobody noticed:** `run_inventory_sync()` iterates *VeraCore's* rows and updates matching DB rows. A DB row VeraCore never returns is never visited — never counted, never logged. The `skipped` counter only measures the opposite direction (VeraCore has a SKU we don't track). Step 5 below fixes this permanently.

### Proof the fix works

| Product | Offer state | SOAP `Available` | VeraCore UI |
|---|---|---:|---:|
| `OBB-PortlandBeeBalm+OregonMintBalm` | inactive | 175 | 175 ✅ |
| `OBB-China+Baby1stYearMilestonePostcards(Chevron)` | no offer | 136 | 136 ✅ |
| `OBB-China+Baby1stYearMilestonePostcards` | no offer | 274 | 274 ✅ |
| `OBB-TheHappyShoppe+Lavender&MugwortEyePillows` | inactive | 100 | 100 ✅ |
| `OBB-HS+Amazonite&LavaDiffuserBracelet` | inactive | 243 | 243 ✅ |

Recovers **11 of 12** frozen items. The 12th, `OBB-MATERNITY MOTHERHOOD TSHIRT [S, M, L, XL]`, is a bracketed size-range placeholder in our DB and is not a real VeraCore product — that is a data cleanup, not a sync bug.

---

## 2. ⚠️ Rules — read before touching anything

1. **Kits stay on REST.** Kits are offer-only constructs; SOAP only found 8 of 126. Do **not** migrate kits. See §6.
2. **A SOAP fault must never be treated as "0 products."** If we returned `[]` on a fault, the sync would zero out every item in the database. The code below raises instead. Do not "helpfully" catch that and return an empty list.
3. **`partNumber` is a SQL `LIKE` pattern.** `%` means everything. Empty string and `*` both return `Invalid Search Criteria`. Always send `%`.
4. **Parse with ElementTree, never regex.** 51 products contain `&`, which arrives XML-escaped as `&amp;`. A regex approach silently loses them — this already happened once during investigation. `ET` unescapes automatically.
5. **Do not touch `templates/` or any HTML.** Backend only.

---

## 3. Pre-flight — confirm your starting state

Run this first. If any line disagrees, **stop and ask Hasan**.

```bash
cd opus-obb-prototype && git status --short && python -c "import veracore_client, veracore_sync; print('imports OK')"
```

Expected: `veracore_sync.py` shows as modified (`M`) — that is the `.strip()` change described in Step 6, which is already applied but not committed. `imports OK` prints.

---

## 4. Step 1 — Add the SOAP client method

**File:** `veracore_client.py`

### 1a. Add the SOAPAction constant

Find this line (currently line 68):

```python
_SOAP_ACTION_ADD_ORDER = "http://sma-promail/AddOrder"
```

Add **directly below it**:

```python
_SOAP_ACTION_GET_PRODUCT_AVAIL = "http://sma-promail/GetProductAvailabilities"

# XML namespaces used when parsing SOAP responses.
_NS_PM = "{http://sma-promail/}"
_NS_ENV = "{http://schemas.xmlsoap.org/soap/envelope/}"
```

`xml.etree.ElementTree` is already imported as `ET` at line 39. **No new imports are needed.** Do not add `re` or `html` — ElementTree handles entity unescaping for us (see Rule 4).

### 1b. Add the method

Find the end of `get_inventory()` — it ends with these two lines (currently 398-399):

```python
        logger.info("[VERACORE] GetInventory pulled %d SKUs", len(normalized))
        return normalized
```

Immediately below, and **before** the comment block `# SOAP helpers (AddOrder)`, paste this method (keep it indented one level, inside the class):

```python
    def get_product_availabilities(self) -> list[dict]:
        """
        SOAP GetProductAvailabilities — read PRODUCT-level warehouse balances.

        Why this exists alongside get_inventory():
          REST /api/GetInventory returns only ACTIVE OFFERS.  Products whose
          offer is inactive (Status.Inactive.Indicator == 1) or that have no
          offer record at all are silently absent from it.  Verified live
          2026-07-30: REST returned 992 rows, this returns 1,326.
          Full write-up in VERACORE_SOAP_INVENTORY_PLAN.md.

        `partNumber` is a SQL LIKE pattern; '%' matches every product.
        Sending it empty or as '*' raises a VeraCore "Invalid Search
        Criteria" fault, so it is hardcoded.

        Returns the same shape as get_inventory() plus 'on_order':
          [{sku, title, available_balance, on_hand, committed, on_order}, ...]

        Raises VeraCoreError on a SOAP fault, unparseable XML, or an empty
        catalog.  It must NEVER return [] — callers zero out stock from it.
        """
        xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <AuthenticationHeader xmlns="http://sma-promail/">
      <Username>{_xml_escape(self.user_id)}</Username>
      <Password>{_xml_escape(self.password)}</Password>
    </AuthenticationHeader>
  </soap:Header>
  <soap:Body>
    <GetProductAvailabilities xmlns="http://sma-promail/">
      <partNumber>%</partNumber>
      <owner>{_xml_escape(self.system_id)}</owner>
    </GetProductAvailabilities>
  </soap:Body>
</soap:Envelope>"""

        logger.info("[VERACORE-SOAP] GetProductAvailabilities starting — owner=%s url=%s",
                    self.system_id or "(none)", self.soap_url)
        response_text = self._soap_request(xml_body, _SOAP_ACTION_GET_PRODUCT_AVAIL)

        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as e:
            logger.error("[VERACORE-SOAP] GetProductAvailabilities unparseable XML — error=%s body=%s",
                         e, response_text[:300], exc_info=True)
            raise VeraCoreError(f"GetProductAvailabilities: unparseable XML: {e}") from e

        # SOAP faults arrive as HTTP 500 with an XML body; _soap_request returns it.
        fault = root.find(f".//{_NS_ENV}Fault")
        if fault is not None:
            msg = (fault.findtext("faultstring") or "unknown SOAP fault").strip()
            logger.error("[VERACORE-SOAP] GetProductAvailabilities fault — msg=%s", msg)
            raise VeraCoreError(f"GetProductAvailabilities SOAP fault: {msg}")

        def _int(el, tag: str) -> int:
            try:
                return int(el.findtext(f"{_NS_PM}{tag}"))
            except (TypeError, ValueError):
                return 0

        # One <WarehouseLevels> per product per warehouse. Today only PFCHIC
        # is configured, but the schema allows several — so aggregate by SKU
        # rather than assuming a single row per product.
        agg: dict[str, dict] = {}
        warehouses: set[str] = set()
        for lvl in root.iter(f"{_NS_PM}WarehouseLevels"):
            sku = (lvl.findtext(f"{_NS_PM}PartNumber") or "").strip()
            if not sku:
                continue
            wh = lvl.find(f"{_NS_PM}Warehouse")
            if wh is not None:
                warehouses.add((wh.findtext(f"{_NS_PM}ID") or "").strip())
            rec = agg.get(sku.upper())
            if rec is None:
                rec = agg[sku.upper()] = {
                    "sku":               sku,
                    "title":             (lvl.findtext(f"{_NS_PM}PartDescription") or "").strip(),
                    "available_balance": 0,
                    "on_hand":           0,
                    "committed":         0,
                    "on_order":          0,
                }
            rec["available_balance"] += _int(lvl, "Available")
            rec["on_hand"]           += _int(lvl, "OnHand")
            rec["committed"]         += _int(lvl, "Reserved")
            rec["on_order"]          += _int(lvl, "OnOrder")

        normalized = list(agg.values())
        if not normalized:
            logger.error("[VERACORE-SOAP] GetProductAvailabilities parsed 0 products — body=%s",
                         response_text[:300])
            raise VeraCoreError(
                "GetProductAvailabilities returned 0 products — refusing to report an empty catalog"
            )

        logger.info("[VERACORE-SOAP] GetProductAvailabilities pulled %d products across %d warehouse(s): %s",
                    len(normalized), len(warehouses), sorted(warehouses))
        return normalized
```

### ✅ Verify Step 1

```bash
cd opus-obb-prototype && OBB_DISABLE_SCHEDULER=1 python -c "from app import get_veracore_client; r=get_veracore_client().get_product_availabilities(); d={x['sku'].strip().upper():x for x in r}; print('products:',len(r)); [print(' ',k,'->',d[k]['available_balance']) for k in ['OBB-PORTLANDBEEBALM+OREGONMINTBALM','OBB-CHINA+BABY1STYEARMILESTONEPOSTCARDS(CHEVRON)','OBB-CHINA+BABY1STYEARMILESTONEPOSTCARDS','OBB-THEHAPPYSHOPPE+LAVENDER&MUGWORTEYEPILLOWS','OBB-HS+AMAZONITE&LAVADIFFUSERBRACELET']]"
```

**Required output** — every number must match exactly:

```
products: 1326
  OBB-PORTLANDBEEBALM+OREGONMINTBALM -> 175
  OBB-CHINA+BABY1STYEARMILESTONEPOSTCARDS(CHEVRON) -> 136
  OBB-CHINA+BABY1STYEARMILESTONEPOSTCARDS -> 274
  OBB-THEHAPPYSHOPPE+LAVENDER&MUGWORTEYEPILLOWS -> 100
  OBB-HS+AMAZONITE&LAVADIFFUSERBRACELET -> 243
```

`products` may drift slightly above 1326 as the warehouse adds SKUs. It must **never** be 0, and must never be ≤992.

**If the two `&` rows raise `KeyError`:** you used regex somewhere instead of ElementTree. Re-read Rule 4.
**If you get `Invalid Search Criteria`:** `partNumber` is not `%`.
**If the log says more than one warehouse:** stop and tell Hasan — the aggregation is correct but §6 assumptions need review.

---

## 5. Step 2 — Point item sync at SOAP, keep kits on REST

**File:** `veracore_sync.py`

### 2a. Fetch both sources

Replace this block (currently lines 101-109):

```python
    # 2. Pull VeraCore inventory (1 API call).
    try:
        rows = vc_client.get_inventory()
    except Exception as e:
        err = f"get_inventory failed: {e}"
        logger.error("[VERACORE SYNC] %s", err, exc_info=True)
        log_sync(db, "inventory", None, None, None, "fail", err)
        result["error"] = err
        return result
```

with:

```python
    # 2. Pull inventory from BOTH sources — they cover different things.
    #    Kits are offer-only constructs, so they come from REST /api/GetInventory.
    #    Items are products; REST hides any product whose offer is inactive or
    #    absent, so they come from SOAP GetProductAvailabilities instead.
    #    See VERACORE_SOAP_INVENTORY_PLAN.md for the evidence.
    try:
        rows = vc_client.get_inventory()
    except Exception as e:
        err = f"get_inventory failed: {e}"
        logger.error("[VERACORE SYNC] %s", err, exc_info=True)
        log_sync(db, "inventory", None, None, None, "fail", err)
        result["error"] = err
        return result

    try:
        product_rows = vc_client.get_product_availabilities()
        logger.info("[VERACORE SYNC] Product source: SOAP (%d products)", len(product_rows))
    except Exception as e:
        # Degrade to REST rather than failing the whole sync — kits still update,
        # and items keep their last known values instead of being zeroed.
        logger.error("[VERACORE SYNC] get_product_availabilities failed, falling back to REST "
                     "for items (inactive/offer-less products will stay stale) — error=%s",
                     e, exc_info=True)
        product_rows = []

    # SOAP is a superset for products, but keep any REST-only SKU as a fallback
    # so switching sources can never reduce coverage.
    soap_skus = {r["sku"].strip().upper() for r in product_rows if r.get("sku")}
    item_rows = product_rows + [
        r for r in rows if r.get("sku") and r["sku"].strip().upper() not in soap_skus
    ]
```

### 2b. Restrict the existing loop to kits only

The loop at line ~114 currently matches kits **and** items. Find the item-matching block that begins:

```python
        # ── Item match (item-level VC SKUs contain '+') ──
        item = items_by_vc_sku.get(sku_upper) or items_by_sku.get(sku_upper)
```

Delete that whole block down to and including its `continue` — up to but **not** including the final two lines of the loop:

```python
        logger.debug("[VERACORE SYNC] No matching kit or item for VeraCore SKU '%s' — skipping", sku)
        result["skipped"] += 1
```

Then change that `logger.debug` line to:

```python
        logger.debug("[VERACORE SYNC] No matching kit for VeraCore offer '%s' — skipping", sku)
```

### 2c. Add the item loop

Immediately **after** the existing `for row in rows:` loop ends (i.e. after the two lines above), add:

```python
    # 3b. Items — sourced from SOAP product availabilities.
    for row in item_rows:
        sku = row.get("sku")
        if not sku:
            result["skipped"] += 1
            continue
        sku_upper = sku.strip().upper()

        item = items_by_vc_sku.get(sku_upper) or items_by_sku.get(sku_upper)
        if not item:
            continue  # product we don't track — normal, not an error

        new_qty = max(0, int(row.get("available_balance", 0) or 0))
        old_qty = int(item.get("quantity_available", 0) or 0)
        patch = {"inventory_synced_at": synced_at_iso}
        if old_qty != new_qty:
            patch["quantity_available"] = new_qty
        try:
            db.table("items").update(patch).eq("id", item["id"]).execute()
            if old_qty != new_qty:
                logger.info("[VERACORE SYNC] Item %s qty: %d → %d", item["sku"], old_qty, new_qty)
                item["quantity_available"] = new_qty
        except Exception as e:
            logger.warning("[VERACORE SYNC] Item update failed for %s: %s", item["sku"], e)
            result["skipped"] += 1
            continue
        result["items_synced"] += 1
```

### ✅ Verify Step 2

Read-only dry run — **makes no writes**:

```bash
cd opus-obb-prototype && OBB_DISABLE_SCHEDULER=1 python -c "from app import get_supabase,get_veracore_client; vc=get_veracore_client(); db=get_supabase(); soap={r['sku'].strip().upper():r['available_balance'] for r in vc.get_product_availabilities()}; items=db.table('items').select('sku,name,veracore_sku,quantity_available').execute().data; k=lambda x:((x.get('veracore_sku') or '').strip() or (x.get('sku') or '').strip()).upper(); ch=[(x['name'],x['quantity_available'],soap[k(x)]) for x in items if k(x) in soap and int(x['quantity_available'] or 0)!=soap[k(x)]]; print('items that will change:',len(ch)); [print(f'  {n[:44]:<44} {o} -> {v}') for n,o,v in ch[:25]]"
```

Expect roughly 11 rows moving off 0, and **175 / 136 / 274 / 100 / 243 must appear**. Small extra churn on other items is normal — SOAP is fresher than the last REST run.

---

## 6. Step 3 — Reverse reconciliation (the actual root-cause fix)

Without this, the next silent gap is invisible again. **Do not skip it.**

**File:** `veracore_sync.py`. Insert immediately **before** the closing `log_sync(...)` call at the end of `run_inventory_sync()`:

```python
    # 4. Reverse reconciliation — find OUR rows that neither source returned.
    #    The loops above iterate VeraCore's rows, so a DB row VeraCore never
    #    mentions is never visited and its stock silently freezes forever.
    #    This is exactly how the inactive-offer bug went unnoticed for months.
    seen = {r["sku"].strip().upper() for r in rows if r.get("sku")}
    seen |= {r["sku"].strip().upper() for r in item_rows if r.get("sku")}

    def _key(r):
        return ((r.get("veracore_sku") or "").strip() or (r.get("sku") or "").strip()).upper()

    unseen_items = [i for i in all_items if _key(i) and _key(i) not in seen]
    unseen_kits  = [k for k in all_kits  if _key(k) and _key(k) not in seen]
    result["unseen_items"] = len(unseen_items)
    result["unseen_kits"]  = len(unseen_kits)

    if unseen_items or unseen_kits:
        logger.warning(
            "[VERACORE SYNC] STALE STOCK: %d item(s) and %d kit(s) were not returned by "
            "VeraCore — their quantities are frozen. items=%s kits=%s",
            len(unseen_items), len(unseen_kits),
            [i.get("sku") for i in unseen_items][:10],
            [k.get("sku") for k in unseen_kits][:10],
        )
```

Also add the two new counters to the `log_sync(...)` response dict on the line below so they land in `veracore_sync_log`.

### ✅ Verify Step 3

Run a real sync locally and read the log:

```bash
cd opus-obb-prototype && OBB_DISABLE_SCHEDULER=1 python -c "from app import get_supabase,get_veracore_client; from veracore_sync import run_inventory_sync; print(run_inventory_sync(get_supabase(), get_veracore_client()))"
```

Expected: a `STALE STOCK` warning reporting **1 item** (`OBB-MATERNITY MOTHERHOOD TSHIRT [S, M, L, XL]`) and **118 kits**. The returned dict must include `unseen_items` and `unseen_kits`.

---

## 7. Step 4 — Keep the `.strip()` normalization

`veracore_sync.py` already has `.strip().upper()` on lines 93-96 and 120 (uncommitted). **Keep it.** SOAP returns trailing spaces on `PartNumber` — e.g. `'OBB-China+Baby1stYearMilestonePostcards(Chevron) '` — so this is now load-bearing, not cosmetic. If you revert it, Chevron postcards break again.

---

## 8. Step 5 — Regression check

```bash
cd opus-obb-prototype && python -m pytest tests/ -q
```

Must be no worse than the pre-change baseline. Record the before/after counts in the PR.

---

## 9. Kits — known gap, do NOT try to fix in code

267 kit offers exist in VeraCore but stop at prefix `CD`. Our `kits` table runs `CE`→`CO` (Sept 2025 → **July 2026, the current month**) with no VeraCore record in either source. That is why 118 kits report as unseen.

This is an **operations question for Sheena/Ting**, not an engineering one. Step 3 surfaces it in the logs. Do not paper over it, do not migrate kits to SOAP, do not delete the kits.

---

## 10. Rollback

Every change is additive. To revert:

```bash
cd opus-obb-prototype && git checkout veracore_client.py veracore_sync.py
```

No migration, no schema change, no data backfill. The only DB writes are `items.quantity_available` / `inventory_synced_at`, which the next sync recomputes from scratch.

---

## 11. Definition of done

- [x] `get_product_availabilities()` returns ≥1,326 products; the five reference SKUs match §1 exactly — **verified live, all 5 exact**
- [x] Both `&` products (Happy Shoppe 100, GEN 3 Amazonite 243) resolve — **confirmed**
- [x] A SOAP fault raises `VeraCoreError` — it never yields `[]` — **implemented and code-reviewed; no real fault occurred during testing to exercise this path live**
- [x] Kits still sync from REST; kit counts unchanged from before the change — **synced=71 both runs, consistent**
- [x] ~11 items move off 0, including 175 / 136 / 274 / 100 / 243 — **10 items moved off 0 in the dry check, all 5 references present; an 11th (Toe Talk, 120) surfaced during live execution — see the collision writeup above**
- [x] No currently-working item changes value except by genuine warehouse movement — **verified: broad collision scan (legacy sku vs veracore_sku both matching a different real product) returns 0 after the fix**
- [x] `STALE STOCK` warning appears with 1 item + 118 kits, and is visible in logs — **exact match, both live runs**
- [x] `pytest tests/` no worse than baseline — **6 passed, 0 failed** (3 suites requiring a live local server on :8000 excluded — pre-existing, unrelated to this change)
- [ ] Sheena asked about the missing `CE`→`CO` kit offers — not yet sent
- [x] Commit the changes — `cd61c53`, pushed to `origin/main` 2026-07-30

---

## 12. Explicitly not doing

- **Not** asking Sheena to reactivate offers. That is a workaround: if our reader depends on warehouse staff maintaining offer status, the bug returns the next time anyone deactivates a product.
- **Not** waiting on `DynamicReporting` / `WarehouseProducts` permission grants. The Product Summary report would also work, but SOAP needs nothing from anyone.
- **Not** migrating kits to SOAP.
- **Not** touching `templates/` or any HTML.
