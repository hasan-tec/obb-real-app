-- ============================================================
-- Migration 004: Merge dirty import stubs into canonical items
-- ============================================================
-- During the shipment history import, item names from the spreadsheet
-- were auto-generated as dirty stubs (TitleCase, no category, no cost,
-- slightly different SKU format). These are the same physical products
-- as their canonical counterparts in the items table.
--
-- This migration:
--   1. Merges 39 dirty stubs → their canonical equivalent
--      (shipment_items + kit_items refs moved to canonical, stub deleted)
--   2. Deletes 3 test records (ASDASD, ASDASDAS, WKH21)
--   3. Fixes 5 SKU format errors (missing OBB- prefix, double-dash)
--
-- Strategy for each merge:
--   a. INSERT canonical ref for every shipment/kit the dirty stub was in
--   b. DELETE dirty stub refs (ON CONFLICT DO NOTHING handles edge cases)
--   c. DELETE dirty stub item
--
-- ⚠️  Run AFTER migration 003.
-- ⚠️  Run in Supabase SQL Editor.
-- ============================================================

BEGIN;

DO $$
DECLARE
  r            RECORD;
  dirty_id     UUID;
  can_id       UUID;
  merged_count INTEGER := 0;
  skip_count   INTEGER := 0;
BEGIN

  -- ── MERGE PAIRS (dirty_sku → canonical_sku) ─────────────────
  -- Pattern: dirty stub created from spreadsheet import (no category/cost)
  --          canonical = properly entered item with metadata
  FOR r IN SELECT * FROM (VALUES
    -- clear typos / format differences (1-2 chars)
    ('OBB-AMMINAHSKINCARE+STRETCHMARKSERUMW/GOLDBEADS',         'OBB-AMINNAHSKINCARE+STRETCHMARKSERUMW/GOLDBEADS'),
    ('OBB-BABYMAMATANK(EXTRA LARGE)',                            'OBB-BABYMAMATANK(EXTRALARGE)'),
    ('OBB-BELLYBRACE [EXTRA LARGE]',                            'OBB-BELLYBRACE(EXTRALARGE)PINK'),
    ('OBB-BELLYBRACE [LARGE]',                                  'OBB-BELLYBRACE(LARGE)PINK'),
    ('OBB-BELLYBRACE [MEDIUM]',                                 'OBB-BELLYBRACE(MEDIUM)PINK'),
    ('OBB-BELLYBRACE [SMALL]',                                  'OBB-BELLYBRACE(SMALL)PINK'),
    ('OBB-BIRTHINGAFFIRMATIONCARDS',                            'OBB-BIRTHINGAFFIRMATIONCARDDECK'),
    ('OBB-CURIOSITYCARDS+GARDENOFYOURMIN',                      'OBB-CURIOSITYCARDS+GARDENOFYOURMIND'),
    ('OBB-KEABABIES+HORIZONESILICONEBIBS',                      'OBB-KEABABIES+HORIZONSILICONEBIBS'),
    ('OBB-MAMAMORPHOSISPREGNANCYWORKBOOK',                      'OBB-MAMAMORPHISISPREGNANCYWORKBOOK'),
    ('OBB-PREGNANCYMILESTONECARDS(WE''RE EXPECTING!)',           'OBB-PREGNANCYMILESTONECARDS(WE''REEXPECTING!)'),
    ('OBB-SUNFLOWERMOTHERHOOD+NAUSEARELIEFBANDS',               'OBB-SUNFLOWERMOTHERHOOD+NAUSEARELIEFBAND'),
    ('OBB-WICK+SCENTS+LEMONGRASSBARSOAP',                       'OBB-WICKSCENTS+LEMONGRASSBARSOAP'),
    ('OBB-WOODENCLOSETDIVIDER/HANGER',                          'OBB-WOODENCLOSETDIVIDER'),
    ('OBB-VITAMASQUES+MORNINGGOGGLEEYEMASK',                    'OBB-VITAMASQUES+MORNINGGOGGLEMASK'),

    -- same product, different SKU naming style
    ('OBB-AFTERSPA+DETOXBRUSHW/MASSAGENODULES',                 'OBB-AFTERSPA+DETOXBRUSHWITHMASSAGENODULES'),
    ('OBB-ATTITUDE+STRETCHMARKBODYOILBLOOMINGBELLY',            'OBB-ATTITUDE+STRETCHMARKBODYOILBLOOMINGBELLY ORR AMMIN'),
    ('OBB-AYNIL+DAILYAFFIRMATIONSTUMBLER600ML',                 'OBB-AYNIL+DAILYAFFIRMATIONSTUMBLER'),
    ('OBB-AYNIL+MOMTHINGSTOTEBAG',                              'OBB-AYNIL-MOMTHINGS+TOTEBAG'),
    ('OBB-AZALA+COOLINGBODYBALM',                               'OBB-AZALA+COOLINGBODYBALM60ML'),
    ('OBB-DRBOTANICALS+COFFEEWALNUTFACIALEXFOLIATOR30ML',       'OBB-DRBOTANICALS+COFFEEWALNUTFACIALEXFOLIATOR'),
    ('OBB-FAMILIUS+115HACKS&HACKTIVITIESFORPARENTS',            'OBB-FAMILUS+115HACKS&HACKTIVITIESFORPARENTSOFMINIHUMAN'),
    ('OBB-GOWIPE+FACELIFTWIPES',                                'OBB-GOWIPE+FACELIFTWIPES EXP 10/2024'),
    ('OBB-HAPPYHIVES+PERINEALBALM',                             'OBB-HAPPYHIVE+PERINEALBALM'),
    ('OBB-HAPPYHIVES+RESTLESS',                                 'OBB-HAPPYHIVE+RESTLESS'),
    ('OBB-HS+BROWNCATCOOLINGSLEEPEYEMASK',                      'OBB-HAPPYSHOPPE+BROWNCATCOOLINGSLEEPEYEMASK'),
    ('OBB-KANU+MOTHERCOOLINGLEGGEL',                            'OBB-KANU+COOLINGLEGGEL4.2OZ'),
    ('OBB-KEABABIES+SOOTHENURSINGPADS(SOFTWHITE)',               'OBB-KEABABIES+SOOTHINGNURSINGPADS(SOFTWHITE)'),
    ('OBB-LAMBBOOTIES',                                         'OBB-LAMBBOOTIES(MEDIUM)'),
    ('OBB-MUDMASKY+3IN1HAIRMASK',                               'OBB-MUDMASKY+3IN1HAIRMASK 75ML'),
    ('OBB-OOLUTION+NOSTRETCHCREAM',                             'OBB-OOLUTION+NOSTRETCHCREAM50ML'),
    ('OBB-PIVOTALFACIAL+COOLINGGLOBES',                         'OBB-PIVOTAL+FACIALCOOLINGGLOBES'),
    ('OBB-SIMON&SCHUSTER+YOURPERFECTNURSERYBOOK',               'OBB-SIMONANDSCHUSTER+YOURPERFECTNURSERYBOOK(HARDCOVER)'),
    ('OBB-TUMMYTAPE+SOFTTOUCHBLUSH(SINGLE)',                    'OBB-TUMMYTAPE+SINGLESOFTTOUCHBLUSH'),
    ('OBB-VERAWELLA+LACTATIONSUPPORTCAPSULES',                  'OBB-VERAWELLA+LACTATIONSUPPORT60CAPSULES'),
    ('OBB-WILLOWCOLLECTIVE+BABYBANDANABIB&TEETHINGTOY',         'OBB-WILLOWCOLLECTIVE+BABYBANDANADROOLBIB&TEETHINGTOY'),
    ('OBB-WILLOWCOLLECTIVE+CLOUDHANGINGDOORKNOBSIGN',           'OBB-WILLOWCOLLECTIVE+CLOUDHANGINGDOORKNOBSIGNSSHHSLEEP'),
    ('OBB-WILLOWCOLLECTIVE+SNUGGLESWADDLE(THESNUGGLEISREAL)',   'OBB-WILLOWCOLLECTIVE+SNUGGLESWADDLE(SNUGGLEISREAL)'),
    ('OBB-WILLOWCOLLECTIVE+SOUPERMOM&BABYSPOONSET',             'OBB-WILLOWCOLLECTIVE+SOUPERMOMANDBABYSPOONSETOF2'),

    -- dirty stub has OBB- prefix, canonical is MISSING OBB- prefix
    -- handled together: stub history → canonical, then canonical SKU fixed below
    ('OBB-MITCHELL&PEACH+FINERADIANCEOIL',                      'MITCHELL&PEACH+FINERADIANCEOIL'),
    ('OBB-SKIN&CO+MORNINGDEWCLEANSER',                          'SKIN&CO+MORNINGDEWCLEANSER')

  ) AS t(dirty_sku, canonical_sku)
  LOOP
    SELECT id INTO dirty_id  FROM items WHERE sku = r.dirty_sku;
    SELECT id INTO can_id    FROM items WHERE sku = r.canonical_sku;

    IF dirty_id IS NULL THEN
      RAISE NOTICE '[004] SKIP — dirty not found: %', r.dirty_sku;
      skip_count := skip_count + 1;
      CONTINUE;
    END IF;

    IF can_id IS NULL THEN
      RAISE NOTICE '[004] SKIP — canonical not found: %', r.canonical_sku;
      skip_count := skip_count + 1;
      CONTINUE;
    END IF;

    IF dirty_id = can_id THEN
      RAISE NOTICE '[004] SKIP — same id for %', r.dirty_sku;
      skip_count := skip_count + 1;
      CONTINUE;
    END IF;

    -- Move shipment_items: dirty → canonical
    INSERT INTO shipment_items (shipment_id, item_id)
      SELECT shipment_id, can_id FROM shipment_items WHERE item_id = dirty_id
      ON CONFLICT DO NOTHING;
    DELETE FROM shipment_items WHERE item_id = dirty_id;

    -- Move kit_items: dirty → canonical
    INSERT INTO kit_items (kit_id, item_id, quantity)
      SELECT kit_id, can_id, quantity FROM kit_items WHERE item_id = dirty_id
      ON CONFLICT DO NOTHING;
    DELETE FROM kit_items WHERE item_id = dirty_id;

    -- Delete dirty stub (item_alternatives cascade handles itself)
    DELETE FROM items WHERE id = dirty_id;

    RAISE NOTICE '[004] Merged: % → %', r.dirty_sku, r.canonical_sku;
    merged_count := merged_count + 1;
  END LOOP;

  RAISE NOTICE '[004] MERGE DONE. merged=%, skipped=%', merged_count, skip_count;
END $$;


-- ── STEP 2: Fix canonical SKUs that are missing OBB- prefix ────
-- (Do AFTER merge loop so conflicting dirty SKUs are gone)

UPDATE items
SET sku = 'OBB-MITCHELL&PEACH+FINERADIANCEOIL'
WHERE sku = 'MITCHELL&PEACH+FINERADIANCEOIL';

UPDATE items
SET sku = 'OBB-SKIN&CO+MORNINGDEWCLEANSER'
WHERE sku = 'SKIN&CO+MORNINGDEWCLEANSER';

UPDATE items
SET sku = 'OBB-VOIR+INVISIBLEDRYSHAMPOO&CONDITIONER'
WHERE sku = 'VOIR+INVISIBLEDRYSHAMPOO&CONDITIONER';

UPDATE items
SET sku = 'OBB-DIRTYLAMB+MINTMOCHAEYESERUM'
WHERE sku = 'DIRTYLAMB+MINTMOCHAEYESERUM';

-- Fix the double-dash SKU
UPDATE items
SET sku = 'OBB-COVERIT+DISPOSABLEBABYCHANGINGSTATIONCOVER'
WHERE sku = 'OBB--COVERIT+DISPOSABLEBABYCHANGINGSTATIONCOVER';

-- Fix the space-in-SKU for before toothpaste (cosmetic only, no dup)
UPDATE items
SET sku = 'OBB-BEFORE+PURIFYINGTOOTHPASTESUPERMINTWITHFLOURIDE'
WHERE sku = 'OBB-BEFORE+PURIFYINGTOOTHPASTE SUPERMINTWITHFLOURIDE';

-- Fix Mama Motherhood tshirt stubs that have bracket+space format
UPDATE items SET sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT(EXTRALARGE)STUB'
WHERE sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT [EXTRA LARGE]';

UPDATE items SET sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT(LARGE)STUB'
WHERE sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT [LARGE]';

UPDATE items SET sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT(MEDIUM)STUB'
WHERE sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT [MEDIUM]';

UPDATE items SET sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT(SMALL)STUB'
WHERE sku = 'OBB-MOTHERNOUNMATERNITYTSHIRT [SMALL]';

-- Note: the four MOTHERNOUNMATERNITYTSHIRT stubs above need to be
-- merged into OBB-MOTHERNOUNMATERNITYTSHIRT(EXTRALARGE/LARGE/MEDIUM/SMALL)
-- but those canonical items have DIFFERENT SKUs without PINK suffix.
-- Doing a manual merge for these four:
DO $$
DECLARE
  stub_id UUID;
  can_id  UUID;
  r RECORD;
BEGIN
  FOR r IN SELECT * FROM (VALUES
    ('OBB-MOTHERNOUNMATERNITYTSHIRT(EXTRALARGE)STUB',   'OBB-MOTHERNOUNMATERNITYTSHIRT(EXTRALARGE)'),
    ('OBB-MOTHERNOUNMATERNITYTSHIRT(LARGE)STUB',        'OBB-MOTHERNOUNMATERNITYTSHIRT(LARGE)'),
    ('OBB-MOTHERNOUNMATERNITYTSHIRT(MEDIUM)STUB',       'OBB-MOTHERNOUNMATERNITYTSHIRT(MEDIUM)'),
    ('OBB-MOTHERNOUNMATERNITYTSHIRT(SMALL)STUB',        'OBB-MOTHERNOUNMATERNITYTSHIRT(SMALL)')
  ) AS t(stub_sku, can_sku)
  LOOP
    SELECT id INTO stub_id FROM items WHERE sku = r.stub_sku;
    SELECT id INTO can_id  FROM items WHERE sku = r.can_sku;

    IF stub_id IS NULL OR can_id IS NULL THEN
      RAISE NOTICE '[004] SKIP MotherNoun stub: % → %', r.stub_sku, r.can_sku;
      CONTINUE;
    END IF;

    INSERT INTO shipment_items (shipment_id, item_id)
      SELECT shipment_id, can_id FROM shipment_items WHERE item_id = stub_id
      ON CONFLICT DO NOTHING;
    DELETE FROM shipment_items WHERE item_id = stub_id;

    INSERT INTO kit_items (kit_id, item_id, quantity)
      SELECT kit_id, can_id, quantity FROM kit_items WHERE item_id = stub_id
      ON CONFLICT DO NOTHING;
    DELETE FROM kit_items WHERE item_id = stub_id;

    DELETE FROM items WHERE id = stub_id;
    RAISE NOTICE '[004] Merged MotherNoun: % → %', r.stub_sku, r.can_sku;
  END LOOP;
END $$;


-- ── STEP 3: Delete test records ─────────────────────────────────
-- (kit_items + shipment_items cascade automatically)
DELETE FROM items WHERE sku IN ('ASDASD', 'ASDASDAS', 'WKH21');


COMMIT;


-- ============================================================
-- REQUIRES MANUAL REVIEW — items below were dirty stubs but
-- have NO clear canonical counterpart. They are real products
-- from the shipment history that just lack metadata.
-- Ask Ting to fill in category/cost for these:
-- ============================================================
-- OBB-AMINNAH+BIRTHDAYCAKESUGARPOLISH
-- OBB-AMINNAH+KISSTHESTARSGOLDLIPMASK
-- OBB-AMINNAH+PEACHBOOTYPOLISH8OZ
-- OBB-AMINNAH+PINACOLADAHAND&BODYCLEANSER
-- OBB-ANGELDEAR+BAMBOOSWADDLEBLANKET(JUNGLE)    ← different design from SLOTH, keep separate
-- OBB-ATTITUDE+BODYBUTTERBLOOMINGBELLY           ← different product from StretchMarkOil
-- OBB-ATTITUDE+NURSINGBALMBLOOMINGBELLY          ← different product
-- OBB-AYNIL+BROWNGRADIENTSCARF
-- OBB-AYNIL+ROSEQUARTZ&LAVABEADDIFFUSERBRACELET  ← might differ from ROSEQUARTZDIFFUSERBRACELET
-- OBB-AYNIL+SLUMBERSIPCARAFE
-- OBB-BEESWAXFOOD+NOTHINGFANCYSUPPLYCO
-- OBB-CARIBBREW+MOISTURIZINGHAITANCOFFEESCRUB
-- OBB-CHLOEEMERALD+ROSEQUARTZGUASHA
-- OBB-CLLC+PROTECTEDBODYBODYCREAM
-- OBB-HOLISTICABOTANICALS+POSTPARTUMTEA
-- OBB-KEABABIES+SILICONEBIB(TOOCOOL)
-- OBB-LACSNAC+LACTATIONOVERNIGHTOATSMAPLE&BROWNSUGAR
-- OBB-LUNANECTAR+NOCTURNEMAGNESIUMSLEEPOIL
-- OBB-MITCHELL&PEACH+FINERADIANCEOIL             ← now fixed SKU, add metadata
-- OBB-MUDMASKY+SAVEDBYTHESCRUBSFORUNDERARM
-- OBB-ORGANICBATHCO+ZESTYMORNINGHANDSANITIZER
-- OBB-STEAMBASE+TEATREESCALPWATERSCALERMINIATURE
-- OBB-STONESTREETSOAPHOUSE+HAYHOBODYCREAM        ← different product from HAYHOBODYSCRUB
-- OBB-TASTEAHEAVENLTD+BREASTFEEDINGTEA           ← different product from BEAUTYTASTEA
-- OBB-TISFORTAME+TAMINGCREAM100ML
-- OBB-WATERDROP+MICRODRINKICEDTEABLUEBERRY
-- OBB-WILLOWCOLLECTIVE+LETSEATCAKECANDLE
-- OBB-WILLOWCOLLECTIVE+MIDNIGHTEMBERCANDLE [REPLACED: PO...]  ← clean up SKU manually
-- ============================================================
