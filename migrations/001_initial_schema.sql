-- ============================================================
-- OBB Curation Engine — Phase 1 Initial Schema
-- Run this in Supabase SQL Editor
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ───────── CUSTOMERS ─────────
CREATE TABLE IF NOT EXISTS customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    shopify_customer_id TEXT,
    cratejoy_customer_id TEXT,
    due_date DATE,
    trimester INTEGER CHECK (trimester BETWEEN 1 AND 4),
    clothing_size TEXT CHECK (clothing_size IN ('S', 'M', 'L', 'XL', 'universal', NULL)),
    wants_daddy_item BOOLEAN DEFAULT FALSE,
    baby_gender TEXT,
    subscription_status TEXT DEFAULT 'active' CHECK (subscription_status IN ('active', 'cancelled-prepaid', 'cancelled-expired', 'paused')),
    platform TEXT CHECK (platform IN ('shopify', 'cratejoy', 'both')),
    prepaid_months_remaining INTEGER DEFAULT 0,
    phone TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    province TEXT,
    zip TEXT,
    country TEXT DEFAULT 'US',
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Unique constraint on lowercase email for dedup
CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_email_lower ON customers (LOWER(email));

-- Index on shopify/cratejoy IDs for fast lookup
CREATE INDEX IF NOT EXISTS idx_customers_shopify_id ON customers (shopify_customer_id) WHERE shopify_customer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_customers_cratejoy_id ON customers (cratejoy_customer_id) WHERE cratejoy_customer_id IS NOT NULL;

-- ───────── ITEMS ─────────
CREATE TABLE IF NOT EXISTS items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    sku TEXT UNIQUE,
    category TEXT,
    has_sizing BOOLEAN DEFAULT FALSE,
    unit_cost NUMERIC(10, 2),
    is_therabox BOOLEAN DEFAULT FALSE,
    veracore_sku TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ───────── ITEM ALTERNATIVES (pipe items) ─────────
CREATE TABLE IF NOT EXISTS item_alternatives (
    item_id UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    alternative_item_id UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    PRIMARY KEY (item_id, alternative_item_id)
);

-- ───────── KITS ─────────
CREATE TABLE IF NOT EXISTS kits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku TEXT NOT NULL UNIQUE,
    name TEXT,
    trimester INTEGER NOT NULL CHECK (trimester BETWEEN 1 AND 4),
    size_variant INTEGER DEFAULT 1 CHECK (size_variant BETWEEN 1 AND 4),
    is_welcome_kit BOOLEAN DEFAULT FALSE,
    veracore_sku TEXT,
    quantity_available INTEGER DEFAULT 0,
    age_rank INTEGER DEFAULT 0,
    cost_per_kit NUMERIC(10, 2),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ───────── KIT ITEMS (many-to-many) ─────────
CREATE TABLE IF NOT EXISTS kit_items (
    kit_id UUID NOT NULL REFERENCES kits(id) ON DELETE CASCADE,
    item_id UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    quantity INTEGER DEFAULT 1,
    PRIMARY KEY (kit_id, item_id)
);

-- ───────── SHIPMENTS (customer memory) ─────────
CREATE TABLE IF NOT EXISTS shipments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    kit_id UUID REFERENCES kits(id),
    kit_sku TEXT,
    ship_date DATE,
    trimester_at_ship INTEGER CHECK (trimester_at_ship BETWEEN 1 AND 4),
    platform TEXT CHECK (platform IN ('shopify', 'cratejoy')),
    order_id TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shipments_customer ON shipments (customer_id);

-- ───────── SHIPMENT ITEMS (items in a shipment) ─────────
CREATE TABLE IF NOT EXISTS shipment_items (
    shipment_id UUID NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
    item_id UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    PRIMARY KEY (shipment_id, item_id)
);

-- ───────── DECISIONS ─────────
CREATE TABLE IF NOT EXISTS decisions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    kit_id UUID REFERENCES kits(id),
    kit_sku TEXT,
    decision_type TEXT NOT NULL CHECK (decision_type IN ('auto', 'manual-override', 'needs-curation', 'incomplete-data')),
    reason TEXT,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'shipped', 'override', 'rejected')),
    order_id TEXT,
    platform TEXT CHECK (platform IN ('shopify', 'cratejoy')),
    trimester INTEGER CHECK (trimester BETWEEN 1 AND 4),
    ship_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decisions_customer ON decisions (customer_id);
CREATE INDEX IF NOT EXISTS idx_decisions_status ON decisions (status);

-- ───────── WEBHOOK LOGS ─────────
CREATE TABLE IF NOT EXISTS webhook_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source TEXT NOT NULL CHECK (source IN ('shopify', 'cratejoy')),
    event_type TEXT NOT NULL,
    event_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    headers JSONB DEFAULT '{}'::jsonb,
    status TEXT DEFAULT 'received' CHECK (status IN ('received', 'processed', 'failed', 'duplicate')),
    error_message TEXT,
    processing_time_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Idempotency: unique on event_id + source to prevent duplicate processing
CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_logs_event ON webhook_logs (source, event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_webhook_logs_created ON webhook_logs (created_at DESC);

-- ───────── ACTIVITY LOG ─────────
CREATE TABLE IF NOT EXISTS activity_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail TEXT,
    result TEXT DEFAULT 'success' CHECK (result IN ('success', 'warning', 'error', 'info')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_activity_log_created ON activity_log (created_at DESC);

-- ───────── UPDATED_AT TRIGGER ─────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_kits_updated_at
    BEFORE UPDATE ON kits
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_decisions_updated_at
    BEFORE UPDATE ON decisions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ───────── ROW LEVEL SECURITY ─────────
-- Enable RLS (but allow service_role full access)
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE items ENABLE ROW LEVEL SECURITY;
ALTER TABLE kits ENABLE ROW LEVEL SECURITY;
ALTER TABLE kit_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE shipments ENABLE ROW LEVEL SECURITY;
ALTER TABLE shipment_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE activity_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE item_alternatives ENABLE ROW LEVEL SECURITY;

-- Service role policies (our backend uses service_role key)
CREATE POLICY "service_role_all" ON customers FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON items FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON kits FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON kit_items FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON shipments FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON shipment_items FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON decisions FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON webhook_logs FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON activity_log FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON item_alternatives FOR ALL USING (true) WITH CHECK (true);

-- Done!
-- Run this in Supabase SQL Editor, then your backend can connect via service_role key.
