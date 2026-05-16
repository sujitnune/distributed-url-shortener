-- Distributed URL Shortener — PostgreSQL Schema
-- Run once on initial deploy (auto-run by app on startup)

CREATE TABLE IF NOT EXISTS urls (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR(10) UNIQUE NOT NULL,
    target_url TEXT NOT NULL,
    primary_node VARCHAR(50) NOT NULL,
    replicas TEXT NOT NULL DEFAULT '[]',
    backend_server VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_logs (
    id SERIAL PRIMARY KEY,
    message TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_stats (
    key VARCHAR(50) PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

-- Seed initial stats rows
INSERT INTO system_stats (key, value) VALUES
    ('cdn_hits', 0),
    ('cdn_misses', 0),
    ('backend1_requests', 0),
    ('backend2_requests', 0),
    ('backend3_requests', 0)
ON CONFLICT (key) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_urls_short_code ON urls (short_code);
CREATE INDEX IF NOT EXISTS idx_event_logs_created ON event_logs (created_at DESC);
