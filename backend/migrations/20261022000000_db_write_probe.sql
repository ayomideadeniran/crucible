-- Lightweight singleton table used by the readiness probe to verify that the
-- database accepts writes. A probe transaction inserts and rolls back here;
-- the row keeps the table from being empty without growing over time.
CREATE TABLE IF NOT EXISTS db_write_probe (
    id smallint PRIMARY KEY,
    last_write_at timestamptz NOT NULL DEFAULT NOW()
);

INSERT INTO db_write_probe (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
