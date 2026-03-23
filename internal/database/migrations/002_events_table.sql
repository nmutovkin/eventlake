-- +goose Up
CREATE TABLE events (
    id              UUID DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    event_type      TEXT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT,
    user_id         TEXT,
    session_id      TEXT,
    properties      JSONB NOT NULL DEFAULT '{}',

    PRIMARY KEY (tenant_id, timestamp, id)
) PARTITION BY LIST (tenant_id);

-- +goose Down
DROP TABLE events;
