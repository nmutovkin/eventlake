-- +goose Up
CREATE TABLE rollups (
    tenant_id    UUID NOT NULL REFERENCES tenants(id),
    event_type   TEXT NOT NULL,
    period       TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    group_key    TEXT NOT NULL DEFAULT '',
    group_value  TEXT NOT NULL DEFAULT '',
    count        BIGINT NOT NULL,

    PRIMARY KEY (tenant_id, event_type, period, period_start, group_key, group_value)
);

CREATE INDEX idx_rollups_lookup ON rollups (tenant_id, event_type, period, period_start);

-- +goose Down
DROP TABLE rollups;
