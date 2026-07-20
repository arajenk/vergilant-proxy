CREATE TABLE requests (
    id                 BIGSERIAL PRIMARY KEY,
    project_key        TEXT NOT NULL,
    timestamp          TIMESTAMPTZ NOT NULL,
    provider           TEXT NOT NULL,
    model              TEXT NOT NULL,
    status             INT NOT NULL,
    latency_ms         BIGINT NOT NULL,
    first_token_ms     BIGINT,
    input_tokens       INT NOT NULL,
    output_tokens      INT NOT NULL,
    estimated_cost_usd NUMERIC(12, 6) NOT NULL,
    error              TEXT
);

-- Serves both the proxy's per-project monthly-usage count (rate limiting) and
-- the alert engine's per-project time-window queries.
CREATE INDEX idx_requests_project_time ON requests (project_key, timestamp);

CREATE TABLE projects (
    id         BIGSERIAL PRIMARY KEY,
    key        TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);