-- Everything the proxy needs, and nothing else. Two tables: one holding the
-- keys it validates, one holding the metadata it records.
--
-- Note what is absent: there is no column anywhere below for a request or
-- response body, because the proxy never has one to store. Bodies pass through
-- memory and are gone when the response finishes.

-- One row per forwarded request. Metadata only.
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

-- Backs the per-project monthly request count, which the proxy reads on every
-- request. Without this index that count is a sequential scan and the hot path
-- gets slower as the table grows.
CREATE INDEX idx_requests_project_time ON requests (project_key, timestamp);

-- A project is a key the proxy checks before forwarding anything. Clients send
-- it as the X-Monitor-Key header. Insert a row here to mint one:
--
--   INSERT INTO projects (key, name) VALUES ('your-key-here', 'my app');
-- monthly_request_limit overrides MONTHLY_REQUEST_LIMIT for one project. NULL,
-- the default, means "use the env value"; 0 means "no cap for this project".
-- Set it by hand if one project should get a different ceiling than the rest.
CREATE TABLE projects (
    id                    BIGSERIAL PRIMARY KEY,
    key                   TEXT NOT NULL UNIQUE,
    name                  TEXT NOT NULL,
    monthly_request_limit INT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
