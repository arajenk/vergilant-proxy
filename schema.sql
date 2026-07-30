-- Everything the proxy needs: the keys it validates, the metadata it records,
-- and a thin owners table the quota package reads.
--
-- There is no column for a request or response body. Bodies pass through memory
-- and are gone when the response finishes.

-- One row per forwarded request. Metadata only.
CREATE TABLE requests (
    id                 BIGSERIAL PRIMARY KEY,
    project_key        TEXT NOT NULL,
    timestamp          TIMESTAMPTZ NOT NULL,
    provider           TEXT NOT NULL,
    model              TEXT NOT NULL,
    status             INT NOT NULL,
    latency_ms         BIGINT NOT NULL,
    -- The split of latency_ms: the key lookup, and the provider's time. Nullable
    -- because a request that failed before either stage has no number, and a
    -- zero would claim the proxy added nothing.
    validate_ms        BIGINT,
    upstream_ms        BIGINT,
    first_token_ms     BIGINT,
    input_tokens       INT NOT NULL,
    output_tokens      INT NOT NULL,
    estimated_cost_usd NUMERIC(12, 6) NOT NULL,
    error              TEXT
);

-- Backs the monthly request count read on every request. Without it that count
-- is a sequential scan that slows down as the table grows.
CREATE INDEX idx_requests_project_time ON requests (project_key, timestamp);

-- The owner of a project, and the only thing the proxy reads about one. Running
-- the proxy standalone, leave projects.user_id NULL and never insert here: an
-- unowned project has no history and gets the full grace window.
--
-- consecutive_cap_months is whole months in a row finished over cap. At 2 or
-- more, recording stops at the limit instead of twice it (see the quota
-- package). Nothing here writes it; whatever bills the account does.
CREATE TABLE users (
    id                     BIGSERIAL PRIMARY KEY,
    consecutive_cap_months INT NOT NULL DEFAULT 0
);

-- A key the proxy checks before forwarding anything, sent by clients as the
-- X-Monitor-Key header. Insert a row to mint one:
--
--   INSERT INTO projects (key, name) VALUES ('your-key-here', 'my app');
--
-- monthly_request_limit overrides MONTHLY_REQUEST_LIMIT for one project. NULL
-- means use the env value, 0 means no cap.
CREATE TABLE projects (
    id                    BIGSERIAL PRIMARY KEY,
    key                   TEXT NOT NULL UNIQUE,
    name                  TEXT NOT NULL,
    user_id               BIGINT REFERENCES users(id),
    monthly_request_limit INT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
