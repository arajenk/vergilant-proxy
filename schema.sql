-- Everything the proxy needs, and nothing else. Three tables: one holding the
-- keys it validates, one holding the metadata it records, and a thin owners
-- table the quota ladder reads.
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
    -- The split of latency_ms: this proxy's own key lookup, and the provider's
    -- time. Nullable because a request that failed before reaching either stage
    -- has no honest number, and a zero would claim the proxy added nothing.
    validate_ms        BIGINT,
    upstream_ms        BIGINT,
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

-- The owner of a project, and the only thing the proxy reads about one.
--
-- If you are running this proxy on its own you can leave projects.user_id NULL
-- and never insert here. An unowned project has no history, which the ladder
-- treats the same as a clean one, so it simply gets the full grace window.
--
-- consecutive_cap_months is how many whole months in a row this owner has
-- finished over their cap. See the quota package: at 2 or more, a project stops
-- being recorded at its limit instead of at twice the limit. Nothing in this
-- module writes the column; it is set by whatever bills the account.
CREATE TABLE users (
    id                     BIGSERIAL PRIMARY KEY,
    consecutive_cap_months INT NOT NULL DEFAULT 0
);

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
    user_id               BIGINT REFERENCES users(id),
    monthly_request_limit INT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
