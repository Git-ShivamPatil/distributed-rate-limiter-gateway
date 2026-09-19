-- The policy store: who the tenants are, what limits apply to them, and which
-- API keys speak for them.
--
-- The constraints here are not decoration. A limit with a zero period or a
-- tenant id containing the key delimiter would be rejected by the Go
-- validation as well, but the database is the one place every writer passes
-- through -- including psql at 2am -- so the invariants live here too.

CREATE TABLE policies (
    name         TEXT PRIMARY KEY CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    -- 'closed' refuses a request when the counter store cannot be reached;
    -- 'open' admits it and stops enforcing. Closed is the default because an
    -- unenforced limit is indistinguishable from no limit at all.
    failure_mode TEXT NOT NULL DEFAULT 'closed' CHECK (failure_mode IN ('closed', 'open')),
    description  TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE policy_limits (
    policy_name  TEXT NOT NULL REFERENCES policies(name) ON DELETE CASCADE ON UPDATE CASCADE,
    -- The name is part of the counter's storage key, so renaming a limit
    -- starts a fresh counter rather than reinterpreting the old one.
    name         TEXT NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    algorithm    TEXT NOT NULL CHECK (algorithm IN ('token_bucket', 'sliding_window')),
    count        BIGINT NOT NULL CHECK (count > 0 AND count <= 100000000),
    period_ms    BIGINT NOT NULL CHECK (period_ms > 0 AND period_ms <= 86400000),
    -- Token bucket capacity. Zero means "same as count", which makes
    -- `100 per minute` behave the way most people read it.
    burst        BIGINT NOT NULL DEFAULT 0 CHECK (burst >= 0 AND burst <= 100000000),

    -- An endpoint rule is a limit that only applies to matching requests. A
    -- limit with the defaults applies to everything the tenant sends, which is
    -- the tenant-wide quota.
    match_method TEXT NOT NULL DEFAULT '*' CHECK (match_method IN ('*', 'GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS')),
    match_prefix TEXT NOT NULL DEFAULT '' CHECK (match_prefix = '' OR match_prefix LIKE '/%'),

    -- A sliding window keeps one row per admitted request in Redis, so its
    -- count is capped far lower than a bucket's.
    CONSTRAINT sliding_window_is_logged CHECK (
        algorithm <> 'sliding_window' OR (count <= 100000 AND burst IN (0, count))
    ),
    PRIMARY KEY (policy_name, name)
);

CREATE TABLE tenants (
    id          TEXT PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    name        TEXT NOT NULL,
    policy_name TEXT NOT NULL REFERENCES policies(name) ON UPDATE CASCADE,
    -- A disabled tenant is refused rather than unlimited: the row staying
    -- behind is what makes "we turned them off" different from "we lost them".
    disabled_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX tenants_policy_name_idx ON tenants (policy_name);

CREATE TABLE api_keys (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE ON UPDATE CASCADE,
    -- The key itself is never stored. A leaked database dump must not be a
    -- set of working credentials.
    key_hash   BYTEA NOT NULL UNIQUE,
    -- The first few characters, so a human can tell two keys apart in a list
    -- without the store holding anything that authenticates.
    prefix     TEXT NOT NULL,
    label      TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);

CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);

-- Keep updated_at honest without every writer having to remember it.
CREATE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER policies_touch BEFORE UPDATE ON policies
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER tenants_touch BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- Every gateway caches policies in process. These triggers are how a replica
-- that did not serve the admin request finds out that its cache is stale:
-- without them, a policy change takes effect on one node immediately and on
-- the others whenever their TTL happens to lapse.
CREATE FUNCTION notify_tenant_change() RETURNS trigger AS $$
DECLARE
    changed TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN changed := OLD.id; ELSE changed := NEW.id; END IF;
    PERFORM pg_notify('policy_change', 'tenant:' || changed);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION notify_policy_change() RETURNS trigger AS $$
DECLARE
    changed TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN changed := OLD.name; ELSE changed := NEW.name; END IF;
    PERFORM pg_notify('policy_change', 'policy:' || changed);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION notify_policy_limit_change() RETURNS trigger AS $$
DECLARE
    changed TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN changed := OLD.policy_name; ELSE changed := NEW.policy_name; END IF;
    PERFORM pg_notify('policy_change', 'policy:' || changed);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tenants_notify AFTER INSERT OR UPDATE OR DELETE ON tenants
    FOR EACH ROW EXECUTE FUNCTION notify_tenant_change();
CREATE TRIGGER policies_notify AFTER INSERT OR UPDATE OR DELETE ON policies
    FOR EACH ROW EXECUTE FUNCTION notify_policy_change();
CREATE TRIGGER policy_limits_notify AFTER INSERT OR UPDATE OR DELETE ON policy_limits
    FOR EACH ROW EXECUTE FUNCTION notify_policy_limit_change();
