-- The two tenants the case study uses, so that `make migrate` followed by the
-- published curl produces the published answer.
--
-- These mirror configs/local.yaml exactly. They are demonstration data and say
-- so in their descriptions; a deployment that wants an empty policy store can
-- run `migrate down 1` or delete the two rows.
--
-- ON CONFLICT DO NOTHING throughout: re-running a migration must not overwrite
-- an operator's edits to a tenant that happens to share a name with the demo.

INSERT INTO policies (name, failure_mode, description) VALUES
    ('free', 'closed', 'Demonstration policy: 20 requests a minute.'),
    ('pro',  'closed', 'Demonstration policy: 1000 a minute with a 200 burst, capped at 25 a second.')
ON CONFLICT (name) DO NOTHING;

INSERT INTO policy_limits (policy_name, name, algorithm, count, period_ms, burst) VALUES
    ('free', 'per-minute', 'token_bucket',   20,  60000,  20),
    ('pro',  'per-minute', 'token_bucket', 1000,  60000, 200),
    ('pro',  'per-second', 'sliding_window', 25,   1000,   0)
ON CONFLICT (policy_name, name) DO NOTHING;

INSERT INTO tenants (id, name, policy_name) VALUES
    ('acme',   'Acme Corp (demo)',    'free'),
    ('globex', 'Globex Inc (demo)',   'pro')
ON CONFLICT (id) DO NOTHING;
