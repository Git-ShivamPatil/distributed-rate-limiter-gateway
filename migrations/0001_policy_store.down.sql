DROP TRIGGER IF EXISTS policy_limits_notify ON policy_limits;
DROP TRIGGER IF EXISTS policies_notify ON policies;
DROP TRIGGER IF EXISTS tenants_notify ON tenants;
DROP TRIGGER IF EXISTS tenants_touch ON tenants;
DROP TRIGGER IF EXISTS policies_touch ON policies;

DROP FUNCTION IF EXISTS notify_policy_limit_change();
DROP FUNCTION IF EXISTS notify_policy_change();
DROP FUNCTION IF EXISTS notify_tenant_change();
DROP FUNCTION IF EXISTS touch_updated_at();

DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS tenants;
DROP TABLE IF EXISTS policy_limits;
DROP TABLE IF EXISTS policies;
