-- Only the demonstration rows, and only if they still look like the ones this
-- migration inserted. A tenant an operator has since repointed at another
-- policy is theirs, not ours.
DELETE FROM tenants WHERE id IN ('acme', 'globex') AND name LIKE '%(demo)';
DELETE FROM policy_limits WHERE policy_name IN ('free', 'pro');
DELETE FROM policies WHERE name IN ('free', 'pro') AND description LIKE 'Demonstration policy:%';
