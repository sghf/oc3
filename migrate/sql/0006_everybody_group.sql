-- 2026-10-07: the Everybody group, that every account belongs to. The historical
-- collector expects it (immutable group, user_add_evt trigger of auth_user adding
-- each new account to it), but never creates it: a database built from the
-- collector dump has none, and the trigger then leaves memberships with a NULL
-- group. Data rather than schema; idempotent. The memberships left with a NULL
-- group are given to Everybody, and an account without the membership gets it.
INSERT INTO auth_group (role, description, privilege)
  SELECT 'Everybody', 'Every user of the collector', 'F' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM auth_group WHERE role = 'Everybody');
UPDATE auth_membership
  SET group_id = (SELECT id FROM auth_group WHERE role = 'Everybody')
  WHERE group_id IS NULL;
INSERT IGNORE INTO auth_membership (user_id, group_id, primary_group)
  SELECT auth_user.id, (SELECT id FROM auth_group WHERE role = 'Everybody'), 'F'
  FROM auth_user
  WHERE NOT EXISTS (
    SELECT 1 FROM auth_membership m JOIN auth_group g ON g.id = m.group_id
    WHERE m.user_id = auth_user.id AND g.role = 'Everybody');
