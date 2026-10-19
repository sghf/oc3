-- 2026-10-08: the memberships the claim rules granted. A team a rule names
-- follows the claims; once no rule names it any more, the memberships recorded
-- here are revoked (when the rule changes, and at the next OIDC sign-in), while
-- those added by hand stay. Seeded with the current memberships of the teams the
-- rules name, of the accounts linked to an identity: those already followed the
-- claims. The seed only fills an empty table: run again over memberships the
-- rules already track, it would also record those added by hand since, which a
-- rule change would then revoke.
CREATE TABLE IF NOT EXISTS auth_oidc_memberships (
  user_id  INT NOT NULL,
  group_id INT NOT NULL,
  PRIMARY KEY (user_id, group_id),
  KEY k_group (group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
INSERT IGNORE INTO auth_oidc_memberships (user_id, group_id)
  SELECT DISTINCT am.user_id, am.group_id FROM auth_membership am
  JOIN auth_oidc_mapping_groups mg ON mg.group_id = am.group_id
  WHERE EXISTS (SELECT 1 FROM auth_user_identities i WHERE i.user_id = am.user_id)
    AND NOT EXISTS (SELECT 1 FROM auth_oidc_memberships);
