-- 2026-10-07: the rules translating the claims of an OpenID Connect identity into
-- access and teams. A rule matches when the claim (a name, or a dotted path into a
-- nested claim) equals the value, or, for a list, contains it; one rule per claim
-- and value. allow_access 'T': once at least one such rule exists, only the
-- identities matching one may sign in, and those may have their account created at
-- their first sign-in. auth_oidc_mapping_groups: the teams a rule grants, as many
-- as needed; the teams named by a rule follow the claims at every sign-in. Read by
-- oc3 at every OIDC sign-in and for the Bearer tokens, written from the Claim
-- mappings page (Manager only).
CREATE TABLE IF NOT EXISTS auth_oidc_mappings (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  claim        VARCHAR(128) NOT NULL,
  value        VARCHAR(255) NOT NULL,
  allow_access CHAR(1) NOT NULL DEFAULT 'F',
  author       VARCHAR(100) NULL DEFAULT NULL,
  updated      DATETIME NOT NULL,
  UNIQUE KEY uk_claim_value (claim, value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
