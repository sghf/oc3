-- 2026-10-07: the OpenID Connect identities of the users. A user signing in
-- through an OIDC provider is known by the pair (issuer, subject) the provider
-- gives, never by the email, which changes and can be reassigned. oc3 reads it at
-- every OIDC sign-in and for every Bearer access token, and writes it when an
-- identity is first linked to an auth_user account. A table of its own rather than
-- columns of auth_user, which the historical collector shares.
CREATE TABLE IF NOT EXISTS auth_user_identities (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  user_id    INT NOT NULL,
  issuer     VARCHAR(255) NOT NULL,
  subject    VARCHAR(255) NOT NULL,
  created    DATETIME NOT NULL,
  last_login DATETIME NULL DEFAULT NULL,
  UNIQUE KEY uk_identity (issuer, subject),
  KEY k_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
