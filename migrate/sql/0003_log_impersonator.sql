-- 2026-10-06: the user who really signed in when an action is made as another
-- user (impersonation). log_user names the user the action is made as; NULL here
-- when nobody is impersonated. Written by oc3 on every audit entry of an
-- impersonated request.
ALTER TABLE log
  ADD COLUMN IF NOT EXISTS log_impersonator VARCHAR(100) NULL DEFAULT NULL
    COMMENT 'user who really signed in, when acting as log_user'
    AFTER log_user;
