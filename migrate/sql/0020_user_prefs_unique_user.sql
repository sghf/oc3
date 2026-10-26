-- 2026-10-26: one row of preferences per user, which the saves now rely on: an
-- INSERT ... ON DUPLICATE KEY UPDATE replaces a read then a write, which two
-- first saves of a user, sent together by the frontend, both made as an insert,
-- the second answered 500 by the unique key, or a second row without it. Some
-- collector databases have that key (uk_user_id, from the historical schema),
-- none of the migrations creates it.
--
-- The duplicates go first, the latest row of each user kept, the one the saves
-- updated last: the key cannot be created over them. A data statement before the
-- DDL, against the rule of one DDL first: the migration is recorded once both
-- ran, and the deletion leaves nothing to delete when run again, so a failure of
-- the key is fixed by running the migration again.
DELETE older FROM user_prefs older
  JOIN user_prefs newer ON newer.user_id = older.user_id AND newer.id > older.id;
ALTER TABLE user_prefs ADD UNIQUE INDEX IF NOT EXISTS uk_user_id (user_id);
