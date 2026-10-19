-- 2026-10-13: the safe files encrypted at rest (AES-256-GCM, a per-file key
-- wrapped by a master key of the safe keys file). NULL enc_version: a plaintext
-- file, as the historical collector stores them. enc_uuid is the stored name the
-- encryption applies to: a row only counts as encrypted while it holds that same
-- file, so a new version uploaded by the historical collector, which does not know
-- these columns, reads as plaintext. md5 stays the one of the plaintext, which the
-- agents compare with their copy; sha256 is added beside it.
ALTER TABLE safe
  ADD COLUMN IF NOT EXISTS enc_version TINYINT NULL DEFAULT NULL
    COMMENT 'NULL: plaintext, 1: AES-256-GCM envelope',
  ADD COLUMN IF NOT EXISTS enc_uuid VARCHAR(512) NULL DEFAULT NULL
    COMMENT 'stored name the encryption applies to',
  ADD COLUMN IF NOT EXISTS enc_key_id VARCHAR(64) NULL DEFAULT NULL
    COMMENT 'id of the master key wrapping the file key',
  ADD COLUMN IF NOT EXISTS enc_wrapped_key VARBINARY(128) NULL DEFAULT NULL
    COMMENT 'file key wrapped by the master key',
  ADD COLUMN IF NOT EXISTS sha256 CHAR(64) NULL DEFAULT NULL
    COMMENT 'sha256 of the plaintext';
