-- 2026-10-21: a failed step of an orchestration run may be acknowledged, its
-- failure accepted with a comment, for the run to go on with the next step:
-- the status acknowledged, who acknowledged it, when and why.
ALTER TABLE orchestration_run_steps
  MODIFY status ENUM('waiting','running','succeeded','failed','acknowledged','stopped','not_run') NOT NULL,
  ADD COLUMN IF NOT EXISTS acked_by VARCHAR(255) NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS acked_at DATETIME NULL,
  ADD COLUMN IF NOT EXISTS ack_comment TEXT NULL;
