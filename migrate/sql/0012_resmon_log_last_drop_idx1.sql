-- 2026-10-14: idx1 (node_id, svc_id) of resmon_log_last is a prefix of its unique
-- key uk (node_id, svc_id, rid), which serves every query idx1 would: it only
-- cost a write at each change of a row, and the table is mostly updated.
ALTER TABLE resmon_log_last DROP INDEX IF EXISTS idx1, ALGORITHM=INPLACE, LOCK=NONE;
