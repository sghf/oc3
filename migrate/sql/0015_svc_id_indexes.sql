-- 2026-10-21: an index on svc_id for the tables a service deletion
-- (DeleteServiceCascade) empties of the rows of the service, which had none
-- leading with it: each deletion scanned them whole. resmon_log, the history of
-- the resources, the largest, took 28 of the 35 ms of a deletion in dev, more
-- with the volumes of production. Built in place, the tables kept writable
-- meanwhile.
ALTER TABLE resmon_log ADD INDEX IF NOT EXISTS k_svc_id (svc_id), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE resmon_log_last ADD INDEX IF NOT EXISTS k_svc_id (svc_id), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE form_output_results ADD INDEX IF NOT EXISTS k_svc_id (svc_id), ALGORITHM=INPLACE, LOCK=NONE;
