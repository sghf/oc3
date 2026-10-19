-- 2026-10-06: the kind of an svcactions row, as the historical collector declares
-- it (init/models/db.py): 'status' for an action, begun or ended, '' for a log line
-- of an action. oc3 writes it with every action, and the actions of a service
-- (GET /services/{svc_id}/actions) are read by it; a database created before the
-- column existed refuses every action the agents report ("Unknown column
-- 'log_type'"). IF NOT EXISTS: the column is already there in a collector
-- database kept up to date by web2py.
ALTER TABLE svcactions
  ADD COLUMN IF NOT EXISTS log_type VARCHAR(30) DEFAULT '';
