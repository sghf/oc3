-- 2026-10-22: the creation dates of the forms and of the metrics were columns
-- "TIMESTAMP ... ON UPDATE current_timestamp()", the implicit definition MySQL
-- once gave the first TIMESTAMP of a table: every change of a form or of a
-- metric (PUT /forms/{id}, PUT /metrics/{id}, and the historical collector's
-- equivalents) moved its creation date to the time of the change. They keep
-- their default, the time of the insert, and no longer follow the updates.
-- The dates already moved stay as they are: nothing tells the original ones.
ALTER TABLE forms MODIFY form_created TIMESTAMP NOT NULL DEFAULT current_timestamp();
ALTER TABLE metrics MODIFY metric_created TIMESTAMP NOT NULL DEFAULT current_timestamp();
