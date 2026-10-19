-- 2026-10-02: the availability target of a service, in percent, which the
-- Services view compares with its availability rate. NULL: no SLA.
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS svc_sla DECIMAL(6,3) NULL DEFAULT NULL
  COMMENT 'availability target, in percent';
