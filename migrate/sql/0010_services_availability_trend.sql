-- 2026-10-12: the trend of the availability rate of a service, in points: its
-- 30-day rate now minus the same rate 7 days ago, so that the Services view says
-- whether availability improves or deteriorates. The scheduler refreshes it with
-- svc_availability (task services_availability). NULL: no status history 7 days
-- ago.
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS svc_availability_trend DECIMAL(9,6) NULL DEFAULT NULL
    COMMENT 'svc_availability now minus 7 days ago, in points'
    AFTER svc_availability;
