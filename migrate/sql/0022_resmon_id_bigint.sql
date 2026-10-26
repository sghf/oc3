-- 2026-10-26: resmon.id becomes a BIGINT UNSIGNED. A signed int ends at
-- 2,147,483,647, and the feeder consumed an auto-increment value for each
-- resource of each instance status it wrote with INSERT ... ON DUPLICATE KEY
-- UPDATE, the rows only updated included: ~4,300 a minute in dev, 371 million
-- consumed. At the end of the range no resource can be inserted any more. The
-- feeder now only refreshes the instances unchanged; the wider type keeps the
-- counter clear of the end anyway. The counter keeps its value. A change of
-- type rebuilds the table, writes locked meanwhile: one row per resource, a
-- short rebuild. Run again, the statement rebuilds it to the same type.
ALTER TABLE resmon MODIFY id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT;
