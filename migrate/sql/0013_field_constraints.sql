-- 2026-10-16: the values allowed for some attributes the users set, per kind of
-- object and attribute: in "suggest" mode the detail panels offer them in a
-- dropdown, still accepting another value; in "strict" mode the api refuses any
-- other, except clearing the attribute or keeping the value it holds; in "regex"
-- mode the value must match the whole of pattern (RE2 syntax), with the same
-- exceptions. Only the constrained attributes have a row: one without is free
-- text, the default. values_json is the ordered list of the values, in display
-- order, empty in "regex" mode; pattern is empty in the other modes.
CREATE TABLE IF NOT EXISTS field_constraints (
  object_kind VARCHAR(16) NOT NULL,
  field VARCHAR(64) NOT NULL,
  mode ENUM('suggest','strict','regex') NOT NULL,
  values_json LONGTEXT NOT NULL,
  pattern VARCHAR(1024) NOT NULL DEFAULT '',
  updated DATETIME NOT NULL,
  updated_by VARCHAR(255) NOT NULL DEFAULT '',
  PRIMARY KEY (object_kind, field)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
