-- 2026-10-07: the teams a claim rule grants (see auth_oidc_mappings).
CREATE TABLE IF NOT EXISTS auth_oidc_mapping_groups (
  mapping_id INT NOT NULL,
  group_id   INT NOT NULL,
  PRIMARY KEY (mapping_id, group_id),
  KEY k_group (group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
