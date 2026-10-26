-- 2026-10-26: the connectors of the forms, the external APIs their inputs and
-- rest outputs call through oc3, never from the browser. A Manager declares each
-- one: its base url, the methods and path prefixes the forms may use, the
-- private networks it may resolve to, its timeout and answer size, how it
-- authenticates. The secrets (password, token, api key, client secret, client
-- key) are one JSON object sealed with the safe master key, as the safe files
-- are: content, master key id and wrapped per-record key.
CREATE TABLE IF NOT EXISTS form_connectors (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  description TEXT NOT NULL,
  base_url VARCHAR(512) NOT NULL,
  methods VARCHAR(64) NOT NULL DEFAULT 'GET',
  paths_json TEXT NOT NULL,
  networks_json TEXT NOT NULL,
  headers_json TEXT NOT NULL,
  timeout_s INT NOT NULL DEFAULT 10,
  max_bytes INT NOT NULL DEFAULT 5242880,
  auth_type VARCHAR(32) NOT NULL DEFAULT 'none',
  auth_json TEXT NOT NULL,
  tls_json TEXT NOT NULL,
  secrets MEDIUMBLOB NULL,
  secrets_key_id VARCHAR(64) NULL,
  secrets_wrapped_key VARBINARY(512) NULL,
  updated_by VARCHAR(255) NOT NULL DEFAULT '',
  created DATETIME NOT NULL,
  updated DATETIME NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_form_connectors_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- The groups a connector is published to: a form may call it when one of the
-- groups responsible for the form is among them.
CREATE TABLE IF NOT EXISTS form_connector_groups (
  connector_id INT NOT NULL,
  group_id INT NOT NULL,
  PRIMARY KEY (connector_id, group_id),
  KEY k_form_connector_groups_group (group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
