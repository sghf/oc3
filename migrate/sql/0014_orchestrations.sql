-- 2026-10-20: the orchestrations, the "workflows" of the Orchestration pages
-- (not those of the request forms, which the workflows table holds). An
-- orchestration is an ordered list of steps, each one agent action on a set of
-- targets (services, instances, nodes or clusters), picked one by one or
-- matched by a filterset; steps_json holds them, as the api reads and writes
-- them.
CREATE TABLE IF NOT EXISTS orchestrations (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  description TEXT NOT NULL,
  steps_json LONGTEXT NOT NULL,
  created DATETIME NOT NULL,
  updated DATETIME NOT NULL,
  updated_by VARCHAR(255) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY uk_orchestrations_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- A run of an orchestration, the plan it ran frozen in plan_json: editing or
-- deleting the orchestration afterwards changes neither. The filtersets are
-- resolved when the run starts; the targets they then matched are those of
-- orchestration_run_targets. The api server advances the running runs, step
-- after step, and stops a run at the first step that fails.
CREATE TABLE IF NOT EXISTS orchestration_runs (
  id INT NOT NULL AUTO_INCREMENT,
  orchestration_id INT NULL,
  name VARCHAR(255) NOT NULL,
  plan_json LONGTEXT NOT NULL,
  status ENUM('running','succeeded','failed','stopped') NOT NULL,
  current_step INT NOT NULL DEFAULT 0,
  started DATETIME NOT NULL,
  ended DATETIME NULL,
  user_id INT NULL,
  started_by VARCHAR(255) NOT NULL DEFAULT '',
  message TEXT NOT NULL,
  PRIMARY KEY (id),
  KEY k_orchestration_runs_orchestration (orchestration_id),
  KEY k_orchestration_runs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- The steps of a run, in the order of the plan: step is the index in plan_json.
CREATE TABLE IF NOT EXISTS orchestration_run_steps (
  run_id INT NOT NULL,
  step INT NOT NULL,
  status ENUM('waiting','running','succeeded','failed','stopped','not_run') NOT NULL,
  started DATETIME NULL,
  ended DATETIME NULL,
  PRIMARY KEY (run_id, step)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- A target of a step of a run, and the agent action queued for it. The results
-- are copied from action_queue, which purges its done entries after a day.
CREATE TABLE IF NOT EXISTS orchestration_run_targets (
  id INT NOT NULL AUTO_INCREMENT,
  run_id INT NOT NULL,
  step INT NOT NULL,
  position INT NOT NULL,
  node_id CHAR(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  svc_id CHAR(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  cluster_id CHAR(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  label VARCHAR(512) NOT NULL DEFAULT '',
  status ENUM('waiting','queued','running','ok','failed','refused','timed_out','cancelled','not_run') NOT NULL,
  action_id INT NULL,
  command TEXT NULL,
  ret INT NULL,
  stdout MEDIUMTEXT NULL,
  stderr MEDIUMTEXT NULL,
  message TEXT NOT NULL,
  queued DATETIME NULL,
  started DATETIME NULL,
  ended DATETIME NULL,
  PRIMARY KEY (id),
  KEY k_orchestration_run_targets_step (run_id, step, position),
  KEY k_orchestration_run_targets_action (action_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
