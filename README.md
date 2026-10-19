[![Go](https://github.com/opensvc/oc3/actions/workflows/go.yml/badge.svg)](https://github.com/opensvc/oc3/actions/workflows/go.yml)

## configuration example
```yaml
db:
  password: xxxxxxx
  log:
    level: info
    slow_query_threshold: 500ms

feeder:
  addr: 127.0.0.1:8080
  pprof:
    ux.enable: true
    net.enable: true
  metrics:
    enable: true
  ui:
    enable: true

server:
  tx: true
  addr: 127.0.0.1:8081
  auth:
    # Users signing in with their collector password (HTTP Basic). Set to false
    # once everyone signs in through OpenID Connect (server.oidc), which it then
    # requires: passwords are no longer accepted nor set. Nodes keep their own
    # authentication.
    basic_users: true
  pprof:
    ux.enable: true
    net.enable: true
  metrics:
    enable: true
  ui:
    enable: true

scheduler:
  pprof:
    ux.socket: /oc3/scheduler.pprof
    ux.enable: true
    net.enable: true
  metrics:
    enable: true

messenger:
  url: http://0.0.0.0:8889
  key: magix123
  pprof:
    ux:
      enable: true
    net:
      enable: true
  metrics:
    enable: true

worker.slow:
  addr: 127.0.0.1:8100
  tx: true
  pprof:
    ux.enable: true
    net.enable: true
  runners: 3
  metrics:
    enable: true
  queues:
    - "daemon_status"
    - "daemon_ping"

worker.fast:
  addr: 127.0.0.1:8101
  tx: true
  pprof:
    ux.enable: true
    net.enable: true
  runners: 3
  metrics:
    enable: true
  queues:
    - "instance_resource_info"
    - "instance_status"
    - "instance_action"
    - "node_checks"
    - "node_disk"
    - "object_config"
    - "san_switch"
    - "system"
```

## database schema

oc3 runs on the database of the historical collector, created from its schema
dump, plus the schema changes of `migrate/sql/`, numbered and applied in their
order. The database records the ones applied in its `oc3_schema_migrations` table.

    oc3 db status              # the migrations, applied or pending; fails unless up to date
    oc3 db migrate --dry-run   # print the statements left to apply
    oc3 db migrate             # apply them

Run `oc3 db migrate` at each installation and upgrade, after a backup of the
database. It takes a MariaDB named lock, so it can run from several nodes or
containers at once. The subsystems wait up to `db.schema.wait` (5m) for it, then
refuse to start on a schema missing a migration (`db.schema.check: warn` only logs
it). They also refuse an applied migration changed since.

Every migration can run again on a database that already has its effect: a
database kept up to date by the historical collector, or by hand, is migrated the
same way as a new one. To add a schema change, add the next `NNNN_name.sql` file,
one DDL statement per file, written to run again (`IF NOT EXISTS`, `IF EXISTS`,
`INSERT IGNORE`...). Never edit a released migration: add a new one.

In an OpenSVC service, run it as a container of the oc3 image that is not
detached, along with the subsystems:

    [container#migrate]
    type = docker
    image = {env.oc3_image}
    netns = container#0
    detach = false
    start_timeout = 5m
    command = db migrate
    secrets_environment = OC3_DB_PASSWORD=oc3/db_password
    volume_mounts = volume#0:/etc/oc3:/etc/oc3:ro
    subset = api

## manual build docker image

    docker build \
        --build-arg BUILDTIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --build-arg VERSION="$(git describe --tags --abbrev)" \
        -t opensvc/oc3 .