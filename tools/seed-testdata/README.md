# seed-testdata

Realistic test data for an oc3 collector database, removable at any time
without touching the rest of the database.

`seed_testdata.py` writes SQL on stdout and never connects to the database
itself: pipe its output into the `mysql` client.

## What it loads

At the defaults (`--clusters 500 --services 10000`, 60 days of history), about
625,000 rows in 120 MB of SQL:

| Table | Rows | Content |
|---|---|---|
| `apps` | 35 | applications `SD-ERP`, `SD-CRM`… with domain and ops team |
| `clusters` | 500 | one application and environment each, on one of 7 sites |
| `nodes` | ~1,500 | 1 to 6 per cluster: hardware, OS, agent version, location, warranty |
| `node_ip` | ~3,800 | 2 or 3 addresses per node |
| `checks_live` | ~21,000 | file systems, links, volume groups, multipaths; a few out of bounds |
| `services` | 10,000 | `svc`, `vol`, `cfg`, `sec`; failover or flex; configuration included |
| `svcmon` | ~30,000 | an instance per service and node of its cluster |
| `resmon` | ~140,000 | ip, fs, disk, app, container, sync resources |
| `svcactions` | ~68,000 | agent actions and their log lines, mostly ok, some failed, some acknowledged |
| `log` | ~83,000 | collector log of the nodes and services |
| `services_log`, `svcmon_log`, `resmon_log` (+ `_last`) | ~250,000 | availability and status history |
| `dashboard` | ~3,700 | alerts on the services down or degraded and the checks out of bounds |

Every name starts with the prefix (`sd` by default): `sd-par-erp-prd-017n2`,
`erp/svc/sd-api-00017`. The same `--prefix` and `--seed` give the same data.
Dates are relative to the time of the load.

## Usage

```sh
./seed_testdata.py up      | mysql opensvc    # load
./seed_testdata.py status  | mysql opensvc    # count what is loaded
./seed_testdata.py refresh | mysql opensvc    # date the live state now
./seed_testdata.py down    | mysql opensvc    # remove the test data, and only it
```

On the dev stack (`dev/svc/oc3`):

```sh
dbsql() { docker exec -i dev..oc3.container.db sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD" opensvc'; }
./seed_testdata.py up | dbsql
```

Options of `up` (`./seed_testdata.py up --help`): `--clusters`, `--services`,
`--days` of history, `--actions` per service, `--node-logs`, `--service-logs`,
`--resource-history`, `--prefix`, `--seed`, and `--group TEAM` (repeatable)
to make an existing team responsible for the test applications, so that its
members, and not only the Managers, see the test nodes and services.

## Safety

- `up` applies in one transaction, all or nothing. It refuses to run again
  while a previous load is in place: the application names, and the ids it
  records, would collide.
- Every cluster, node, service and application loaded is recorded in the table
  `zz_seed_registry`.
- `down` removes, from **every** table having a `node_id` or `svc_id` column,
  the rows of the nodes and services recorded there, so also what oc3 wrote
  about them after the load (alerts, logs, history, attachments). Then it
  removes the alert references (`dashboard_ref`, shared by alerts of the same
  type and instance) that no remaining alert uses, the clusters and the
  applications recorded, their responsible teams, and the registry. Rows of
  other objects are never matched. It applies in one transaction too.
- The alerts loaded name a test object in their instance (`sd:…`), so that their
  references are the test data's alone.

## Keeping the data alive

oc3's scheduler scrubs the instances and resources whose agent has not reported
for about 20 minutes, and raises "not updated" alerts on stale nodes and
services. For as long as the data must look alive, run `refresh` every few
minutes:

```sh
while sleep 300; do ./seed_testdata.py refresh | dbsql; done
```

Without it, the test nodes and services stay, with their history, but lose
their instances and resources after a while, as nodes that stopped reporting
would. `down` removes the alerts and logs the scheduler raised about them.
