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

## A collector after years of service

`activity` adds, to the objects `up` loaded, the activity of a long service
life: the history of the tables the scheduler trims (`--retention`, 365 days,
its default retention), and the inventory the agents push. The database
generates the rows itself (`INSERT … SELECT` over the sequence engine of
MariaDB, the random draws hashes of the object and the day), so that tens of
millions of rows cost no transfer; the same load gives the same rows.

For a benchmark at the scale of 3,000 nodes, 15,000 services and 10 resources a
service on average, two years of service:

```sh
./seed_testdata.py up --nodes 3000 --services 15000 --resources 13 --days 365 --no-history | dbsql
./seed_testdata.py activity --days 730 | dbsql
```

`--no-history` leaves the actions, the logs and the status history to
`activity`. About 33 million rows, 50 minutes on 2 CPUs:

| Table | Rows | Content |
|---|---|---|
| `svcmon`, `resmon`, `resmon_log_last` | 43,000, 410,000 | 14 instances a node, 10.7 resources a service |
| `resinfo` | ~2 M | the keys the agents push about each resource |
| `packages`, `patches` | ~2 M | 300 to 1,050 packages a node, versions by OS |
| `node_users`, `node_groups`, `node_hw`, `node_hba` | ~430,000 | accounts, hardware, FC adapters |
| `svcactions` | ~4.5 M | 0.5 action a day per `svc` or `vol` object, with its log line, a year |
| `log` | ~2.2 M | collector log, 1 a day per node, 0.2 per service, a year |
| `services_log`, `svcmon_log`, `resmon_log` | ~5 M | contiguous status periods, each opened by an incident |
| `dashboard_events`, `dashboard_ref` | ~160,000 | alerts raised and cleared over the year |
| `comp_*` | ~3.6 M | modulesets attached to every node, a weekly check, its daily summary |
| `saves`, `saves_last` | ~1.1 M | a nightly backup of every node |
| `stat_day_svc`, `stat_day_disk_*` | ~11 M | daily statistics over the whole service life: never trimmed |
| `stor_array`, `stor_array_dg`, `diskinfo`, `svcdisks` | ~110,000 | 12 arrays, the disks of the services on them |
| `hbmon`, `hbmon_log`, `hbmon_log_last` | ~330,000 | heartbeats between the nodes of each cluster |

Options of `activity` (`./seed_testdata.py activity --help`) set the rates:
actions, logs and alerts a day or a month, compliance interval, arrays. It
loads by statements of `--chunk-days` of history or of a `--parts` share of
the objects, each committed: interrupted, `down` removes what it loaded. It
refuses to run twice on the same load. Its large inserts go faster with a
larger `innodb_buffer_pool_size` and `innodb_flush_log_at_trx_commit = 2`, and
with the scheduler stopped (see below), for the time of the load.

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

Options of `up` (`./seed_testdata.py up --help`): `--clusters` or `--nodes`, `--services`,
`--resources` per svc object, `--no-history`,
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
  other objects are never matched. It removes by batches, each committed: run
  again, it removes what an interrupted removal left. Then it removes what
  `activity` loaded under names of its own: storage arrays, modulesets, and
  their statistics.
- The alerts loaded name a test object in their instance (`sd:…`), so that their
  references are the test data's alone.

## Keeping the data alive

oc3's scheduler scrubs the instances and resources whose agent has not reported
for about 20 minutes, and raises "not updated" alerts on stale nodes and
services. A load taking longer than that (`activity` at scale) goes with the
scheduler stopped, `refresh` run before it starts again. For as long as the
data must look alive, run `refresh` every few minutes:

```sh
while sleep 300; do ./seed_testdata.py refresh | dbsql; done
```

Without it, the test nodes and services stay, with their history, but lose
their instances and resources after a while, as nodes that stopped reporting
would. `down` removes the alerts and logs the scheduler raised about them.
