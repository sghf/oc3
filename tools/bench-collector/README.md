# bench-collector

Load test of the oc3 API: a few users browsing the collector at the same time,
with the requests of oc3-frontend.

## Scenario

Each virtual user signs in (Basic), loads the application shell (the requests
of `AppShell`), then browses until the end of the run, a think time between
two actions:

| Action | Weight | Requests |
|---|---|---|
| open a list view | 30 | its default request (columns, sort, 50 rows + 1), and the extra requests of the view (alerts counts, compliance summary, saves summaries) |
| next page, sort by a column, type a column filter | 8, 5, 12 | the list request again |
| open the value distribution of a column | 6 | `?props=<col>&stats=1&limit=20` |
| global search | 8 | `/search?q=…` (with a kind prefix sometimes) |
| open a node / a service | 14, 14 | the panel and the badges of its tabs, 6 at a time as a browser |
| open a node or service tab | 6, 4 | actions, storage, packages, stats, compliance, node differences |

The views, weighted by use: alerts, services, nodes, service actions,
instances, logs, resources, checks, packages, compliance status and logs,
saves, disks, action queue. Every 60s the action queue badge is polled. The
requests are those of `oc3-frontend/src/features/*` (paths, props, sort,
filter syntax): see `bench.py`.

## Running it

Users of the collector, of different visibility: `bench-mgr1/2` (Manager, see
everything), `bench-ops1..3` (team `bench-ops`, responsible of 10 apps of the
test data), `bench-dba1` (team `bench-dba`, 3 apps). Password `bench`.

```sh
U="--user mgr:bench-mgr1@example.com:bench --user mgr:bench-mgr2@example.com:bench \
   --user ops:bench-ops1@example.com:bench --user ops:bench-ops2@example.com:bench \
   --user ops:bench-ops3@example.com:bench --user dba:bench-dba1@example.com:bench"
./run.sh baseline-6u $U --duration 300 --think 2 6 --timeout 60
./run.sh load-12u $U --per-user 2 --duration 300 --think 1 3 --timeout 60
```

`run.sh` refreshes the test data (`../seed-testdata`), runs `bench.py` in the
network namespace of the API server of `dev/svc/oc3` (127.0.0.1:8081), and
collects in `results/<name>/`: `summary.txt`, every request
(`requests.jsonl`), the cpu and memory of the containers (`stats.txt`), the
MariaDB queries over 0.5s with their plan (`slow.log`), and the queries still
running after the run, given up by their client (`leftover.txt`).

```sh
./slowlog.py results/<name>/slow.log    # the slow queries by shape, the costliest first
./rewrites.sh                           # times the rewrites of rewrites.sql, one by one
```
