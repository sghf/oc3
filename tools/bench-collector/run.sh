#!/bin/sh
# Runs bench.py against the dev stack (dev/svc/oc3) and collects, in $OUT:
# the summary, every request (requests.jsonl), the containers' cpu and memory
# every 5s (stats.txt), and the MariaDB queries slower than 0.5s (slow.log).
#
#   ./run.sh <name> [bench.py options]
#   ./run.sh users6 --user mgr:bench-mgr1@example.com:bench --duration 300
set -e
NAME=$1
shift
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT_DIR:-$HERE/results}/$NAME
mkdir -p "$OUT"

dbsql() { sudo -n docker exec -i dev..oc3.container.db sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD" opensvc'; }

# The test data alive, the slow log empty.
"$HERE/../seed-testdata/seed_testdata.py" refresh | dbsql
echo "SET GLOBAL slow_query_log = 0; SET GLOBAL slow_query_log_file = '/var/lib/mysql/bench-slow.log';
SET GLOBAL long_query_time = 0.5; SET GLOBAL log_slow_verbosity = 'query_plan,explain';" | dbsql
sudo -n docker exec dev..oc3.container.db sh -c ': > /var/lib/mysql/bench-slow.log'
echo "SET GLOBAL slow_query_log = 1;" | dbsql

( while :; do
    date +%T
    sudo -n docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' | grep dev..oc3
    sleep 5
  done ) > "$OUT/stats.txt" 2>&1 &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null || true' EXIT

PID=$(sudo -n docker inspect -f '{{.State.Pid}}' dev..oc3.container.server)
sudo -n nsenter -t "$PID" -n python3 "$HERE/bench.py" --out "$OUT/requests.jsonl" "$@" | tee "$OUT/summary.txt"

kill $SAMPLER 2>/dev/null || true
echo "SET GLOBAL slow_query_log = 0;" | dbsql
sudo -n docker exec dev..oc3.container.db cat /var/lib/mysql/bench-slow.log > "$OUT/slow.log"
# Queries the clients gave up on, still running.
echo "SELECT ID, TIME, LEFT(INFO, 200) FROM information_schema.PROCESSLIST WHERE COMMAND = 'Execute' AND TIME > 5;" \
  | dbsql > "$OUT/leftover.txt"
echo "results in $OUT"
