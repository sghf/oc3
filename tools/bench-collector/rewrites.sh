#!/bin/sh
# Times each statement of rewrites.sql alone (60s at most), on the dev stack.
HERE=$(cd "$(dirname "$0")" && pwd)
dbsql() { sudo -n docker exec -i dev..oc3.container.db sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD" opensvc -N'; }
NODES=$(echo "SELECT GROUP_CONCAT(QUOTE(node_id)) FROM nodes WHERE cluster_id = (SELECT cluster_id FROM nodes GROUP BY cluster_id HAVING COUNT(*) = 3 LIMIT 1)" | dbsql)
NODE=$(grep "^SET @node" "$HERE/rewrites.sql")
grep -v '^SET @node' "$HERE/rewrites.sql" | sed "s/@nodes/$NODES/g; s/!= @n\b/!= 3/g" | while IFS= read -r line; do
  case "$line" in
    "-- @@ "*) echo; echo "${line#-- @@ }" ;;
    "-- @"*) label=${line#-- @} ;;
    SELECT*)
      start=$(date +%s.%N)
      rows=$(printf '%s\nSET STATEMENT max_statement_time=60 FOR %s\n' "$NODE" "$line" | dbsql 2>&1 | wc -l)
      end=$(date +%s.%N)
      printf '  %7.2f s  %5s rows  %s\n' "$(echo "$end - $start" | bc)" "$rows" "$label" ;;
  esac
done
