#!/usr/bin/env python3
"""Groups the queries of a MariaDB slow log (log_slow_verbosity=query_plan,explain)
by shape, the slowest in total first: count, total and max time, rows examined,
the first table of the plan and the start of the query.

    ./slowlog.py results/<run>/slow.log [--top 20] [--sql 600]
"""

import argparse
import re


def shape(sql):
    s = re.sub(r"'[^']*'", "?", sql)
    s = re.sub(r"\b\d+\b", "N", s)
    s = re.sub(r"\(\?(, \?)*\)", "(?…)", s)
    return re.sub(r"\s+", " ", s).strip()


def entries(path):
    cur = None
    for line in open(path, errors="replace"):
        if line.startswith("# Time:") or line.startswith("# User@Host:"):
            if cur and cur["sql"]:
                yield cur
            if line.startswith("# User@Host:"):
                cur = {"time": 0, "rows": 0, "plan": [], "sql": ""}
            continue
        if cur is None:
            continue
        m = re.match(r"# Query_time: ([\d.]+).*Rows_examined: (\d+)", line)
        if m:
            cur["time"], cur["rows"] = float(m.group(1)), int(m.group(2))
        elif line.startswith("# explain:"):
            cur["plan"].append(line[10:].strip().split("\t"))
        elif line.startswith("#") or line.startswith("SET timestamp") or line.startswith("use "):
            continue
        else:
            cur["sql"] += line
    if cur and cur["sql"]:
        yield cur


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--sql", type=int, default=500)
    args = parser.parse_args()
    groups = {}
    for e in entries(args.log):
        g = groups.setdefault(shape(e["sql"]), {"n": 0, "t": 0, "max": 0, "rows": 0, "e": e})
        g["n"] += 1
        g["t"] += e["time"]
        g["max"] = max(g["max"], e["time"])
        g["rows"] += e["rows"]
    total = sum(g["t"] for g in groups.values())
    print("%d queries over 0.5s, %.0f s in all" % (sum(g["n"] for g in groups.values()), total))
    for i, (sh, g) in enumerate(sorted(groups.items(), key=lambda kv: -kv[1]["t"])[:args.top]):
        plan = [p for p in g["e"]["plan"] if p and p[0] != "id"]
        first = " > ".join("%s(%s%s)" % (p[2], p[3], "" if p[5] in ("NULL", "") else ":" + p[5])
                           for p in plan[:4] if len(p) > 5)
        print("\n#%d  %d x, %.1f s total, max %.1f s, %.0f rows examined avg (%.0f%% of the slow time)" % (
            i + 1, g["n"], g["t"], g["max"], g["rows"] / g["n"], 100 * g["t"] / total))
        print("   plan: %s" % first)
        print("   " + re.sub(r"\s+", " ", g["e"]["sql"]).strip()[:args.sql])


if __name__ == "__main__":
    main()
