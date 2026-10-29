#!/usr/bin/env python3
"""Load test of the oc3 API: users browsing the collector at the same time.

Each virtual user signs in (Basic), loads the application shell, then browses:
opens list views as oc3-frontend requests them (default columns, sort, page
size 50), pages, sorts, types column filters, opens the value distribution of
a column, searches, opens node and service detail panels (their tab badges
requested at once, 6 at a time like a browser), with a think time between
two actions. The action queue badge is polled every 60s, as the frontend does.

The requests are those of oc3-frontend (src/features/*/…Page.tsx,
*/related/queries.ts): see README.md.

    ./bench.py --url http://127.0.0.1:8081 --duration 300 \\
        --user mgr:bench-mgr1@example.com:bench --user ops:bench-ops1@example.com:bench ...

Writes a summary on stdout and every request in --out (JSON lines).
Python 3.8+, standard library only.
"""

import argparse
import base64
import concurrent.futures
import datetime
import json
import random
import re
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

LIMIT = 51  # page size 50 + 1, to know if a next page exists

VIEWS = {
    "nodes": ("/nodes", "node_id,node_frozen,nodename,serial,status,loc_building,loc_floor,loc_rack,cpu_dies,"
              "cpu_cores,cpu_model,mem_bytes,node_env,team_responsible", "nodename", []),
    "services": ("/services", "svc_id,svc_frozen,svc_status_updated,svcname,svc_status,svc_availstatus,"
                 "svc_provisioned,svc_placement,svc_ha,svc_topology,updated", "svcname", []),
    "instances": ("/services_instances", "svc_id,node_id,mon_vmname,mon_frozen,mon_updated,services.svcname,err,"
                  "services.svc_ha,services.svc_availstatus,services.svc_status,services.svc_app,services.svc_env,"
                  "services.svc_topology,mon_vmtype,nodes.node_env,nodes.nodename,mon_smon_status,mon_availstatus,"
                  "mon_overallstatus", "services.svcname,nodes.nodename", []),
    "resources": ("/resources", "id,svc_id,node_id,vmname,rid,services.svcname,nodes.nodename,res_type,res_status,"
                  "res_desc,res_log,res_monitor,res_disable,res_optional,updated",
                  "services.svcname,nodes.nodename,vmname,rid", []),
    "service_actions": ("/services_actions", "id,svc_id,node_id,cron,status,ack,services.svcname,nodes.nodename,"
                        "action,rid,begin,time", "-begin", ["log_type:eq:status"]),
    "action_queue": ("/actions", "id,status,ret,node_id,svc_id,command,nodes.nodename,services.svcname,date_queued,"
                     "date_dequeued", "-id", []),
    "logs": ("/logs", "id,log_date,log_level,services.svcname,nodes.nodename,log_user,log_action,log_fmt,log_dict,"
             "node_id,svc_id,log_impersonator", "-log_date,-id", []),
    "alerts": ("/alerts", "id,dash_severity,dash_type,services.svcname,dash_env,alert,dash_created,dash_updated,"
               "nodes.nodename,svc_id,node_id", "-dash_severity,dash_type", []),
    "checks": ("/checks/live", "id,node_id,svc_id,chk_type,chk_instance,nodes.nodename,services.svcname,chk_value,"
               "chk_low,chk_high,chk_err,chk_threshold_provider,chk_updated",
               "nodes.nodename,chk_type,chk_instance", []),
    "packages": ("/packages", "id,node_id,nodes.nodename,pkg_name,pkg_version,pkg_arch,pkg_type,sig_provider,"
                 "pkg_install_date,pkg_updated", "nodes.nodename,pkg_name,pkg_arch", []),
    "comp_status": ("/compliance/status", "id,run_date,nodes.nodename,services.svcname,run_module,run_status,"
                    "node_id,svc_id", "-run_date", []),
    "comp_logs": ("/compliance/logs", "id,run_date,nodes.nodename,services.svcname,run_module,run_action,"
                  "run_status,node_id,svc_id", "-id", []),
    "saves": ("/saves", "id,node_id,svc_id,save_server,save_app,nodename,svcname,save_name,save_group,save_level,"
              "save_size,save_volume,save_date,save_retention", "-save_date", []),
    "disks": ("/disks", "disk_id,disk_model,disk_size,disk_used,nodename,svcname,node_id,svc_id",
              "nodename,disk_id", []),
}

# Requests a view sends besides its list.
VIEW_EXTRA = {
    "alerts": ["/alerts?props=dash_severity&stats=true&limit=0"],
    "comp_status": ["/compliance/status/summary"],
    "saves": ["/saves/summary?by=app", "/saves/summary?by=server"],
}

# Weight of each step of a user's browsing.
STEPS = [
    ("view", 30), ("page", 8), ("sort", 5), ("filter", 12), ("stats", 6), ("search", 8),
    ("node_panel", 14), ("service_panel", 14), ("node_tab", 6), ("service_tab", 4),
]
VIEW_WEIGHTS = {
    "alerts": 14, "services": 14, "nodes": 14, "instances": 8, "service_actions": 10, "logs": 8, "resources": 5,
    "checks": 5, "packages": 4, "comp_status": 4, "comp_logs": 3, "saves": 3, "disks": 3, "action_queue": 5,
}
FILTERS = {
    "nodes": [("nodename", "{word}"), ("node_env", "in:PRD,PPRD"), ("os_name", "eq:Linux"), ("cpu_cores", "gte:32")],
    "services": [("svcname", "{word}"), ("svc_availstatus", "in:down,warn"), ("svc_status", "!up")],
    "instances": [("services.svcname", "{word}"), ("mon_availstatus", "eq:down")],
    "resources": [("res_status", "in:down,warn"), ("res_type", "eq:fs.xfs"), ("rid", "~^app")],
    "service_actions": [("status", "eq:err"), ("action", "eq:start"), ("services.svcname", "{word}")],
    "logs": [("log_level", "in:warning,error"), ("log_action", "{lword}"), ("nodes.nodename", "{word}")],
    "alerts": [("dash_type", "{aword}"), ("dash_severity", "gte:3")],
    "checks": [("chk_type", "eq:fs_u"), ("chk_err", "gte:1")],
    "packages": [("pkg_name", "{pword}"), ("nodes.nodename", "{word}")],
    "comp_status": [("run_status", "eq:1"), ("run_module", "{mword}")],
    "comp_logs": [("run_module", "{mword}"), ("run_status", "eq:1")],
    "saves": [("save_server", "{word}"), ("save_level", "eq:full")],
    "disks": [("svcname", "{word}"), ("disk_model", "{word}")],
    "action_queue": [("status", "eq:T")],
}
WORDS = ["erp", "crm", "web", "api", "db", "prd", "dev", "par", "fra", "lon", "pgsql", "kafka", "0001", "n2"]
LOG_WORDS = ["node.action", "service.status", "checks"]
ALERT_WORDS = ["check", "service", "action errors"]
PKG_WORDS = ["openssl", "kernel", "python3", "lib", "glibc"]
MOD_WORDS = ["ntp", "sshd", "users", "firewall"]
SEARCH_WORDS = ["erp", "web-01", "sd-par", "api", "kafka", "10.20", "pgsql", "svc: web", "node: fra", "app: sd"]

NODE_PANEL = [
    "/nodes/{n}/tags?orderby=tag_name&limit=0",
    "/nodes/{n}/am_i_responsible",
    "/groups?props=role,privilege&orderby=role&limit=0",
    "/apps?props=app&orderby=app&limit=0",
    "/nodes/{n}/services?props=svc_id,node_id,services.svcname,services.svc_ha,services.svc_topology,mon_vmtype,"
    "mon_vmname,mon_availstatus,mon_overallstatus,mon_frozen,mon_updated&orderby=services.svcname,mon_vmname&limit=0",
    "/services_actions?props=status&stats=1&filter=log_type:eq:status&filter=begin:gte:{d60}&filter=node_id:eq:{n}",
    "/nodes/{n}/hardware?props=id,hw_type,hw_path,hw_class,hw_description,hw_driver,updated&orderby=hw_type,"
    "hw_path&limit=0",
    "/nodes/{n}/ips?props=id,intf,type,addr,mask,mac,flag_deprecated,net_name,net_network,net_netmask,net_gateway,"
    "updated&limit=0",
    "/nodes/{n}/disks?props=disk_id,disk_name,disk_vendor,disk_model,disk_size,disk_used,disk_dg,disk_group,"
    "disk_raid,svc_id,svcname,app,updated&orderby=svcname,disk_id&limit=0",
    "/packages?props=id&limit=1&filter=node_id:eq:{n}",
    "/nodes/{n}/sysreport?begin={d7}&limit=1",
    "/nodes/{n}/alerts?props=id,dash_severity,dash_type,alert,dash_env,dash_created,dash_updated&orderby="
    "-dash_severity,-dash_updated&limit=0",
    "/nodes/{n}/checks?props=id,svc_id,services.svcname,chk_type,chk_instance,chk_value,chk_low,chk_high,chk_err,"
    "chk_threshold_provider,chk_updated&orderby=chk_type,chk_instance&limit=0",
    "/nodes/{n}/compliance/status?props=id,run_date,run_module,run_status,run_log,node_id,nodes.nodename,svc_id,"
    "services.svcname&orderby=run_module&limit=0",
    "/logs?props=id,log_date,log_level,svc_id,services.svcname,log_user,log_impersonator,log_action,log_fmt,"
    "log_dict&orderby=-log_date,-id&limit=101&filter=node_id:eq:{n}",
]
NODE_TABS = {
    "actions": ["/services_actions?props=id,svc_id,node_id,cron,status,ack,acked_by,acked_date,acked_comment,"
                "services.svcname,nodes.nodename,action,rid,subset,begin,end,time,status_log,sid,pid&limit=1001"
                "&filter=log_type:eq:status&filter=begin:gte:{d7}&filter=node_id:eq:{n}",
                "/services_actions?props=id,action,begin,services.svcname,nodes.nodename&limit=0&filter=log_type:eq:"
                "status&filter=status:err&filter=ack:!1&filter=node_id:eq:{n}"],
    "storage": ["/nodes/{n}/hbas?props=id,hba_id,hba_type,updated&orderby=hba_type,hba_id&limit=0",
                "/nodes/{n}/san"],
    "packages": ["/packages?props=id,pkg_name,pkg_version,pkg_arch,pkg_type,sig_provider,pkg_install_date,"
                 "pkg_updated&orderby=pkg_name,pkg_arch&limit=0&filter=node_id:eq:{n}"],
    "stats": ["/nodes/{n}/stats?group=%s&days=1" % g
              for g in ["cpu", "proc", "mem", "swap", "block", "netdev", "netdev_err", "blockdev", "fs", "svc"]],
    "compliance": ["/nodes/{n}/compliance/modulesets", "/nodes/{n}/compliance/rulesets"],
}
SERVICE_PANEL = [
    "/services/{s}/tags?orderby=tag_name&limit=0",
    "/services/{s}/am_i_responsible",
    "/field_constraints/service",
    "/services/{s}/status_log?days=7",
    "/services_instances/{s}?orderby=nodes.nodename,mon_vmname&limit=0",
    "/services/{s}/alerts?props=id,dash_severity,dash_type,alert,dash_env,dash_created,dash_updated&orderby="
    "-dash_severity,-dash_updated&limit=0",
    "/services/{s}/resinfo?props=id,node_id,rid,res_key,res_value,updated&orderby=node_id,rid,res_key&limit=0",
    "/services/{s}/resources?props=id,node_id,vmname,rid,res_type,res_status,res_desc,res_log,res_monitor,"
    "res_disable,res_optional,updated&orderby=node_id,vmname,rid&limit=0",
    "/services/{s}/disks?orderby=nodename,disk_id&limit=0",
    "/services/{s}/actions?props=status&stats=1&filter=log_type:eq:status&filter=begin:gte:{d60}",
    "/services/{s}/compliance/status?props=id,run_date,run_module,run_status,run_log,node_id,nodes.nodename,svc_id,"
    "services.svcname&orderby=run_module&limit=0",
    "/logs?props=id,log_date,log_level,svc_id,services.svcname,log_user,log_impersonator,log_action,log_fmt,"
    "log_dict&orderby=-log_date,-id&limit=101&filter=svc_id:eq:{s}",
    "/services_instances?props=node_id&limit=0&filter=svc_id:eq:{s}",
]
SERVICE_TABS = {
    "actions": ["/services/{s}/actions?props=id,node_id,cron,status,ack,nodes.nodename,action,rid,begin,end,time,"
                "status_log,sid&limit=1001&filter=log_type:eq:status&filter=begin:gte:{d7}"],
    "nodediff": [],
}
STARTUP = [
    "/auth/info", "/users/self?props=email", "/users/self/prefs", "/users/self/groups?props=role,privilege&limit=0",
    "/users/self/hidden_menu_entries", "/users/self/filterset", "/filtersets?props=fset_name&orderby=fset_name&limit=500",
    "/version", "/impersonation", "/actions?props=id,status&limit=200&orderby=-id",
]

ID_RE = re.compile(r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def encode(path):
    """Encodes the query as the frontend does: each value percent-encoded."""
    base, _, query = path.partition("?")
    if not query:
        return base
    parts = []
    for item in query.split("&"):
        k, _, v = item.partition("=")
        parts.append("%s=%s" % (k, urllib.parse.quote(v, safe="")))
    return base + "?" + "&".join(parts)


def template(path):
    """The request with its ids and filter values replaced, to aggregate by kind."""
    t = ID_RE.sub("/{id}", path)
    t = re.sub(r"filter=([a-z_.]+):[^&]*", r"filter=\1:…", t)
    t = re.sub(r"offset=\d+", "offset=N", t)
    t = re.sub(r"[?&]props=[^&]*", "", t)
    t = re.sub(r"q=[^&]*", "q=…", t)
    return t


class Recorder:
    def __init__(self, out):
        self.lock = threading.Lock()
        self.rows = []
        self.out = out

    def add(self, row):
        with self.lock:
            self.rows.append(row)
            if self.out:
                self.out.write(json.dumps(row) + "\n")


class User(threading.Thread):
    def __init__(self, idx, profile, email, password, args, rec, stop):
        super().__init__(daemon=True)
        self.idx = idx
        self.profile = profile
        self.auth = "Basic " + base64.b64encode(("%s:%s" % (email, password)).encode()).decode()
        self.args = args
        self.rec = rec
        self.stop = stop
        self.rng = random.Random(args.seed * 1000 + idx)
        self.pool = concurrent.futures.ThreadPoolExecutor(6)
        self.nodes, self.services = [], []
        self.last_poll = 0
        self.view = None
        self.state = {}

    def get(self, path, step, view=None):
        now = datetime.datetime.now()
        path = path.replace("{d60}", (now - datetime.timedelta(days=60)).strftime("%Y-%m-%d %H:00:00"))
        path = path.replace("{d7}", (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:00:00"))
        url = self.args.url + "/api" + encode(path)
        req = urllib.request.Request(url, headers={"Authorization": self.auth, "Accept": "application/json"})
        t0 = time.monotonic()
        status, size, body = 0, 0, None
        try:
            with urllib.request.urlopen(req, timeout=self.args.timeout) as resp:
                body = resp.read()
                status, size = resp.status, len(body)
        except urllib.error.HTTPError as e:
            status = e.code
            size = len(e.read() or b"")
        except Exception as e:  # timeout, connection reset
            status = "%s" % type(e).__name__
        dt = time.monotonic() - t0
        self.rec.add({"t": time.time(), "user": self.idx, "profile": self.profile, "step": step, "view": view,
                      "req": template(path), "path": path, "status": status, "ms": round(dt * 1000, 1),
                      "bytes": size})
        if body is not None:
            try:
                return json.loads(body)
            except ValueError:
                return None
        return None

    def parallel(self, paths, step, view):
        t0 = time.monotonic()
        list(self.pool.map(lambda p: self.get(p, step, view), paths))
        self.rec.add({"t": time.time(), "user": self.idx, "profile": self.profile, "step": step, "view": view,
                      "req": "<page:%s>" % step, "status": "page", "ms": round((time.monotonic() - t0) * 1000, 1),
                      "bytes": 0})

    def ids(self):
        data = self.get("/nodes?props=node_id&limit=0", "init")
        self.nodes = [r["node_id"] for r in (data or {}).get("data", [])]
        data = self.get("/services?props=svc_id&limit=0", "init")
        self.services = [r["svc_id"] for r in (data or {}).get("data", [])]

    def list_path(self, view, offset=0, sort=None, filters=()):
        path, props, orderby, fixed = VIEWS[view]
        q = "%s?props=%s&orderby=%s&offset=%d&limit=%d" % (path, props, sort or orderby, offset, LIMIT)
        for f in list(fixed) + list(filters):
            q += "&filter=" + f
        return q

    def fill(self, expr):
        return (expr.replace("{word}", self.rng.choice(WORDS)).replace("{lword}", self.rng.choice(LOG_WORDS))
                .replace("{aword}", self.rng.choice(ALERT_WORDS)).replace("{pword}", self.rng.choice(PKG_WORDS))
                .replace("{mword}", self.rng.choice(MOD_WORDS)))

    def open_view(self, view):
        self.view = view
        self.state = {"offset": 0, "sort": None, "filters": []}
        self.parallel([self.list_path(view)] + VIEW_EXTRA.get(view, []), "view", view)

    def step(self):
        kind = weighted(self.rng, STEPS)
        if kind == "view" or self.view is None:
            self.open_view(weighted(self.rng, list(VIEW_WEIGHTS.items())))
        elif kind == "page":
            self.state["offset"] += 50
            self.parallel([self.list_path(self.view, self.state["offset"], self.state["sort"], self.state["filters"])],
                          "page", self.view)
        elif kind == "sort":
            cols = VIEWS[self.view][1].split(",")
            col = self.rng.choice([c for c in cols if not c.endswith("_id") and c != "id"] or cols)
            self.state["sort"] = ("-" if self.rng.random() < 0.5 else "") + col
            self.state["offset"] = 0
            self.parallel([self.list_path(self.view, 0, self.state["sort"], self.state["filters"])], "sort", self.view)
        elif kind == "filter":
            prop, expr = self.rng.choice(FILTERS[self.view])
            self.state["filters"] = [f for f in self.state["filters"] if not f.startswith(prop + ":")]
            self.state["filters"].append("%s:%s" % (prop, self.fill(expr)))
            self.state["offset"] = 0
            self.parallel([self.list_path(self.view, 0, self.state["sort"], self.state["filters"])], "filter",
                          self.view)
        elif kind == "stats":
            path, props, _, fixed = VIEWS[self.view]
            prop = self.rng.choice([c for c in props.split(",") if not c.endswith("id")] or ["id"])
            q = "%s?props=%s&stats=1&limit=20" % (path, prop)
            for f in list(fixed) + self.state["filters"]:
                q += "&filter=" + f
            self.parallel([q], "stats", self.view)
        elif kind == "search":
            w = self.rng.choice(SEARCH_WORDS)
            if ":" in w:
                prefix, word = [x.strip() for x in w.split(":")]
                kinds = {"svc": "service", "node": "node", "app": "app"}[prefix]
                q = "/search?q=%s&kinds=%s&limit=20" % (word, kinds)
            else:
                q = "/search?q=%s&limit=5" % w
            self.parallel([q], "search", None)
        elif kind in ("node_panel", "node_tab") and self.nodes:
            n = self.rng.choice(self.nodes)
            if kind == "node_panel":
                paths = ["/nodes/%s" % n] + [p.replace("{n}", n) for p in NODE_PANEL]
            else:
                tab = self.rng.choice(list(NODE_TABS))
                paths = [p.replace("{n}", n) for p in NODE_TABS[tab]]
                kind = "node_tab:" + tab
            self.parallel(paths, kind, "node")
        elif kind in ("service_panel", "service_tab") and self.services:
            s = self.rng.choice(self.services)
            if kind == "service_panel":
                paths = ["/services/%s" % s] + [p.replace("{s}", s) for p in SERVICE_PANEL]
            else:
                tab = self.rng.choice(list(SERVICE_TABS))
                kind = "service_tab:" + tab
                if tab == "nodediff":
                    inst = self.get("/services_instances?props=node_id&limit=0&filter=svc_id:eq:%s" % s, kind,
                                    "service") or {}
                    nodes = [r.get("node_id") for r in inst.get("data", [])][:6]
                    paths = ["/packages/diff?node_ids=" + ",".join(nodes)] if len(nodes) > 1 else []
                else:
                    paths = [p.replace("{s}", s) for p in SERVICE_TABS[tab]]
            if paths:
                self.parallel(paths, kind, "service")

    def think(self):
        lo, hi = self.args.think
        end = time.time() + self.rng.uniform(lo, hi)
        while time.time() < end and not self.stop.is_set():
            time.sleep(min(0.2, max(0, end - time.time())))

    def run(self):
        time.sleep(self.rng.uniform(0, self.args.ramp))
        self.parallel(STARTUP, "startup", None)
        self.ids()
        self.last_poll = time.time()
        while not self.stop.is_set():
            if time.time() - self.last_poll >= 60:
                self.get("/actions?props=id,status&limit=200&orderby=-id", "poll")
                self.last_poll = time.time()
            self.step()
            self.think()


def weighted(rng, choices):
    total = sum(w for _, w in choices)
    pick = rng.uniform(0, total)
    for value, weight in choices:
        pick -= weight
        if pick <= 0:
            return value
    return choices[-1][0]


def pct(values, p):
    values = sorted(values)
    if not values:
        return 0
    k = (len(values) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    return values[f] + (values[c] - values[f]) * (k - f)


def summary(rows, duration, top):
    reqs = [r for r in rows if r["status"] != "page"]
    pages = [r for r in rows if r["status"] == "page"]
    errors = [r for r in reqs if r["status"] != 200]
    all_ms = [r["ms"] for r in reqs]
    print("requests %d (%.1f/s), errors %d, latency p50 %.0f ms, p95 %.0f ms, p99 %.0f ms, max %.0f ms" % (
        len(reqs), len(reqs) / duration, len(errors), pct(all_ms, 50), pct(all_ms, 95), pct(all_ms, 99),
        max(all_ms or [0])))
    print("\nuser actions (all their requests done):")
    print("  %-28s %6s %8s %8s %8s" % ("step", "count", "p50 ms", "p95 ms", "max ms"))
    by = {}
    for r in pages:
        key = r["step"] + ("" if r["step"] not in ("view", "page", "sort", "filter", "stats") else ":" + r["view"])
        by.setdefault(key, []).append(r["ms"])
    for key, ms in sorted(by.items(), key=lambda kv: -pct(kv[1], 95)):
        print("  %-28s %6d %8.0f %8.0f %8.0f" % (key, len(ms), pct(ms, 50), pct(ms, 95), max(ms)))
    print("\nslowest requests by kind (sorted by total time):")
    print("  %8s %6s %7s %7s %7s %6s  %s" % ("total s", "count", "p50", "p95", "max", "err", "request"))
    by = {}
    for r in reqs:
        by.setdefault(r["req"], []).append(r)
    ranked = sorted(by.items(), key=lambda kv: -sum(x["ms"] for x in kv[1]))
    for key, rs in ranked[:top]:
        ms = [x["ms"] for x in rs]
        print("  %8.1f %6d %7.0f %7.0f %7.0f %6d  %s" % (sum(ms) / 1000, len(ms), pct(ms, 50), pct(ms, 95), max(ms),
                                                        sum(1 for x in rs if x["status"] != 200), key[:150]))
    if errors:
        print("\nerrors:")
        es = {}
        for r in errors:
            es.setdefault((r["status"], r["req"]), 0)
            es[(r["status"], r["req"])] += 1
        for (st, req), n in sorted(es.items(), key=lambda kv: -kv[1])[:15]:
            print("  %5d  %-8s %s" % (n, st, req[:140]))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="http://127.0.0.1:8081")
    parser.add_argument("--user", action="append", required=True,
                        help="profile:email:password, repeatable; one virtual user each (see --per-user)")
    parser.add_argument("--per-user", type=int, default=1, help="virtual users per --user (default: 1)")
    parser.add_argument("--duration", type=int, default=300, help="seconds (default: 300)")
    parser.add_argument("--think", type=float, nargs=2, default=[2, 6], metavar=("MIN", "MAX"),
                        help="think time between two actions, seconds (default: 2 6)")
    parser.add_argument("--ramp", type=float, default=10, help="start the users over these seconds (default: 10)")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--out", help="JSON lines of every request")
    args = parser.parse_args()
    out = open(args.out, "w") if args.out else None
    rec = Recorder(out)
    stop = threading.Event()
    users = []
    for spec in args.user:
        profile, email, password = spec.split(":", 2)
        for _ in range(args.per_user):
            users.append(User(len(users), profile, email, password, args, rec, stop))
    t0 = time.time()
    for u in users:
        u.start()
    try:
        while time.time() - t0 < args.duration:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    stop.set()
    for u in users:
        u.join(timeout=args.timeout)
    duration = time.time() - t0
    if out:
        out.close()
    print("%d users, %.0f s, think %s s" % (len(users), duration, args.think))
    summary(rec.rows, duration, args.top)


if __name__ == "__main__":
    main()
