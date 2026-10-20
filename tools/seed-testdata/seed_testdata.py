#!/usr/bin/env python3
"""Realistic test data for an oc3 collector database, and its removal.

The script writes SQL on stdout; pipe it into the mysql client of the database:

    ./seed_testdata.py up      | mysql opensvc    # load the test data
    ./seed_testdata.py refresh | mysql opensvc    # keep it alive (see below)
    ./seed_testdata.py status  | mysql opensvc    # count it
    ./seed_testdata.py down    | mysql opensvc    # remove it, and only it

Every cluster, node, service and app generated is recorded in the table
zz_seed_registry, and named with a prefix ("sd" by default). `down` removes,
from every table having a node_id or svc_id column, the rows of the nodes and
services recorded there (those the scheduler wrote since about them included),
then the alert references only they used, the clusters and the apps recorded,
and the registry itself. Rows of other objects are never matched.

`up` applies in one transaction, all or nothing, and refuses to run while a
previous load is in place. Dates are relative to the time of the load.

oc3's scheduler scrubs the instances and resources not updated for about 20
minutes, and raises "not updated" alerts on stale objects: run `refresh` every
few minutes for as long as the data must look alive, e.g.

    while sleep 300; do ./seed_testdata.py refresh | mysql opensvc; done

Python 3.8+, standard library only.
"""

import argparse
import json
import random
import sys
import uuid

REGISTRY = "zz_seed_registry"
BATCH = 500

# A fixed namespace: the same prefix and the same --seed give the same ids.
NAMESPACE = uuid.UUID("6f1c5e0e-6d3a-4c1b-9a7e-0c5e5d7a2b10")

SITES = [
    ("par", "FR", "Paris", "75012", "Equinix PA3"),
    ("lyn", "FR", "Lyon", "69007", "Lyon DC2"),
    ("fra", "DE", "Frankfurt", "60314", "Interxion FRA8"),
    ("ams", "NL", "Amsterdam", "1101", "AM5"),
    ("lon", "GB", "London", "E14", "Telehouse North"),
    ("ash", "US", "Ashburn", "20147", "DC11"),
    ("sgp", "SG", "Singapore", "569933", "SG1"),
]

# Code, domain, team: the applications the clusters serve.
APPS = [
    ("ERP", "Finance", "Finance IT"), ("CRM", "Sales", "Sales IT"),
    ("BILLING", "Finance", "Finance IT"), ("PAYMENT", "Finance", "Payments Ops"),
    ("HR", "Corporate", "Corporate IT"), ("WMS", "Logistics", "Supply IT"),
    ("SCM", "Logistics", "Supply IT"), ("BI", "Data", "Data Platform"),
    ("DWH", "Data", "Data Platform"), ("MAIL", "Infrastructure", "Messaging"),
    ("WEB", "Digital", "Web Ops"), ("API", "Digital", "Web Ops"),
    ("IAM", "Security", "Identity"), ("LDAP", "Security", "Identity"),
    ("DNS", "Infrastructure", "Network Ops"), ("MON", "Infrastructure", "Observability"),
    ("LOGS", "Infrastructure", "Observability"), ("BACKUP", "Infrastructure", "Storage Ops"),
    ("CI", "Engineering", "DevTools"), ("GIT", "Engineering", "DevTools"),
    ("KAFKA", "Data", "Streaming"), ("REDIS", "Data", "Data Platform"),
    ("PGSQL", "Data", "DBA"), ("ORACLE", "Data", "DBA"), ("MYSQL", "Data", "DBA"),
    ("ELASTIC", "Data", "Search"), ("K8S", "Engineering", "Platform"),
    ("FILE", "Infrastructure", "Storage Ops"), ("SAP", "Finance", "SAP Basis"),
    ("ML", "Data", "Data Science"), ("PROXY", "Infrastructure", "Network Ops"),
    ("VPN", "Security", "Network Ops"), ("PKI", "Security", "Identity"),
    ("TICKET", "Corporate", "Service Desk"), ("WIKI", "Corporate", "Service Desk"),
]

# Environment, weight: the share of the clusters in each.
ENVS = [("PRD", 45), ("PPRD", 8), ("REC", 10), ("INT", 7), ("DEV", 20), ("TST", 10)]

HARDWARE = [
    ("Dell Inc.", "PowerEdge R650", "Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz", 64, 512 * 1024),
    ("Dell Inc.", "PowerEdge R750", "Intel(R) Xeon(R) Gold 6348 CPU @ 2.60GHz", 56, 768 * 1024),
    ("HPE", "ProLiant DL380 Gen10", "Intel(R) Xeon(R) Gold 6242 CPU @ 2.80GHz", 32, 384 * 1024),
    ("HPE", "ProLiant DL360 Gen11", "Intel(R) Xeon(R) Gold 5418Y", 48, 256 * 1024),
    ("Lenovo", "ThinkSystem SR650 V2", "Intel(R) Xeon(R) Silver 4314 CPU @ 2.40GHz", 32, 256 * 1024),
    ("VMware, Inc.", "VMware Virtual Platform", "Intel(R) Xeon(R) Gold 6254 CPU @ 3.10GHz", 8, 32 * 1024),
    ("VMware, Inc.", "VMware Virtual Platform", "Intel(R) Xeon(R) Gold 6254 CPU @ 3.10GHz", 4, 16 * 1024),
    ("QEMU", "Standard PC (Q35 + ICH9, 2009)", "AMD EPYC 7763 64-Core Processor", 16, 64 * 1024),
]

OSES = [
    ("Linux", "Red Hat", "9.4", "5.14.0-427.13.1.el9_4.x86_64", 30),
    ("Linux", "Red Hat", "8.10", "4.18.0-553.el8_10.x86_64", 20),
    ("Linux", "Ubuntu", "24.04", "6.8.0-45-generic", 20),
    ("Linux", "Ubuntu", "22.04", "5.15.0-122-generic", 12),
    ("Linux", "SUSE", "15.5", "5.14.21-150500.55.73-default", 10),
    ("Linux", "Debian", "12", "6.1.0-25-amd64", 6),
    ("SunOS", "Oracle", "11.4", "11.4.72.176.1", 1),
    ("AIX", "IBM", "7.3", "7300-02-02", 1),
]

AGENT_VERSIONS = ["3.0.0-alpha142", "3.0.0-alpha145", "3.0.0-beta12", "3.0.0-beta15", "2.1-1912"]

COMPONENTS = ["api", "web", "front", "back", "db", "worker", "cache", "queue", "batch",
              "scheduler", "gateway", "auth", "search", "report", "etl", "ftp", "smtp",
              "proxy", "cron", "export", "import", "sync", "admin", "portal", "broker"]

# Object kind, weight; cfg and sec carry no resources.
KINDS = [("svc", 70), ("vol", 15), ("cfg", 10), ("sec", 5)]

ACTIONS = [("start", 18), ("stop", 10), ("restart", 14), ("freeze", 6), ("thaw", 6),
           ("provision", 3), ("push resinfo", 25), ("run", 10), ("sync update", 8)]

CHECKS = [
    ("fs_u", ["/", "/var", "/opt", "/srv/data", "/var/log"], 0, 90, (8, 97)),
    ("fs_i", ["/", "/var"], 0, 90, (1, 40)),
    ("eth", ["eth0.link", "eth1.link", "bond0.link"], 1, 1, (1, 1)),
    ("vg_u", ["rootvg", "datavg"], 0, 95, (20, 99)),
    ("mpath", None, 4, 4, (2, 4)),
]


def weighted(rng, choices):
    total = sum(w for _, w in choices)
    pick = rng.uniform(0, total)
    for value, weight in choices:
        pick -= weight
        if pick <= 0:
            return value
    return choices[-1][0]


class Raw(str):
    """A SQL expression written as is, a date relative to NOW() for instance."""


def ago(seconds):
    return Raw("NOW() - INTERVAL %d SECOND" % max(0, int(seconds)))


def sql(value):
    if value is None:
        return "NULL"
    if isinstance(value, Raw):
        return str(value)
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
    return "'%s'" % text


class Writer:
    """Writes multi-row INSERT statements, BATCH rows each."""

    def __init__(self, out):
        self.out = out
        self.counts = {}

    def insert(self, table, columns, rows, ignore=False):
        rows = list(rows)
        self.counts[table] = self.counts.get(table, 0) + len(rows)
        head = "INSERT %sINTO `%s` (%s) VALUES\n" % (
            "IGNORE " if ignore else "", table, ", ".join("`%s`" % c for c in columns))
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            self.out.write(head)
            self.out.write(",\n".join("(" + ", ".join(sql(v) for v in row) + ")" for row in chunk))
            self.out.write(";\n")


def make_id(prefix, kind, key):
    return str(uuid.uuid5(NAMESPACE, "%s:%s:%s" % (prefix, kind, key)))


def svc_config(svc_id, kind, topology, nodes, resources):
    lines = ["[DEFAULT]", "id = %s" % svc_id, "nodes = %s" % " ".join(nodes)]
    if kind == "svc":
        lines += ["orchestrate = ha", "topology = %s" % topology]
        if topology == "flex":
            lines.append("flex_target = %d" % len(nodes))
    for rid, rtype, desc in resources:
        group, _, driver = rtype.partition(".")
        lines += ["", "[%s]" % rid, "type = %s" % (driver or group)]
        if group == "fs":
            lines.append("mnt = %s" % desc.split(" ")[-1])
        elif group == "ip":
            lines.append("name = %s" % desc)
    return "\n".join(lines) + "\n"


def generate(args, out):
    rng = random.Random(args.seed)
    p = args.prefix
    w = Writer(out)
    day = 86400
    history = args.days * day

    out.write("-- Test data generated by seed_testdata.py: prefix %r, seed %d.\n" % (p, args.seed))
    out.write("SET NAMES utf8mb4;\n")
    out.write(
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
        "  kind VARCHAR(16) NOT NULL,\n"
        "  id VARCHAR(64) NOT NULL,\n"
        "  created DATETIME NOT NULL,\n"
        "  PRIMARY KEY (kind, id)\n"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'objects of seed_testdata.py';\n" % REGISTRY)
    out.write("START TRANSACTION;\n")

    registry = []

    # Applications: codes prefixed, so that none is one of the collector's.
    apps = []
    for code, domain, team in APPS:
        app = "%s-%s" % (p.upper(), code)
        apps.append((app, domain, team))
        registry.append(("app", app))
    w.insert("apps", ["app", "app_domain", "app_team_ops", "description"],
             [(app, domain, team, "%s application (test data)" % app.split("-", 1)[1])
              for app, domain, team in apps])
    if args.group:
        roles = ", ".join(sql(g) for g in args.group)
        out.write(
            "INSERT INTO apps_responsibles (group_id, app_id)\n"
            "  SELECT g.id, a.id FROM auth_group g JOIN apps a ON a.app IN (%s)\n"
            "  WHERE g.role IN (%s);\n" % (", ".join(sql(a[0]) for a in apps), roles))

    clusters, nodes, services = [], [], []
    svcmon, resmon, actions, logs = [], [], [], []
    services_log, services_log_last = [], []
    svcmon_log, svcmon_log_last = [], []
    resmon_log, resmon_log_last = [], []
    dashboard, checks, ips = [], [], []

    # Clusters and their nodes.
    cluster_list = []
    node_count = 0
    for ci in range(args.clusters):
        site = rng.choice(SITES)
        app, domain, team = rng.choice(apps)
        env = weighted(rng, ENVS)
        code = app.split("-", 1)[1].lower()
        name = "%s-%s-%s-%s-%03d" % (p, site[0], code, env.lower(), ci)
        cluster_id = make_id(p, "cluster", ci)
        registry.append(("cluster", cluster_id))
        n_nodes = rng.choices([1, 2, 3, 4, 5, 6], weights=[15, 30, 25, 12, 10, 8])[0]
        hw = rng.choice(HARDWARE)
        os_ = weighted(rng, [(o, o[4]) for o in OSES])
        members = []
        for ni in range(n_nodes):
            nodename = "%sn%d" % (name, ni + 1)
            node_id = make_id(p, "node", "%d.%d" % (ci, ni))
            registry.append(("node", node_id))
            members.append((node_id, nodename))
            node_count += 1
            boot = rng.randint(3, 200) * day
            warranty = rng.randint(-200, 1500) * day
            serial = "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(10))
            frozen = "T" if rng.random() < 0.03 else "F"
            nodes.append((
                node_id, nodename, cluster_id, app, env, env, team, team,
                site[1], site[2], site[3], site[4], "%d" % rng.randint(1, 4),
                "R%02d" % rng.randint(1, 40), hw[0], hw[1], serial,
                "physical" if hw[0] not in ("VMware, Inc.", "QEMU") else "virtual",
                hw[2], hw[3], 2 if hw[3] >= 32 else 1, hw[4], os_[0], os_[1], os_[2],
                os_[3], "x86_64" if os_[0] == "Linux" else ("sparcv9" if os_[0] == "SunOS" else "ppc64"),
                rng.choice(AGENT_VERSIONS), "%s.%s.example.com" % (nodename, site[0]),
                "Europe/Paris" if site[1] in ("FR", "DE", "NL") else "UTC",
                ago(rng.randint(0, 120)), ago(rng.randint(0, 120)), ago(boot),
                ago(-warranty) if warranty < 0 else Raw("NOW() + INTERVAL %d SECOND" % warranty),
                "push" if rng.random() < 0.9 else "feed", frozen,
                rng.choice(["production", "standby", "spare"]) if env == "PRD" else "test",
            ))
            # Two or three addresses, a mac each.
            # A locally administered mac prefix (02:5d), numbered by node: neither
            # one of the collector's nor shared by two test nodes, which the
            # scheduler would raise as a duplicate.
            for k, intf in enumerate(["eth0", "eth1", "bond0"][:rng.randint(2, 3)]):
                mac = "02:5d:%02x:%02x:%02x:%02x" % ((node_count >> 16) & 255, (node_count >> 8) & 255,
                                                       node_count & 255, k)
                ips.append((node_id, mac, intf, "ipv4",
                            "10.%d.%d.%d" % (100 + SITES.index(site), node_count // 80, (node_count % 80) * 3 + k + 1),
                            "255.255.0.0", ago(rng.randint(0, 600))))
            # Checks, a few out of their thresholds.
            for ctype, instances, low, high, (vmin, vmax) in CHECKS:
                for inst in (instances or ["3600%026x" % rng.getrandbits(104) for _ in range(rng.randint(1, 3))]):
                    value = rng.randint(vmin, vmax)
                    if ctype == "fs_u" and rng.random() < 0.04:
                        value = rng.randint(91, 100)
                    checks.append((node_id, "", ctype, inst, value, low, high, "defaults", ago(rng.randint(0, 600))))
                    if value > high or value < low:
                        dashboard.append(("check out of bounds", node_id, "", 2,
                                          "%(ctype)s:%(inst)s check value %(val)d. %(ttype)s thresholds: %(min)d - %(max)d",
                                          json.dumps({"ctype": ctype, "inst": inst, "val": value, "ttype": "defaults", "min": low, "max": high}),
                                          "%s:%s" % (ctype, inst), env))
            # The node's own log: registrations, pushes, changes.
            for _ in range(rng.randint(args.node_logs // 2, args.node_logs)):
                at = ago(rng.randint(0, history))
                kind_ = rng.random()
                if kind_ < 0.45:
                    logs.append(("node.action", "root@%s" % nodename, "run %(action)s",
                                 json.dumps({"action": rng.choice(["pushasset", "pushpkg", "pushdisks", "checks", "sysreport", "pushstats"])}),
                                 at, "", node_id, "info"))
                elif kind_ < 0.65:
                    logs.append(("node.change", "%s.admin@example.com" % team.split()[0].lower(), "change properties %(data)s",
                                 json.dumps({"data": {"loc_rack": "R%02d" % rng.randint(1, 40)}}), at, "", node_id, "info"))
                elif kind_ < 0.85:
                    logs.append(("checks.alert", "scheduler", "check %(t)s:%(i)s out of bounds",
                                 json.dumps({"t": "fs_u", "i": "/var"}), at, "", node_id, "warning"))
                else:
                    logs.append(("node.status", "scheduler", "node %(node)s not updated for %(h)s hours",
                                 json.dumps({"node": nodename, "h": rng.randint(2, 30)}), at, "", node_id, "error"))
        cluster_list.append((cluster_id, name, app, env, members, site))
        clusters.append((cluster_id, name, json.dumps({"version": "3.0.0", "seed": True})))

    # Services, spread over the clusters, ~services/clusters each.
    for si in range(args.services):
        cluster_id, cname, app, env, members, site = cluster_list[si % len(cluster_list)]
        kind = weighted(rng, KINDS)
        ns = app.split("-", 1)[1].lower()
        svcname = "%s/%s/%s-%s-%05d" % (ns, kind, p, rng.choice(COMPONENTS), si)
        svc_id = make_id(p, "service", si)
        registry.append(("service", svc_id))
        topology = "flex" if kind == "svc" and rng.random() < 0.25 else "failover"
        ha = 1 if kind == "svc" and len(members) > 1 and rng.random() < 0.7 else 0
        # Status of the service as a whole.
        if kind in ("cfg", "sec"):
            avail, overall = "n/a", "n/a"
        else:
            avail = weighted(rng, [("up", 90), ("down", 4), ("warn", 3), ("n/a", 3)])
            overall = "warn" if avail == "up" and rng.random() < 0.05 else avail
        frozen = "frozen" if rng.random() < 0.03 else "unfrozen"
        placement = "optimal" if ha and avail == "up" else ("non-optimal" if ha else "n/a")
        # Resources.
        res = []
        if kind == "svc":
            res.append(("ip#0", "ip.host", "%s.%s.example.com" % (svcname.split("/")[-1], site[0])))
            for k in range(rng.randint(1, 3)):
                res.append(("fs#%d" % k, rng.choice(["fs.xfs", "fs.ext4", "fs.flag"]), "/srv/%s/fs%d" % (svcname.split("/")[-1], k)))
            if rng.random() < 0.5:
                res.append(("disk#0", rng.choice(["disk.lvm", "disk.disk", "disk.zpool"]), "vg_%s" % ns))
            for k in range(rng.randint(1, 3)):
                res.append(("app#%d" % k, rng.choice(["app.forking", "app.simple"]), "%s-%s" % (ns, rng.choice(COMPONENTS))))
            if rng.random() < 0.3:
                res.append(("container#0", "container.podman", "%s/%s:%s" % (ns, ns, rng.choice(["1.4", "2.0", "latest"]))))
            if rng.random() < 0.2:
                res.append(("sync#i0", "sync.rsync", "internal"))
        elif kind == "vol":
            res.append(("disk#1", "disk.lvm", "vg_%s" % ns))
            res.append(("fs#1", "fs.xfs", "/srv/%s" % svcname.split("/")[-1]))
        node_names = [n for _, n in members]
        services.append((
            svc_id, svcname, cluster_id, app, env, topology, ha, avail, overall, frozen,
            "True" if kind != "cfg" else "n/a", placement, " ".join(node_names),
            svc_config(svc_id, kind, topology, node_names, res),
            ago(rng.randint(0, 120)), ago(rng.randint(day, history)), ago(rng.randint(0, 3600)),
            rng.choice([None, None, 99.5, 99.9, 99.95]) if env == "PRD" else None,
        ))
        if avail in ("down", "warn"):
            dashboard.append(("service unavailable" if avail == "down" else "service available but degraded",
                              "", svc_id, 3 if avail == "down" else 2,
                              "current availability status: %(s)s" if avail == "down" else "current overall status: %(s)s",
                              json.dumps({"s": avail}), "%s:%s" % (p, svcname), env))
        # Availability history: segments back over the period.
        t = history
        segs = []
        while t > 0:
            dur = rng.randint(6 * 3600, 12 * day)
            status = "up" if rng.random() < 0.85 else rng.choice(["down", "warn"])
            segs.append((status, min(t, history), max(0, t - dur)))
            t -= dur
        segs[-1] = (avail if avail != "n/a" else "up", segs[-1][1], 0)
        for status, begin, end in segs[:-1]:
            services_log.append((svc_id, status, ago(begin), ago(end)))
        services_log_last.append((svc_id, segs[-1][0], ago(segs[-1][1]), ago(0)))
        # Instances: on every node of the cluster; a failover service runs on one.
        leader = rng.randrange(len(members))
        for ni, (node_id, nodename) in enumerate(members):
            if kind in ("cfg", "sec"):
                iavail = ioverall = "n/a"
            elif topology == "flex" or ni == leader:
                iavail, ioverall = (avail, overall) if avail != "n/a" else ("up", "up")
            else:
                iavail = ioverall = "stdby down" if ha else "down"
            svcmon.append((
                node_id, svc_id, kind, iavail, ioverall,
                *(("up" if iavail == "up" else ("n/a" if kind in ("cfg", "sec") else "down")) for _ in range(3)),
                "idle", "none", 1 if frozen == "frozen" else 0, ago(rng.randint(0, 120)), ago(rng.randint(day, history)),
            ))
            svcmon_log.append((node_id, svc_id, "up", "up", ago(history), ago(rng.randint(day, history - day))))
            svcmon_log_last.append((node_id, svc_id, ioverall if ioverall != "stdby down" else "down",
                                    iavail if iavail != "stdby down" else "down", ago(rng.randint(day, history // 2)), ago(0)))
            for rid, rtype, desc in res:
                if iavail == "up":
                    rstatus = "up" if rng.random() > 0.01 else "warn"
                elif iavail in ("down", "stdby down"):
                    rstatus = "stdby up" if rtype.startswith("disk") and ha else "down"
                else:
                    rstatus = "n/a"
                resmon.append((node_id, svc_id, rid, rtype, rstatus, "%s %s" % (rtype.split(".")[-1], desc),
                               "F" if rtype.startswith("sync") else ("T" if rtype.startswith("app") and ha else "F"),
                               "F", "F", "", ago(rng.randint(0, 120)), ago(rng.randint(day, history))))
                if rng.random() < args.resource_history:
                    resmon_log.append((node_id, svc_id, rid, "up", ago(history), ago(rng.randint(day, history - day)), ""))
                    resmon_log_last.append((node_id, svc_id, rid, rstatus, ago(rng.randint(day, history // 2)), ago(0), ""))
        # Agent actions over the period, each with its log lines.
        if kind in ("svc", "vol"):
            n_actions = rng.randint(0, args.actions * 2)
            for _ in range(n_actions):
                node_id, nodename = members[rng.randrange(len(members))]
                action = weighted(rng, ACTIONS)
                status = weighted(rng, [("ok", 92), ("warn", 3), ("err", 5)])
                begin = rng.randint(60, history)
                took = rng.randint(1, 180)
                sid = str(uuid.UUID(int=rng.getrandbits(128), version=4))
                cron = 1 if action == "push resinfo" else 0
                command = "om %s %s" % (svcname, action if action != "push resinfo" else "push resinfo")
                origin = rng.choice(["daemon/monitor", "user", "daemon/scheduler"]) if not cron else "daemon/scheduler"
                acked = status == "err" and rng.random() < 0.4
                actions.append((node_id, svc_id, action, status, ago(begin), ago(begin - took), took, cron, sid,
                                "%s instance %s" % (svcname, action), command, origin, "status",
                                1 if acked else (0 if status == "err" else None),
                                "%s.admin@example.com" % ns if acked else None,
                                "known issue, fixed by %s" % ("restart" if action != "restart" else "hand") if acked else None,
                                ago(begin - took - 3600) if acked else None))
                for k in range(args.action_lines):
                    line = ("instance: %s: <<< done %s [/usr/bin/%s] in %ds" % (svcname, action, command, took)
                            if status == "ok" else
                            "instance: %s: %s: %s" % (svcname, rng.choice([r[0] for r in res] or ["app#0"]),
                                                      "exit code 1" if status == "err" else "took longer than expected"))
                    actions.append((node_id, svc_id, action, status, ago(begin - k), ago(begin - took), None, cron, sid,
                                    line, command, origin, "", None, None, None, None))
                if status == "err" and not acked and rng.random() < 0.5:
                    logs.append(("service.action", "scheduler", "action %(action)s failed on %(node)s",
                                 json.dumps({"action": action, "node": nodename}), ago(begin - took), svc_id, node_id, "error"))
        # The service's own log.
        for _ in range(rng.randint(args.service_logs // 2, args.service_logs)):
            at = ago(rng.randint(0, history))
            r = rng.random()
            if r < 0.4:
                logs.append(("service.action", "%s.admin@example.com" % ns, "run %(action)s",
                             json.dumps({"action": rng.choice(["start", "stop", "restart", "freeze", "thaw"])}), at, svc_id, "", "info"))
            elif r < 0.6:
                logs.append(("service.change", "%s.admin@example.com" % ns, "change properties %(data)s",
                             json.dumps({"data": {"svc_sla": rng.choice([99.5, 99.9])}}), at, svc_id, "", "info"))
            elif r < 0.8:
                logs.append(("service.status", "scheduler", "service '%(svc)s' status changed to %(s)s",
                             json.dumps({"svc": svcname, "s": rng.choice(["down", "warn", "up"])}), at, svc_id, "", "warning"))
            else:
                logs.append(("service.config", "scheduler", "service '%(svc)s' configuration updated",
                             json.dumps({"svc": svcname}), at, svc_id, "", "info"))

    # The registry first: a second load stops at the first id already there.
    w.insert(REGISTRY, ["kind", "id", "created"], [(k, i, Raw("NOW()")) for k, i in registry])
    w.insert("clusters", ["cluster_id", "cluster_name", "cluster_data"], clusters)
    w.insert("nodes", [
        "node_id", "nodename", "cluster_id", "app", "node_env", "asset_env", "team_responsible", "team_support",
        "loc_country", "loc_city", "loc_zip", "loc_building", "loc_floor", "loc_rack", "manufacturer", "model",
        "serial", "type", "cpu_model", "cpu_cores", "cpu_dies", "mem_bytes", "os_name", "os_vendor", "os_release",
        "os_kernel", "os_arch", "version", "fqdn", "tz", "updated", "last_comm", "last_boot", "warranty_end",
        "action_type", "node_frozen", "status"], nodes)
    w.insert("node_ip", ["node_id", "mac", "intf", "type", "addr", "mask", "updated"], ips)
    w.insert("checks_live", ["node_id", "svc_id", "chk_type", "chk_instance", "chk_value", "chk_low", "chk_high",
                             "chk_threshold_provider", "chk_updated"], checks)
    w.insert("services", [
        "svc_id", "svcname", "cluster_id", "svc_app", "svc_env", "svc_topology", "svc_ha", "svc_availstatus",
        "svc_status", "svc_frozen", "svc_provisioned", "svc_placement", "svc_nodes", "svc_config",
        "svc_status_updated", "svc_config_updated", "updated", "svc_sla"], services)
    w.insert("svcmon", [
        "node_id", "svc_id", "mon_svctype", "mon_availstatus", "mon_overallstatus", "mon_ipstatus",
        "mon_fsstatus", "mon_appstatus", "mon_smon_status", "mon_smon_global_expect", "mon_frozen",
        "mon_updated", "mon_changed"], svcmon)
    w.insert("resmon", ["node_id", "svc_id", "rid", "res_type", "res_status", "res_desc", "res_monitor",
                        "res_disable", "res_optional", "vmname", "updated", "changed"], resmon)
    w.insert("svcactions", ["node_id", "svc_id", "action", "status", "begin", "end", "time", "cron", "sid",
                            "status_log", "command", "origin", "log_type", "ack", "acked_by", "acked_comment",
                            "acked_date"], actions)
    w.insert("log", ["log_action", "log_user", "log_fmt", "log_dict", "log_date", "svc_id", "node_id", "log_level"], logs)
    w.insert("services_log", ["svc_id", "svc_availstatus", "svc_begin", "svc_end"], services_log)
    w.insert("services_log_last", ["svc_id", "svc_availstatus", "svc_begin", "svc_end"], services_log_last)
    w.insert("svcmon_log", ["node_id", "svc_id", "mon_overallstatus", "mon_availstatus", "mon_begin", "mon_end"], svcmon_log)
    w.insert("svcmon_log_last", ["node_id", "svc_id", "mon_overallstatus", "mon_availstatus", "mon_begin", "mon_end"],
             svcmon_log_last)
    w.insert("resmon_log", ["node_id", "svc_id", "rid", "res_status", "res_begin", "res_end", "res_log"], resmon_log)
    w.insert("resmon_log_last", ["node_id", "svc_id", "rid", "res_status", "res_begin", "res_end", "res_log"],
             resmon_log_last)
    # Alerts: their instance names a seed object, so that their reference, shared
    # by md5 of type and instance (dashboard_ref, written by the dash_add_evt
    # trigger), is one of the test data's alone.
    w.insert("dashboard", ["dash_type", "node_id", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_instance",
                           "dash_env", "dash_updated"],
             [(t, n, s, sev, f, d, inst if inst.startswith(p) or ":" not in inst else "%s:%s" % (p, inst), env, Raw("NOW()"))
              for t, n, s, sev, f, d, inst, env in dashboard])
    out.write("COMMIT;\n")
    out.write("SELECT 'seed_testdata: loaded' AS done;\n")
    sys.stderr.write("rows generated:\n")
    for table, count in sorted(w.counts.items()):
        sys.stderr.write("  %-20s %8d\n" % (table, count))


def seed_ids(kind):
    return "SELECT id FROM `%s` WHERE kind = '%s'" % (REGISTRY, kind)


def down(out):
    """Removes every row of the objects recorded in the registry, then the registry."""
    out.write(
        "-- Removes the test data of seed_testdata.py, and only it.\n"
        "DROP PROCEDURE IF EXISTS zz_seed_down;\n"
        "DELIMITER //\n"
        "CREATE PROCEDURE zz_seed_down()\n"
        "BEGIN\n"
        "  DECLARE done INT DEFAULT 0;\n"
        "  DECLARE t VARCHAR(64);\n"
        "  DECLARE c VARCHAR(64);\n"
        # Every base table with a node_id or svc_id column, the test data's and
        # what the scheduler wrote about its objects since.
        "  DECLARE cur CURSOR FOR\n"
        "    SELECT col.table_name, col.column_name FROM information_schema.columns col\n"
        "    JOIN information_schema.tables tb ON tb.table_schema = col.table_schema\n"
        "      AND tb.table_name = col.table_name AND tb.table_type = 'BASE TABLE'\n"
        "    WHERE col.table_schema = DATABASE() AND col.column_name IN ('node_id', 'svc_id')\n"
        "    ORDER BY col.table_name = 'dashboard' DESC, col.table_name, col.column_name;\n"
        "  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;\n"
        "  IF (SELECT COUNT(*) FROM information_schema.tables\n"
        "      WHERE table_schema = DATABASE() AND table_name = '%(reg)s') = 0 THEN\n"
        "    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'no seed_testdata registry: nothing to remove';\n"
        "  END IF;\n"
        "  START TRANSACTION;\n"
        # The alert references the test data used, removed last if nothing else uses them.
        "  CREATE TEMPORARY TABLE zz_seed_md5 (dash_md5 BINARY(32) PRIMARY KEY);\n"
        "  INSERT IGNORE INTO zz_seed_md5 SELECT dash_md5 FROM dashboard\n"
        "    WHERE node_id IN (%(nodes)s) OR svc_id IN (%(svcs)s);\n"
        "  INSERT IGNORE INTO zz_seed_md5 SELECT dash_md5 FROM dashboard_events\n"
        "    WHERE (node_id IN (%(nodes)s) OR svc_id IN (%(svcs)s)) AND dash_md5 IS NOT NULL;\n"
        "  OPEN cur;\n"
        "  rows_loop: LOOP\n"
        "    FETCH cur INTO t, c;\n"
        "    IF done THEN LEAVE rows_loop; END IF;\n"
        "    SET @q = CONCAT('DELETE x FROM `', t, '` x JOIN `%(reg)s` r ON r.kind = ''',\n"
        "      IF(c = 'node_id', 'node', 'service'), ''' AND r.id = x.`', c, '`');\n"
        "    PREPARE stmt FROM @q;\n"
        "    EXECUTE stmt;\n"
        "    DEALLOCATE PREPARE stmt;\n"
        "  END LOOP;\n"
        "  CLOSE cur;\n"
        "  DELETE r FROM dashboard_ref r JOIN zz_seed_md5 m ON m.dash_md5 = r.dash_md5\n"
        "    WHERE NOT EXISTS (SELECT 1 FROM dashboard d WHERE d.dash_md5 = r.dash_md5)\n"
        "      AND NOT EXISTS (SELECT 1 FROM dashboard_events e WHERE e.dash_md5 = r.dash_md5);\n"
        "  DROP TEMPORARY TABLE zz_seed_md5;\n"
        "  DELETE FROM clusters WHERE cluster_id IN (%(clusters)s);\n"
        "  DELETE ar FROM apps_responsibles ar JOIN apps a ON a.id = ar.app_id WHERE a.app IN (%(apps)s);\n"
        "  DELETE FROM apps WHERE app IN (%(apps)s);\n"
        "  DELETE FROM `%(reg)s`;\n"
        "  COMMIT;\n"
        "END //\n"
        "DELIMITER ;\n"
        "CALL zz_seed_down();\n"
        "DROP PROCEDURE zz_seed_down;\n"
        "DROP TABLE `%(reg)s`;\n"
        "SELECT 'seed_testdata: removed' AS done;\n"
        % {"reg": REGISTRY, "nodes": seed_ids("node"), "svcs": seed_ids("service"),
           "clusters": seed_ids("cluster"), "apps": seed_ids("app")})


def refresh(out):
    """Dates the live state of the test data now, as agents reporting would."""
    nodes, svcs = seed_ids("node"), seed_ids("service")
    out.write(
        "-- Keeps the test data of seed_testdata.py from going stale.\n"
        "UPDATE nodes SET updated = NOW(), last_comm = NOW() WHERE node_id IN (%(n)s);\n"
        "UPDATE services SET svc_status_updated = NOW(), updated = NOW() WHERE svc_id IN (%(s)s);\n"
        "UPDATE svcmon SET mon_updated = NOW() WHERE svc_id IN (%(s)s);\n"
        "UPDATE resmon SET updated = NOW() WHERE svc_id IN (%(s)s);\n"
        "UPDATE checks_live SET chk_updated = NOW() WHERE node_id IN (%(n)s);\n"
        "UPDATE node_ip SET updated = NOW() WHERE node_id IN (%(n)s);\n"
        "UPDATE dashboard SET dash_updated = NOW() WHERE node_id IN (%(n)s) OR svc_id IN (%(s)s);\n"
        "UPDATE services_log_last SET svc_end = NOW() WHERE svc_id IN (%(s)s);\n"
        "UPDATE svcmon_log_last SET mon_end = NOW() WHERE svc_id IN (%(s)s);\n"
        "UPDATE resmon_log_last SET res_end = NOW() WHERE svc_id IN (%(s)s);\n"
        % {"n": nodes, "s": svcs})


def status(out):
    """Counts the objects of the registry and the rows of the main tables they own."""
    nodes, svcs = seed_ids("node"), seed_ids("service")
    out.write("SELECT kind, COUNT(*) AS objects FROM `%s` GROUP BY kind;\n" % REGISTRY)
    parts = []
    for table, column in [("svcmon", "svc_id"), ("resmon", "svc_id"), ("svcactions", "svc_id"), ("log", "svc_id"),
                          ("dashboard", "svc_id"), ("checks_live", "node_id"), ("services_log", "svc_id"),
                          ("svcmon_log", "svc_id"), ("resmon_log", "svc_id")]:
        ids = nodes if column == "node_id" else svcs
        parts.append("SELECT '%s' AS tbl, COUNT(*) AS seed_rows FROM `%s` WHERE `%s` IN (%s)" % (table, table, column, ids))
    parts.append("SELECT 'log (nodes)', COUNT(*) FROM log WHERE node_id IN (%s)" % nodes)
    out.write("\nUNION ALL ".join(parts) + ";\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    up = sub.add_parser("up", help="SQL loading the test data")
    up.add_argument("--prefix", default="sd", help="prefix of every name generated (default: sd)")
    up.add_argument("--seed", type=int, default=42, help="random seed: the same gives the same data (default: 42)")
    up.add_argument("--clusters", type=int, default=500)
    up.add_argument("--services", type=int, default=10000)
    up.add_argument("--days", type=int, default=60, help="history covered by actions and logs (default: 60)")
    up.add_argument("--actions", type=int, default=4, help="mean agent actions per service (default: 4)")
    up.add_argument("--action-lines", type=int, default=1, help="log lines per action (default: 1)")
    up.add_argument("--node-logs", type=int, default=20, help="most collector log entries per node (default: 20)")
    up.add_argument("--service-logs", type=int, default=8, help="most collector log entries per service (default: 8)")
    up.add_argument("--resource-history", type=float, default=0.3,
                    help="share of the resources with a status history (default: 0.3)")
    up.add_argument("--group", action="append", default=[],
                    help="existing team made responsible for the test apps, so that its members see them; repeatable")
    sub.add_parser("down", help="SQL removing the test data, and only it")
    sub.add_parser("refresh", help="SQL dating the live state of the test data now")
    sub.add_parser("status", help="SQL counting the test data")
    args = parser.parse_args()
    out = sys.stdout
    if args.command == "up":
        if not args.prefix.isalnum() or not args.prefix.islower():
            parser.error("--prefix: lower case letters and digits only")
        if args.clusters < 1 or args.services < 0:
            parser.error("--clusters must be 1 or more, --services 0 or more")
        if args.days < 3:
            parser.error("--days must be 3 or more: the history spans a few days at least")
        generate(args, out)
    elif args.command == "down":
        down(out)
    elif args.command == "refresh":
        refresh(out)
    else:
        status(out)


if __name__ == "__main__":
    main()
