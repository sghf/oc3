#!/usr/bin/env python3
"""Realistic test data for an oc3 collector database, and its removal.

The script writes SQL on stdout; pipe it into the mysql client of the database:

    ./seed_testdata.py up       | mysql opensvc    # load the test data
    ./seed_testdata.py activity | mysql opensvc    # add years of activity
    ./seed_testdata.py refresh  | mysql opensvc    # keep it alive (see below)
    ./seed_testdata.py status   | mysql opensvc    # count it
    ./seed_testdata.py down     | mysql opensvc    # remove it, and only it

Every cluster, node, service and app generated is recorded in the table
zz_seed_registry, and named with a prefix ("sd" by default). `down` removes,
from every table having a node_id or svc_id column, the rows of the nodes and
services recorded there (those the scheduler wrote since about them included),
then the alert references only they used, the clusters and the apps recorded,
and the registry itself. Rows of other objects are never matched.

`up` applies in one transaction, all or nothing, and refuses to run while a
previous load is in place. Dates are relative to the time of the load.
`activity`, by statements each committed, runs once after `up`; `down`
removes what it loaded too, by batches.

oc3's scheduler scrubs the instances and resources not updated for about 20
minutes, and raises "not updated" alerts on stale objects: run `refresh` every
few minutes for as long as the data must look alive, e.g.

    while sleep 300; do ./seed_testdata.py refresh | mysql opensvc; done

Python 3.8+, standard library only.
"""

import argparse
import json
import math
import random
import shutil
import string
import sys
import tempfile
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

# Driver group, weight: the resources of a svc object after its first address,
# and the order of their sections in its configuration.
RESOURCE_GROUPS = [("fs", 28), ("app", 28), ("container", 15), ("disk", 12), ("ip", 5), ("sync", 6), ("task", 6)]
RESOURCE_ORDER = ["ip", "disk", "fs", "container", "app", "sync", "task"]

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


NOW = Raw("NOW()")


def ago(seconds):
    # From the epoch, not NOW() - INTERVAL: a local time computed back across a
    # change to summer time may not exist, which a TIMESTAMP column refuses.
    return Raw("FROM_UNIXTIME(UNIX_TIMESTAMP() - %d)" % max(0, int(seconds)))


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
        self.tables = []

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

    def table(self, name, columns):
        """A table whose rows are appended as they are generated, written by flush()."""
        t = Spool(name, columns)
        self.tables.append(t)
        return t

    def flush(self):
        """Writes the spooled tables, in the order of their creation."""
        for t in self.tables:
            t.copy_to(self.out)
            self.counts[t.name] = self.counts.get(t.name, 0) + t.count


class Spool:
    """The INSERT statements of a table, spooled to a temporary file as its rows
    come, so that a large load does not hold them all in memory."""

    def __init__(self, name, columns):
        self.name = name
        self.head = "INSERT INTO `%s` (%s) VALUES\n" % (name, ", ".join("`%s`" % c for c in columns))
        self.rows = []
        self.count = 0
        self.file = tempfile.TemporaryFile("w+", encoding="utf-8")

    def append(self, row):
        self.rows.append(row)
        self.count += 1
        if len(self.rows) >= BATCH:
            self._write()

    def _write(self):
        if self.rows:
            self.file.write(self.head)
            self.file.write(",\n".join("(" + ", ".join(sql(v) for v in row) + ")" for row in self.rows))
            self.file.write(";\n")
            self.rows = []

    def copy_to(self, out):
        self._write()
        self.file.seek(0)
        shutil.copyfileobj(self.file, out)
        self.file.close()


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

    # The registry first: a second load stops at the first id already there.
    registry = w.table(REGISTRY, ["kind", "id", "created"])

    # Applications: codes prefixed, so that none is one of the collector's.
    apps = []
    for code, domain, team in APPS:
        app = "%s-%s" % (p.upper(), code)
        apps.append((app, domain, team))
        registry.append(("app", app, NOW))
    w.insert("apps", ["app", "app_domain", "app_team_ops", "description"],
             [(app, domain, team, "%s application (test data)" % app.split("-", 1)[1])
              for app, domain, team in apps])
    if args.group:
        roles = ", ".join(sql(g) for g in args.group)
        out.write(
            "INSERT INTO apps_responsibles (group_id, app_id)\n"
            "  SELECT g.id, a.id FROM auth_group g JOIN apps a ON a.app IN (%s)\n"
            "  WHERE g.role IN (%s);\n" % (", ".join(sql(a[0]) for a in apps), roles))

    clusters = w.table("clusters", ["cluster_id", "cluster_name", "cluster_data"])
    nodes = w.table("nodes", [
        "node_id", "nodename", "cluster_id", "app", "node_env", "asset_env", "team_responsible", "team_support",
        "loc_country", "loc_city", "loc_zip", "loc_building", "loc_floor", "loc_rack", "manufacturer", "model",
        "serial", "type", "cpu_model", "cpu_cores", "cpu_dies", "mem_bytes", "os_name", "os_vendor", "os_release",
        "os_kernel", "os_arch", "version", "fqdn", "tz", "updated", "last_comm", "last_boot", "warranty_end",
        "action_type", "node_frozen", "status"])
    ips = w.table("node_ip", ["node_id", "mac", "intf", "type", "addr", "mask", "updated"])
    checks = w.table("checks_live", ["node_id", "svc_id", "chk_type", "chk_instance", "chk_value", "chk_low",
                                     "chk_high", "chk_threshold_provider", "chk_updated"])
    services = w.table("services", [
        "svc_id", "svcname", "cluster_id", "svc_app", "svc_env", "svc_topology", "svc_ha", "svc_availstatus",
        "svc_status", "svc_frozen", "svc_provisioned", "svc_placement", "svc_nodes", "svc_config",
        "svc_status_updated", "svc_config_updated", "updated", "svc_sla"])
    svcmon = w.table("svcmon", [
        "node_id", "svc_id", "mon_svctype", "mon_availstatus", "mon_overallstatus", "mon_ipstatus",
        "mon_fsstatus", "mon_appstatus", "mon_smon_status", "mon_smon_global_expect", "mon_frozen",
        "mon_updated", "mon_changed"])
    resmon = w.table("resmon", ["node_id", "svc_id", "rid", "res_type", "res_status", "res_desc", "res_monitor",
                                "res_disable", "res_optional", "vmname", "updated", "changed"])
    actions = w.table("svcactions", ["node_id", "svc_id", "action", "status", "begin", "end", "time", "cron", "sid",
                                     "status_log", "command", "origin", "log_type", "ack", "acked_by",
                                     "acked_comment", "acked_date"])
    logs = w.table("log", ["log_action", "log_user", "log_fmt", "log_dict", "log_date", "svc_id", "node_id",
                           "log_level"])
    services_log = w.table("services_log", ["svc_id", "svc_availstatus", "svc_begin", "svc_end"])
    services_log_last = w.table("services_log_last", ["svc_id", "svc_availstatus", "svc_begin", "svc_end"])
    svcmon_log = w.table("svcmon_log", ["node_id", "svc_id", "mon_overallstatus", "mon_availstatus", "mon_begin",
                                        "mon_end"])
    svcmon_log_last = w.table("svcmon_log_last", ["node_id", "svc_id", "mon_overallstatus", "mon_availstatus",
                                                  "mon_begin", "mon_end"])
    resmon_log = w.table("resmon_log", ["node_id", "svc_id", "rid", "res_status", "res_begin", "res_end", "res_log"])
    resmon_log_last = w.table("resmon_log_last", ["node_id", "svc_id", "rid", "res_status", "res_begin", "res_end",
                                                  "res_log"])
    # Alerts: their instance names a seed object, so that their reference, shared
    # by md5 of type and instance (dashboard_ref, written by the dash_add_evt
    # trigger), is one of the test data's alone.
    dashboard = w.table("dashboard", ["dash_type", "node_id", "svc_id", "dash_severity", "dash_fmt", "dash_dict",
                                      "dash_instance", "dash_env", "dash_updated"])

    def add_alert(row):
        t, n, s, sev, f, d, inst, env = row
        if not inst.startswith(p) and ":" in inst:
            inst = "%s:%s" % (p, inst)
        dashboard.append((t, n, s, sev, f, d, inst, env, NOW))

    # Clusters and their nodes, --clusters of them or as many as --nodes takes.
    cluster_list = []
    node_count = 0
    for ci in range(10 ** 9 if args.nodes else args.clusters):
        if args.nodes and node_count >= args.nodes:
            break
        site = rng.choice(SITES)
        app, domain, team = rng.choice(apps)
        env = weighted(rng, ENVS)
        code = app.split("-", 1)[1].lower()
        name = "%s-%s-%s-%s-%03d" % (p, site[0], code, env.lower(), ci)
        cluster_id = make_id(p, "cluster", ci)
        registry.append(("cluster", cluster_id, NOW))
        n_nodes = rng.choices([1, 2, 3, 4, 5, 6], weights=[15, 30, 25, 12, 10, 8])[0]
        if args.nodes:
            n_nodes = min(n_nodes, args.nodes - node_count)
        hw = rng.choice(HARDWARE)
        os_ = weighted(rng, [(o, o[4]) for o in OSES])
        members = []
        for ni in range(n_nodes):
            nodename = "%sn%d" % (name, ni + 1)
            node_id = make_id(p, "node", "%d.%d" % (ci, ni))
            registry.append(("node", node_id, NOW))
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
                ago(-warranty) if warranty < 0 else Raw("FROM_UNIXTIME(UNIX_TIMESTAMP() + %d)" % warranty),
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
                        add_alert(("check out of bounds", node_id, "", 2,
                                          "%(ctype)s:%(inst)s check value %(val)d. %(ttype)s thresholds: %(min)d - %(max)d",
                                          json.dumps({"ctype": ctype, "inst": inst, "val": value, "ttype": "defaults", "min": low, "max": high}),
                                          "%s:%s" % (ctype, inst), env))
            # The node's own log: registrations, pushes, changes.
            for _ in range(0 if args.no_history else rng.randint(args.node_logs // 2, args.node_logs)):
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
        registry.append(("service", svc_id, NOW))
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
            # An address, then --resources in all on average, drawn by driver group.
            short = svcname.split("/")[-1]
            n_res = max(2, int(round(rng.gauss(args.resources, args.resources / 3))))
            groups = sorted((weighted(rng, RESOURCE_GROUPS) for _ in range(n_res - 1)), key=RESOURCE_ORDER.index)
            res.append(("ip#0", "ip.host", "%s.%s.example.com" % (short, site[0])))
            seen = {"ip": 1}
            for group in groups:
                k = seen.get(group, 0)
                seen[group] = k + 1
                if group == "ip":
                    res.append(("ip#%d" % k, "ip.host", "%s-%d.%s.example.com" % (short, k, site[0])))
                elif group == "disk":
                    res.append(("disk#%d" % k, rng.choice(["disk.lvm", "disk.disk", "disk.zpool"]), "vg_%s%d" % (ns, k)))
                elif group == "fs":
                    res.append(("fs#%d" % k, rng.choice(["fs.xfs", "fs.ext4", "fs.flag"]), "/srv/%s/fs%d" % (short, k)))
                elif group == "container":
                    res.append(("container#%d" % k, "container.podman",
                                "%s/%s:%s" % (ns, rng.choice(COMPONENTS), rng.choice(["1.4", "2.0", "latest"]))))
                elif group == "app":
                    res.append(("app#%d" % k, rng.choice(["app.forking", "app.simple"]), "%s-%s" % (ns, rng.choice(COMPONENTS))))
                elif group == "sync":
                    res.append(("sync#i%d" % k, "sync.rsync", "internal"))
                else:
                    res.append(("task#%d" % k, "task.host", "%s-%s" % (ns, rng.choice(COMPONENTS))))
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
            add_alert(("service unavailable" if avail == "down" else "service available but degraded",
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
            if not args.no_history:
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
                if args.no_history:
                    # The current status period of every resource; activity writes the past ones.
                    resmon_log_last.append((node_id, svc_id, rid, rstatus, ago(rng.randint(day, history // 2)), ago(0), ""))
                elif rng.random() < args.resource_history:
                    resmon_log.append((node_id, svc_id, rid, "up", ago(history), ago(rng.randint(day, history - day)), ""))
                    resmon_log_last.append((node_id, svc_id, rid, rstatus, ago(rng.randint(day, history // 2)), ago(0), ""))
        if args.no_history:
            continue
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

    w.flush()
    out.write("COMMIT;\n")
    out.write("SELECT 'seed_testdata: loaded' AS done;\n")
    sys.stderr.write("rows generated:\n")
    for table, count in sorted(w.counts.items()):
        sys.stderr.write("  %-20s %8d\n" % (table, count))


# The activity of a long service life (activity), generated by the database
# itself from the objects up loaded: INSERT ... SELECT over the sequence engine
# of MariaDB (seq_1_to_N), the random draws hashes of the object and the day
# (CRC32), so that millions of rows cost no transfer and the same load gives
# the same rows. Its working tables, dropped at the end, and by down.
WORK_TABLES = ["zz_seed_n", "zz_seed_s", "zz_seed_i", "zz_seed_r", "zz_seed_pkg", "zz_seed_users", "zz_seed_groups",
               "zz_seed_hw", "zz_seed_rkeys", "zz_seed_at", "zz_seed_ev", "zz_seed_ms", "zz_seed_cm", "zz_seed_dg",
               "zz_seed_sd"]

PACKAGE_STEMS = [
    "bash", "coreutils", "glibc", "openssl", "openssh", "systemd", "kernel", "curl", "wget", "vim", "sudo", "tar",
    "gzip", "bzip2", "xz", "zlib", "zstd", "lz4", "rsync", "git", "python3", "perl", "ruby", "nodejs",
    "java-17-openjdk", "java-21-openjdk", "gcc", "make", "cmake", "binutils", "gdb", "strace", "lsof", "tcpdump",
    "iproute", "iptables", "nftables", "firewalld", "NetworkManager", "chrony", "rsyslog", "logrotate", "cronie",
    "postfix", "dbus", "polkit", "pam", "shadow-utils", "util-linux", "e2fsprogs", "xfsprogs", "lvm2",
    "device-mapper", "mdadm", "multipath-tools", "nfs-utils", "cifs-utils", "samba", "sssd", "krb5", "openldap",
    "cyrus-sasl", "audit", "selinux-policy", "policycoreutils", "ca-certificates", "gnupg2", "rpm", "dnf", "dpkg",
    "grub2", "dracut", "kmod", "pciutils", "usbutils", "dmidecode", "smartmontools", "sysstat", "procps-ng",
    "psmisc", "htop", "iotop", "net-tools", "bind-utils", "dnsmasq", "haproxy", "nginx", "httpd", "php", "mariadb",
    "postgresql", "redis", "memcached", "podman", "containerd", "runc", "buildah", "skopeo", "kubectl", "helm",
    "ansible", "puppet", "opensvc", "zabbix-agent", "node_exporter", "filebeat", "telegraf", "jq", "tmux", "screen",
    "less", "which", "file", "findutils", "grep", "sed", "gawk", "diffutils", "patch", "unzip", "zip", "bc",
    "libxml2", "libxslt", "libyaml", "json-c", "pcre2", "sqlite", "ncurses", "readline", "libffi", "expat", "icu",
    "freetype", "fontconfig", "cairo", "pango", "nss", "nspr", "libcurl", "libssh", "libuv", "protobuf", "grpc",
]

SYSTEM_USERS = [
    ("root", 0), ("bin", 1), ("daemon", 2), ("adm", 3), ("lp", 4), ("sync", 5), ("shutdown", 6), ("halt", 7),
    ("mail", 8), ("operator", 11), ("games", 12), ("ftp", 14), ("nobody", 65534), ("dbus", 81), ("sshd", 74),
    ("rpc", 32), ("rpcuser", 29), ("postfix", 89), ("tcpdump", 72), ("tss", 59), ("chrony", 997),
    ("polkitd", 998), ("systemd-coredump", 999), ("opensvc", 990), ("zabbix", 991), ("backup", 993),
]
SYSTEM_GROUPS = [
    ("root", 0), ("bin", 1), ("daemon", 2), ("sys", 3), ("adm", 4), ("tty", 5), ("disk", 6), ("lp", 7), ("mem", 8),
    ("kmem", 9), ("wheel", 10), ("cdrom", 11), ("mail", 12), ("man", 15), ("dialout", 18), ("floppy", 19),
    ("games", 20), ("tape", 33), ("video", 39), ("ftp", 50), ("lock", 54), ("audio", 63), ("users", 100),
    ("nobody", 65534), ("dbus", 81), ("utmp", 22), ("utempter", 35), ("input", 999), ("kvm", 36), ("render", 998),
    ("systemd-journal", 190), ("sshd", 74), ("rpc", 32), ("postdrop", 90), ("postfix", 89), ("tss", 59),
    ("chrony", 997), ("polkitd", 996), ("opensvc", 990), ("zabbix", 991), ("backup", 993), ("ssh_keys", 995),
]

# hw_type, hw_path, hw_class, hw_description, hw_driver: the hardware of a
# node, each item kept by 80% of them.
HARDWARE_ITEMS = (
    [("cpu", "/0/400/%d" % i, "processor", "Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz", "") for i in range(2)]
    + [("mem", "/0/1000/%d" % i, "memory", "DIMM DDR4 Synchronous Registered 32GiB 3200 MHz", "") for i in range(12)]
    + [("pci", "0000:00:1f.2", "storage", "C620 Series Chipset SATA Controller", "ahci"),
       ("pci", "0000:18:00.0", "storage", "MegaRAID SAS-3 3108 [Invader]", "megaraid_sas"),
       ("pci", "0000:3b:00.0", "network", "Ethernet Controller X710 for 10GbE SFP+", "i40e"),
       ("pci", "0000:3b:00.1", "network", "Ethernet Controller X710 for 10GbE SFP+", "i40e"),
       ("pci", "0000:5e:00.0", "bus", "LPe32000 PCIe Fibre Channel Adapter", "lpfc"),
       ("pci", "0000:5e:00.1", "bus", "LPe32000 PCIe Fibre Channel Adapter", "lpfc"),
       ("pci", "0000:00:14.0", "bus", "C620 Series Chipset USB 3.0 xHCI Controller", "xhci_hcd"),
       ("pci", "0000:03:00.0", "display", "Integrated Matrox G200eW3 Graphics Controller", "mgag200"),
       ("disk", "/dev/sda", "disk", "SSDSC2KB480G8 480GB", "sd"),
       ("disk", "/dev/sdb", "disk", "SSDSC2KB480G8 480GB", "sd"),
       ("nic", "eth0", "network", "eth0 10000Mb/s", "i40e"),
       ("nic", "eth1", "network", "eth1 10000Mb/s", "i40e")])

# Driver group, keys: the information the agents push about their resources.
RESOURCE_KEYS = {
    "fs": ["mnt", "dev", "fs_type", "size", "used"],
    "ip": ["ipaddr", "ipdev", "netmask", "name"],
    "disk": ["name", "devs", "size"],
    "app": ["script", "start", "stop", "check", "timeout"],
    "container": ["name", "image", "hostname", "run_args"],
    "sync": ["src", "dst", "schedule", "last_sync"],
    "task": ["command", "schedule", "last_run", "user"],
}

# Scope, type, instance (None: the name of the object), severity, format,
# dict, weight: the alerts raised over the history.
ALERT_TYPES = [
    ("node", "check out of bounds", "fs_u:/var", 2,
     "%(ctype)s:%(inst)s check value %(val)d. %(ttype)s thresholds: %(min)d - %(max)d",
     {"ctype": "fs_u", "inst": "/var", "val": 95, "ttype": "defaults", "min": 0, "max": 90}, 30),
    ("node", "check out of bounds", "fs_u:/", 2,
     "%(ctype)s:%(inst)s check value %(val)d. %(ttype)s thresholds: %(min)d - %(max)d",
     {"ctype": "fs_u", "inst": "/", "val": 93, "ttype": "defaults", "min": 0, "max": 90}, 15),
    ("node", "check out of bounds", "fs_u:/srv/data", 2,
     "%(ctype)s:%(inst)s check value %(val)d. %(ttype)s thresholds: %(min)d - %(max)d",
     {"ctype": "fs_u", "inst": "/srv/data", "val": 97, "ttype": "defaults", "min": 0, "max": 90}, 15),
    ("node", "check out of bounds", "vg_u:datavg", 2,
     "%(ctype)s:%(inst)s check value %(val)d. %(ttype)s thresholds: %(min)d - %(max)d",
     {"ctype": "vg_u", "inst": "datavg", "val": 98, "ttype": "defaults", "min": 0, "max": 95}, 10),
    ("node", "check value not updated", "fs_u:/opt", 1, "%(ctype)s:%(inst)s check value not updated",
     {"ctype": "fs_u", "inst": "/opt"}, 10),
    ("node", "node maintenance expired", None, 1, "node hardware maintenance expired", {}, 5),
    ("node", "netmask misconfigured", None, 1, "%(n)s IP addresses with a misconfigured netmask", {"n": 1}, 5),
    ("node", "os obsolescence warning", None, 1, "%(o)s support ends in %(a)s days", {"o": "RHEL 8", "a": 90}, 10),
    ("svc", "service unavailable", None, 3, "current availability status: %(s)s", {"s": "down"}, 25),
    ("svc", "service available but degraded", None, 2, "current overall status: %(s)s", {"s": "warn"}, 30),
    ("svc", "action errors", None, 3, "%(err)s action errors", {"err": "1"}, 25),
    ("svc", "service frozen", None, 1, "service frozen", {}, 8),
    ("svc", "instance status not updated", None, 2, "instance status not updated for %(h)s hours", {"h": 2}, 7),
    ("svc", "flex error", None, 2, "%(n)d instances up, out of %(smin)d - %(smax)d",
     {"n": 1, "smin": 2, "smax": 4}, 5),
]

# Moduleset, modules: base is attached to every node, one of the others too.
MODULESETS = [
    ("base", ["sysctl", "ntp", "dns", "users", "groups", "sshd", "motd", "packages"]),
    ("security", ["auditd", "pam_policy", "passwd_policy", "sudoers", "firewall", "selinux"]),
    ("oracle", ["ora_kernel", "ora_users", "ora_limits", "hugepages"]),
    ("postgres", ["pg_kernel", "pg_users", "pg_hba"]),
    ("web", ["nginx_conf", "tls_certs", "logrotate_web"]),
    ("monitoring", ["mon_agent", "snmp", "syslog_fwd"]),
    ("backup", ["bkp_agent", "bkp_schedule"]),
    ("k8s", ["k8s_kernel", "k8s_runtime", "k8s_registry", "k8s_cni"]),
]

# Vendor, model: the storage arrays, with their disk groups.
ARRAY_MODELS = [("HITACHI", "VSP G700"), ("EMC", "PowerMax 2000"), ("PURE", "FlashArray//X70"),
                ("HPE", "3PAR 8440"), ("NETAPP", "AFF A400"), ("IBM", "FlashSystem 7300")]
DISK_GROUPS = ["dg_ssd01", "dg_ssd02", "dg_sas01", "dg_sas02", "dg_nl01", "dg_rep01"]

ACTION_STATUS = [("ok", 92), ("warn", 3), ("err", 5)]


class Script:
    """Writes SQL statements, their $names substituted."""

    def __init__(self, out, **names):
        self.out = out
        self.names = names

    def __call__(self, text, **names):
        self.out.write(string.Template(text).substitute(self.names, **names).strip().rstrip(";") + ";\n")

    def step(self, text):
        """Reports the progress of the load, as the mysql client prints the results."""
        self.out.write("SELECT CONCAT(NOW(), ' %s') AS step;\n" % text.replace("'", ""))


def lit(value):
    return sql(value)


def pick(expr, values):
    """One of values, drawn by the integer expr."""
    return "ELT(1 + (%s) %% %d, %s)" % (expr, len(values), ", ".join(lit(v) for v in values))


def wpick(expr, choices):
    """One of the (value, weight) choices, drawn by the integer expr."""
    total = sum(w for _, w in choices)
    parts, acc = [], 0
    for value, weight in choices[:-1]:
        acc += weight
        parts.append("WHEN (%s) %% %d < %d THEN %s" % (expr, total, acc, lit(value)))
    return "CASE %s ELSE %s END" % (" ".join(parts), lit(choices[-1][0]))


def variants(bucket, choices):
    """{column: CASE expression}: the columns of one of the (weight, {column: SQL
    expression}) choices, drawn by bucket, an expression from 0 to 99."""
    exprs = {}
    for column in choices[0][1]:
        parts, acc = [], 0
        for weight, values in choices[:-1]:
            acc += weight
            parts.append("WHEN %s < %d THEN %s" % (bucket, acc, values[column]))
        exprs[column] = "CASE %s ELSE %s END" % (" ".join(parts), choices[-1][1][column])
    return exprs


def uuid_of(expr):
    """A uuid formatted md5 of the SQL expression."""
    m = "MD5(%s)" % expr
    return "CONCAT_WS('-', SUBSTR(%s, 1, 8), SUBSTR(%s, 9, 4), SUBSTR(%s, 13, 4), SUBSTR(%s, 17, 4), SUBSTR(%s, 21, 12))" % (
        (m,) * 5)


def day_chunks(first, last, size):
    """The [lo, hi] ranges of days ago covering first to last by size, the oldest first:
    the ids of the rows then grow with their dates, as when an agent sends them."""
    hi = last
    while hi >= first:
        lo = max(first, hi - size + 1)
        yield lo, hi
        hi = lo - 1


def rate_draw(rate):
    """The multiplicity and the threshold (out of 10000) of a mean of rate a day."""
    k = max(1, int(math.ceil(rate)))
    return k, int(round(rate / k * 10000))


def segments(q, table, columns, source, key, pmin, pmax, max_dur, values, ret, chunk):
    """Contiguous status periods of each row of source, back over ret days: periods
    of pmin to pmax days, each starting with an incident of at most max_dur seconds
    (part 0), followed by the return to normal (part 1). values selects the columns
    from s, where $begin and $end are the dates of the period and s.hb draws the
    incident."""
    maxk = max(1, ret // pmin)
    keyx = ", ".join("x.%s" % c for c in key)
    keyg = ", ".join("g.%s" % c for c in key)
    hi = ret + 1
    while hi > 0:
        lo = max(0, hi - chunk)
        q.step("%s: periods from %d to %d days ago" % (table, hi, lo))
        q("""
INSERT INTO $table ($columns)
SELECT $values FROM (
  SELECT g.*, p.seq AS part, (k.seq + 1) * g.P * 86400 + g.o AS t1, k.seq * g.P * 86400 + g.o AS t0,
    60 + CRC32(CONCAT_WS(':', 'dur', $keyg, k.seq)) % LEAST(g.P * 21600, $max_dur) AS dur,
    CRC32(CONCAT_WS(':', 'bad', $keyg, k.seq)) AS hb
  FROM (
    SELECT x.*, $pmin + CRC32(CONCAT_WS(':', 'P', $keyx)) % $prange AS P, CRC32(CONCAT_WS(':', 'o', $keyx)) % 86400 AS o
    FROM ($source) x
  ) g
  JOIN seq_1_to_$maxk k ON k.seq BETWEEN GREATEST(1, FLOOR($lo / g.P) - 1) AND CEIL($hi / g.P) AND (k.seq + 1) * g.P <= $ret
  JOIN seq_0_to_1 p
) s
WHERE s.t1 > $lo * 86400 AND s.t1 <= $hi * 86400
ORDER BY s.t1 DESC, s.part""", table=table, columns=columns, source=source, keyx=keyx, keyg=keyg, pmin=pmin,
          prange=pmax - pmin + 1, max_dur=max_dur, maxk=maxk, lo=lo, hi=hi, ret=ret,
          values=string.Template(values).substitute(
              begin="FROM_UNIXTIME(UNIX_TIMESTAMP() - IF(s.part = 0, s.t1, s.t1 - s.dur))",
              end="FROM_UNIXTIME(UNIX_TIMESTAMP() - IF(s.part = 0, s.t1 - s.dur, s.t0))"))
        hi = lo


def activity(args, out):
    """The activity of --days of service of the objects loaded by up."""
    rng = random.Random(args.seed)
    p = args.prefix
    hist = args.days
    ret = min(args.retention, hist)
    chunk = args.chunk_days
    q = Script(out, p=p, ret=ret, hist=hist, reg=REGISTRY)
    w = Writer(out)

    out.write("-- Activity generated by seed_testdata.py: prefix %r, %d days, %d of retention.\n" % (p, hist, ret))
    out.write("SET NAMES utf8mb4;\n")
    # Once only, and after up: fails without the registry, or with this marker in it.
    q("INSERT INTO `$reg` (kind, id, created) VALUES ('activity', 'loaded', NOW())")

    # Working tables: the objects loaded, numbered, and the templates of the rows.
    q.step("working tables")
    q("DROP TABLE IF EXISTS %s" % ", ".join(WORK_TABLES))
    q("""
CREATE TABLE zz_seed_n (idx INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (idx), UNIQUE KEY (node_id), KEY (cluster_id))
SELECT n.node_id, n.nodename, n.cluster_id, n.app, n.node_env AS env, n.team_responsible AS team, n.os_name,
  n.os_vendor, n.os_release, n.type AS hw
FROM nodes n JOIN `$reg` r ON r.kind = 'node' AND r.id = n.node_id ORDER BY n.nodename""")
    q("""
CREATE TABLE zz_seed_s (idx INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (idx), UNIQUE KEY (svc_id))
SELECT s.svc_id, s.svcname, SUBSTRING_INDEX(s.svcname, '/', 1) AS ns,
  SUBSTRING_INDEX(SUBSTRING_INDEX(s.svcname, '/', 2), '/', -1) AS kind, s.svc_app AS app, s.svc_env AS env,
  s.svc_topology AS topology, s.cluster_id, (SELECT COUNT(*) FROM svcmon m WHERE m.svc_id = s.svc_id) AS n_inst
FROM services s JOIN `$reg` r ON r.kind = 'service' AND r.id = s.svc_id ORDER BY s.svcname""")
    q("""
CREATE TABLE zz_seed_i (idx INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (idx), UNIQUE KEY (svc_id, rk), KEY (node_id))
SELECT m.svc_id, ROW_NUMBER() OVER (PARTITION BY m.svc_id ORDER BY m.node_id) AS rk, m.node_id, n.nodename,
  n.cluster_id, s.svcname, s.ns, s.kind, s.app
FROM svcmon m JOIN zz_seed_s s ON s.svc_id = m.svc_id JOIN zz_seed_n n ON n.node_id = m.node_id
ORDER BY s.idx, n.idx""")
    q("""
CREATE TABLE zz_seed_r (idx INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (idx), KEY (svc_id))
SELECT r.node_id, r.svc_id, r.rid, r.res_type, SUBSTRING_INDEX(r.res_type, '.', 1) AS grp,
  SUBSTRING_INDEX(r.res_desc, ' ', -1) AS target, s.svcname, s.ns, s.topology
FROM resmon r JOIN zz_seed_s s ON s.svc_id = r.svc_id ORDER BY s.idx, r.node_id, r.rid""")

    # Packages: the most common first, so that a node has the first 300 to all of them.
    variants_ = [(v % stem, 1 if v.startswith(("python3-", "perl-")) or v.endswith("-common") else 0)
                 for stem in PACKAGE_STEMS
                 for v in ["lib%s", "%s-libs", "%s-devel", "%s-common", "python3-%s", "perl-%s"]]
    rng.shuffle(variants_)
    packages = [(s, 0) for s in PACKAGE_STEMS] + variants_
    q("CREATE TABLE zz_seed_pkg (i INT PRIMARY KEY, name VARCHAR(100), version VARCHAR(40), noarch TINYINT)")
    w.insert("zz_seed_pkg", ["i", "name", "version", "noarch"],
             [(i + 1, name, "%d.%d.%d" % (rng.randint(0, 12), rng.randint(0, 30), rng.randint(0, 20)), noarch)
              for i, (name, noarch) in enumerate(packages)])
    q("CREATE TABLE zz_seed_users (name VARCHAR(64), uid INT)")
    w.insert("zz_seed_users", ["name", "uid"], SYSTEM_USERS)
    q("CREATE TABLE zz_seed_groups (name VARCHAR(64), gid INT)")
    w.insert("zz_seed_groups", ["name", "gid"], SYSTEM_GROUPS)
    q("""CREATE TABLE zz_seed_hw (i INT PRIMARY KEY, hw_type VARCHAR(16), hw_path VARCHAR(32), hw_class VARCHAR(128),
  hw_description VARCHAR(128), hw_driver VARCHAR(32))""")
    w.insert("zz_seed_hw", ["i", "hw_type", "hw_path", "hw_class", "hw_description", "hw_driver"],
             [(i + 1,) + item for i, item in enumerate(HARDWARE_ITEMS)])
    q("CREATE TABLE zz_seed_rkeys (grp VARCHAR(16), res_key VARCHAR(32))")
    w.insert("zz_seed_rkeys", ["grp", "res_key"], [(g, k) for g, keys in RESOURCE_KEYS.items() for k in keys])
    q("""CREATE TABLE zz_seed_at (scope VARCHAR(4), lo INT, hi INT, dash_type VARCHAR(60), inst VARCHAR(100),
  sev INT, fmt VARCHAR(100), dict TEXT)""")
    rows, acc = [], {"node": 0, "svc": 0}
    for scope, dash_type, inst, sev, fmt, d, weight in ALERT_TYPES:
        rows.append((scope, acc[scope], acc[scope] + weight, dash_type, inst, sev, fmt, json.dumps(d)))
        acc[scope] += weight
    w.insert("zz_seed_at", ["scope", "lo", "hi", "dash_type", "inst", "sev", "fmt", "dict"], rows)

    # Inventory: what the agents push about their nodes, now.
    # Statements on a share of the objects each (idx modulo parts), not to
    # hold millions of rows in a single transaction.
    parts = args.parts
    n_pkg = len(packages)
    for part in range(parts):
        q.step("packages: part %d/%d" % (part + 1, parts))
        q("""
INSERT INTO packages (node_id, pkg_name, pkg_version, pkg_arch, pkg_type, pkg_install_date, pkg_updated, pkg_sig)
SELECT n.node_id, k.name,
  CONCAT(k.version, '-', 1 + (CRC32(CONCAT_WS(':', 'pv', n.node_id, k.i)) % 25 = 0), CASE n.os_vendor
    WHEN 'Red Hat' THEN CONCAT('.el', SUBSTRING_INDEX(n.os_release, '.', 1)) WHEN 'Ubuntu' THEN CONCAT('ubuntu0.', n.os_release)
    WHEN 'Debian' THEN CONCAT('+deb', n.os_release, 'u1') WHEN 'SUSE' THEN '.150500' ELSE '' END),
  CASE WHEN n.os_vendor IN ('Ubuntu', 'Debian') THEN IF(k.noarch, 'all', 'amd64')
    WHEN n.os_name = 'Linux' THEN IF(k.noarch, 'noarch', 'x86_64') WHEN n.os_name = 'SunOS' THEN 'sparc' ELSE 'ppc' END,
  CASE WHEN n.os_vendor IN ('Ubuntu', 'Debian') THEN 'deb' WHEN n.os_name = 'Linux' THEN 'rpm'
    WHEN n.os_name = 'SunOS' THEN 'pkg' ELSE 'lpp' END,
  FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT_WS(':', 'pi', n.node_id, k.i)) % ($hist * 86400))),
  FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT('pu', n.node_id)) % 3600)),
  IF(n.os_vendor IN ('Red Hat', 'SUSE'), LEFT(MD5(n.os_vendor), 16), NULL)
FROM zz_seed_n n JOIN zz_seed_pkg k ON k.i <= 300 + CRC32(CONCAT('pk', n.node_id)) % $span
WHERE n.idx % $parts = $part""", span=n_pkg - 299, parts=parts, part=part)
    q.step("patches, accounts, hardware, hbas")
    q("""
INSERT INTO patches (node_id, patch_num, patch_rev, patch_install_date, patch_updated)
SELECT n.node_id, CONCAT(IF(n.os_name = 'SunOS', '15', 'IJ'), LPAD(1000 + k.seq * 37, 6, '0')),
  LPAD(1 + CRC32(CONCAT_WS(':', 'pr', n.node_id, k.seq)) % 20, 2, '0'),
  FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT_WS(':', 'pd', n.node_id, k.seq)) % ($hist * 86400))), NOW()
FROM zz_seed_n n JOIN seq_1_to_300 k ON k.seq <= 100 + CRC32(CONCAT('np', n.node_id)) % 200
WHERE n.os_name IN ('SunOS', 'AIX')""")
    q("""
INSERT IGNORE INTO node_users (node_id, user_name, user_id, updated)
SELECT n.node_id, u.name, u.uid, FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT('uu', n.node_id)) % 3600))
FROM zz_seed_n n JOIN zz_seed_users u
UNION ALL
SELECT n.node_id, CONCAT(LOWER(REPLACE(n.app, '-', '')), 'adm'), 5000 + CRC32(n.app) % 1000, NOW() FROM zz_seed_n n
UNION ALL
SELECT x.node_id, CONCAT('u', LPAD(x.u, 5, '0')), 10000 + x.u, NOW() FROM (
  SELECT n.node_id, 1 + CRC32(CONCAT_WS(':', 'hu', n.node_id, k.seq)) % 3000 AS u
  FROM zz_seed_n n JOIN seq_1_to_40 k ON k.seq <= 5 + CRC32(CONCAT('nu', n.node_id)) % 36
) x""")
    q("""
INSERT IGNORE INTO node_groups (node_id, group_name, group_id, updated)
SELECT n.node_id, g.name, g.gid, FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT('uu', n.node_id)) % 3600))
FROM zz_seed_n n JOIN zz_seed_groups g
UNION ALL
SELECT n.node_id, LOWER(REPLACE(n.app, '-', '')), 5000 + CRC32(n.app) % 1000, NOW() FROM zz_seed_n n
UNION ALL
SELECT n.node_id, LOWER(REPLACE(n.team, ' ', '_')), 6000 + CRC32(n.team) % 1000, NOW() FROM zz_seed_n n
UNION ALL
SELECT x.node_id, CONCAT('u', LPAD(x.u, 5, '0')), 10000 + x.u, NOW() FROM (
  SELECT n.node_id, 1 + CRC32(CONCAT_WS(':', 'hu', n.node_id, k.seq)) % 3000 AS u
  FROM zz_seed_n n JOIN seq_1_to_40 k ON k.seq <= 5 + CRC32(CONCAT('nu', n.node_id)) % 36
) x""")
    q("""
INSERT INTO node_hw (node_id, hw_type, hw_path, hw_class, hw_description, hw_driver, updated)
SELECT n.node_id, h.hw_type, h.hw_path, h.hw_class, h.hw_description, h.hw_driver, NOW()
FROM zz_seed_n n JOIN zz_seed_hw h ON CRC32(CONCAT_WS(':', 'hw', n.node_id, h.i)) % 10 < 8
ORDER BY n.idx, h.i""")
    q("""
INSERT INTO node_hba (node_id, hba_id, hba_type, updated)
SELECT x.node_id, LOWER(CONCAT_WS(':', '10', '00', SUBSTR(x.m, 1, 2), SUBSTR(x.m, 3, 2), SUBSTR(x.m, 5, 2),
  SUBSTR(x.m, 7, 2), SUBSTR(x.m, 9, 2), SUBSTR(x.m, 11, 2))), 'fc', NOW()
FROM (SELECT n.node_id, MD5(CONCAT_WS(':', 'hba', n.node_id, k.seq)) AS m
  FROM zz_seed_n n JOIN seq_1_to_4 k ON k.seq <= 2 + 2 * (CRC32(CONCAT('nh', n.node_id)) % 2)
  WHERE n.hw = 'physical') x""")

    # Inventory: what the agents push about their resources.
    q.step("resinfo")
    values = {
        "fs.mnt": "r.target",
        "fs.dev": "CONCAT('/dev/mapper/vg_', r.ns, '-lv', SUBSTRING_INDEX(r.rid, '#', -1))",
        "fs.fs_type": "SUBSTRING_INDEX(r.res_type, '.', -1)",
        "fs.size": "CONCAT(1 + x.hv % 2000, 'G')",
        "fs.used": "CONCAT(x.hv % 100, '%')",
        "ip.ipaddr": "CONCAT('10.', 200 + x.hv % 40, '.', (x.hv DIV 64) % 256, '.', 1 + (x.hv DIV 16384) % 254)",
        "ip.ipdev": pick("x.hv", ["bond0", "eth0", "eth1"]),
        "ip.netmask": "'24'",
        "ip.name": "r.target",
        "disk.name": "r.target",
        "disk.devs": "CONCAT('/dev/mapper/36', LEFT(MD5(CONCAT(r.svc_id, r.rid)), 30))",
        "disk.size": "CONCAT(10 * (1 + x.hv % 200), 'G')",
        "app.script": "CONCAT('/srv/', r.ns, '/bin/', r.target, '.sh')",
        "app.start": "CONCAT(10 + x.hv % 90)",
        "app.stop": "CONCAT(10 + x.hv % 90)",
        "app.check": "CONCAT(10 + x.hv % 90)",
        "app.timeout": "CONCAT(30 * (1 + x.hv % 10), 's')",
        "container.name": "CONCAT(REPLACE(SUBSTRING_INDEX(r.svcname, '/', -1), '-', '_'), '.', REPLACE(r.rid, '#', ''))",
        "container.image": "r.target",
        "container.hostname": "SUBSTRING_INDEX(r.svcname, '/', -1)",
        "container.run_args": "'--net=host -v /srv:/srv:rw'",
        "sync.src": "CONCAT('/srv/', SUBSTRING_INDEX(r.svcname, '/', -1))",
        "sync.dst": "CONCAT('/srv/', SUBSTRING_INDEX(r.svcname, '/', -1))",
        "sync.schedule": "'@60'",
        "sync.last_sync": "DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP() - (x.hv % 3600)), '%Y-%m-%d %H:%i:%s')",
        "task.command": "CONCAT('/srv/', r.ns, '/bin/', r.target, '-job.sh')",
        "task.schedule": pick("x.hv", ["@1h", "00:00-02:00", "@10m", "sun 03:00"]),
        "task.last_run": "DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP() - (x.hv % 86400)), '%Y-%m-%d %H:%i:%s')",
        "task.user": "'root'",
    }
    value = "CASE CONCAT(x.grp, '.', x.res_key) %s ELSE '' END" % " ".join(
        "WHEN %s THEN %s" % (lit(k), v) for k, v in values.items())
    for part in range(parts * 2):
        q.step("resinfo: part %d/%d" % (part + 1, parts * 2))
        q("""
INSERT INTO resinfo (node_id, svc_id, rid, res_key, res_value, topology, updated)
SELECT r.node_id, r.svc_id, r.rid, x.res_key, $value, r.topology, FROM_UNIXTIME(UNIX_TIMESTAMP() - (x.hv % 600))
FROM (SELECT r.idx, r.grp, k.res_key, CRC32(CONCAT_WS(':', 'ri', r.idx, k.res_key)) AS hv
  FROM zz_seed_r r JOIN zz_seed_rkeys k ON k.grp = r.grp WHERE r.idx % $parts = $part) x
JOIN zz_seed_r r ON r.idx = x.idx
ORDER BY r.idx""", value=value, parts=parts * 2, part=part)

    # Storage: arrays, their disk groups, the disks of the services on them.
    if args.arrays:
        q.step("storage")
        arrays = []
        for i in range(args.arrays):
            vendor, model = ARRAY_MODELS[i % len(ARRAY_MODELS)]
            name = "%s-%s-%s%02d" % (p, SITES[i % len(SITES)][0], model.split()[0].split("/")[0].lower(), i)
            arrays.append((name, model, 256 * 1024 * rng.randint(1, 4), "%d.%d.%d" % (rng.randint(5, 9), rng.randint(0, 9),
                                                                                       rng.randint(0, 99)), 0,
                           "%s array (test data)" % vendor))
        w.insert(REGISTRY, ["kind", "id", "created"], [("array", a[0], NOW) for a in arrays])
        w.insert("stor_array", ["array_name", "array_model", "array_cache", "array_firmware", "array_level",
                                "array_comment"], arrays)
        q("""
INSERT INTO stor_array_dg (array_id, dg_name, dg_size, dg_used, dg_free, dg_reserved)
SELECT x.id, x.dg, x.size, x.size * x.pct DIV 100, x.size - x.size * x.pct DIV 100, x.size DIV 20
FROM (SELECT a.id, g.dg, 1048576 * (20 + CRC32(CONCAT(a.array_name, g.dg)) % 80) AS size,
    30 + CRC32(CONCAT('pct', a.array_name, g.dg)) % 60 AS pct
  FROM stor_array a JOIN `$reg` r ON r.kind = 'array' AND r.id = a.array_name
  JOIN ($dgs) g) x""", dgs=" UNION ALL ".join("SELECT %s AS dg" % lit(dg) for dg in DISK_GROUPS))
        q("""
CREATE TABLE zz_seed_dg (idx INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (idx))
SELECT dg.id, dg.array_id, a.array_name, a.array_model, dg.dg_name, dg.dg_size
FROM stor_array_dg dg JOIN stor_array a ON CAST(a.id AS CHAR) = dg.array_id
JOIN `$reg` r ON r.kind = 'array' AND r.id = a.array_name ORDER BY a.array_name, dg.dg_name""")
        q("""
CREATE TABLE zz_seed_sd (KEY (svc_id))
SELECT s.svc_id, s.svcname, s.app, s.ns, k.seq AS k,
  UPPER(CONCAT('6', LEFT(MD5(CONCAT_WS(':', 'disk', s.svc_id, k.seq)), 31))) AS disk_id,
  g.array_name, g.array_model, g.dg_name, 10240 * (1 + CRC32(CONCAT_WS(':', 'dsz', s.svc_id, k.seq)) % 200) AS size
FROM zz_seed_s s JOIN seq_1_to_6 k ON k.seq <= 1 + CRC32(CONCAT('nd', s.svc_id)) % 6
JOIN zz_seed_dg g ON g.idx = 1 + CRC32(CONCAT_WS(':', 'dg', s.svc_id, k.seq)) % $n_dg
WHERE s.kind = 'vol' OR EXISTS (SELECT 1 FROM zz_seed_r r WHERE r.svc_id = s.svc_id AND r.grp = 'disk')""",
          n_dg=args.arrays * len(DISK_GROUPS))
        vendor = "CASE d.array_model %s END" % " ".join(
            "WHEN %s THEN %s" % (lit(m), lit(v)) for v, m in ARRAY_MODELS)
        q("""
INSERT INTO svcdisks (disk_id, disk_size, disk_vendor, disk_model, disk_dg, disk_updated, disk_local, disk_used,
  disk_region, app_id, node_id, svc_id)
SELECT d.disk_id, d.size, $vendor, d.array_model, CONCAT('vg_', d.ns), NOW(), 'F',
  d.size * (20 + CRC32(d.disk_id) % 75) DIV 100, '0', ap.id, i.node_id, i.svc_id
FROM zz_seed_sd d JOIN zz_seed_i i ON i.svc_id = d.svc_id LEFT JOIN apps ap ON ap.app = d.app
ORDER BY i.idx, d.k""", vendor=vendor)
        q("""
INSERT INTO diskinfo (disk_id, disk_devid, disk_arrayid, disk_updated, disk_raid, disk_size, disk_group, disk_level,
  disk_name, disk_alloc, disk_created)
SELECT d.disk_id, UPPER(SUBSTR(MD5(d.disk_id), 1, 4)), d.array_name, NOW(), 'RAID6', d.size, d.dg_name, 0,
  CONCAT(REPLACE(SUBSTRING_INDEX(d.svcname, '/', -1), '-', '_'), '_', d.k), d.size,
  FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT('dc', d.disk_id)) % ($hist * 86400)))
FROM zz_seed_sd d""")

    # Heartbeats between the nodes of each cluster.
    q.step("heartbeats")
    hb_source = """SELECT a.cluster_id, a.node_id, b.node_id AS peer_node_id, ELT(k.seq, 'hb#1.rx', 'hb#1.tx', 'hb#2.rx',
  'hb#2.tx') AS name, IF(k.seq <= 2, ':10011', CONCAT('/dev/mapper/hb_', LEFT(a.cluster_id, 8))) AS `desc`,
  IF(k.seq <= 2, 'unicast', 'disk') AS driver
FROM zz_seed_n a JOIN zz_seed_n b ON b.cluster_id = a.cluster_id AND b.node_id <> a.node_id JOIN seq_1_to_4 k"""
    q("""
INSERT INTO hbmon (cluster_id, node_id, peer_node_id, driver, name, `desc`, state, beating, last_beating, updated)
SELECT x.cluster_id, x.node_id, x.peer_node_id, x.driver, x.name, x.`desc`, 'running', 1, NOW(), NOW() FROM ($src) x""",
      src=hb_source)
    q("""
INSERT INTO hbmon_log_last (cluster_id, node_id, peer_node_id, name, state, beating, begin, end, updated)
SELECT x.cluster_id, x.node_id, x.peer_node_id, x.name, 'running', 1,
  FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT_WS(':', 'hl', x.node_id, x.peer_node_id, x.name)) % 2592000)), NOW(), NOW()
FROM ($src) x""", src=hb_source)
    segments(q, "hbmon_log", "cluster_id, node_id, peer_node_id, name, `desc`, state, beating, begin, end", hb_source,
             ["node_id", "peer_node_id", "name"], 30, 180, 3600,
             "s.cluster_id, s.node_id, s.peer_node_id, s.name, s.`desc`, "
             "IF(s.part = 0 AND s.hb % 4 = 0, 'stopped', 'running'), IF(s.part = 0, 0, 1), $begin, $end",
             ret, chunk * 4)

    # Compliance: modulesets, their attachment to the nodes, their runs.
    q.step("compliance")
    w.insert(REGISTRY, ["kind", "id", "created"], [("modset", "%s-%s" % (p, m), NOW) for m, _ in MODULESETS])
    w.insert("comp_moduleset", ["modset_name", "modset_author"],
             [("%s-%s" % (p, m), "admin@example.com") for m, _ in MODULESETS])
    for m, modules in MODULESETS:
        q("""
INSERT INTO comp_moduleset_modules (modset_id, modset_mod_name, modset_mod_author, autofix)
SELECT m.id, x.name, 'admin@example.com', 'F' FROM comp_moduleset m JOIN ($mods) x WHERE m.modset_name = $name""",
          mods=" UNION ALL ".join("SELECT %s AS name" % lit(mod) for mod in modules), name=lit("%s-%s" % (p, m)))
    q("""
CREATE TABLE zz_seed_ms (idx INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (idx))
SELECT m.id, m.modset_name, m.modset_name = '$p-base' AS base
FROM comp_moduleset m JOIN `$reg` r ON r.kind = 'modset' AND r.id = m.modset_name
ORDER BY m.modset_name = '$p-base' DESC, m.modset_name""")
    q("""
CREATE TABLE zz_seed_cm (KEY (node_id))
SELECT n.node_id, n.idx AS nidx, ms.id AS modset_id, mm.modset_mod_name AS module
FROM zz_seed_n n JOIN zz_seed_ms ms ON ms.base OR ms.idx = 2 + CRC32(CONCAT('ms', n.node_id)) % $extra
JOIN comp_moduleset_modules mm ON mm.modset_id = ms.id""", extra=len(MODULESETS) - 1)
    q("""
INSERT INTO comp_node_moduleset (modset_id, modset_mod_author, modset_updated, node_id)
SELECT DISTINCT modset_id, 'admin@example.com', FROM_UNIXTIME(UNIX_TIMESTAMP() - (CRC32(CONCAT('ma', node_id)) % ($hist * 86400))),
  node_id FROM zz_seed_cm""")
    status = "IF(x.hs % 100 < 85, 0, IF(x.hs % 100 < 95, 1, 2))"
    run_log = ("CASE %s WHEN 0 THEN CONCAT('check: ', x.module, ': all rules compliant') "
               "WHEN 1 THEN CONCAT('check: ', x.module, ': ', 1 + x.hs %% 5, ' rules not compliant') "
               "ELSE CONCAT('check: ', x.module, ': not applicable') END") % status
    q("""
INSERT INTO comp_status (run_module, run_status, run_log, run_date, run_action, node_id, svc_id)
SELECT x.module, $status, $run_log, FROM_UNIXTIME(UNIX_TIMESTAMP() - (x.hs % 86400)), 'check', x.node_id, ''
FROM (SELECT c.*, CRC32(CONCAT_WS(':', 'cs', c.node_id, c.module)) AS hs FROM zz_seed_cm c) x""",
      status=status, run_log=run_log)
    for lo, hi in day_chunks(1, ret, chunk * 2):
        q.step("comp_log: days %d to %d ago" % (hi, lo))
        q("""
INSERT INTO comp_log (run_module, run_status, run_log, run_date, run_action, node_id, svc_id)
SELECT x.module, $status, $run_log, x.run_date, IF(x.hs % 100 = 99, 'fix', 'check'), x.node_id, ''
FROM (SELECT c.node_id, c.module, CRC32(CONCAT_WS(':', 'cl', c.node_id, c.module, d.seq)) AS hs,
    FROM_UNIXTIME(UNIX_TIMESTAMP() - (d.seq * 86400 + CRC32(CONCAT('ct', c.node_id)) % 86400 - CRC32(c.module) % 300)) AS run_date
  FROM zz_seed_cm c JOIN seq_${lo}_to_${hi} d ON (d.seq + CRC32(CONCAT('co', c.node_id))) % $interval = 0) x
ORDER BY x.run_date""", status=status, run_log=run_log, lo=lo, hi=hi, interval=args.comp_interval)
    for part in range(parts):
        q.step("comp_log_daily: part %d/%d" % (part + 1, parts))
        q("""
INSERT IGNORE INTO comp_log_daily (run_module, run_status, run_date, node_id, svc_id)
SELECT c.run_module, c.run_status, DATE(c.run_date), c.node_id, c.svc_id
FROM zz_seed_n n JOIN comp_log c ON c.node_id = n.node_id WHERE n.idx % $parts = $part ORDER BY c.id""",
          parts=parts, part=part)

    # Agent actions, each with its log lines.
    k, thr = rate_draw(args.actions_per_day)
    action = wpick("x.h1 DIV 10000", ACTIONS)
    status = wpick("x.h1 DIV 10000000", ACTION_STATUS)
    for lo, hi in day_chunks(1, ret, chunk):
        q.step("svcactions: days %d to %d ago" % (hi, lo))
        q("""
INSERT INTO svcactions (node_id, svc_id, action, status, begin, end, time, cron, sid, status_log, command, origin,
  log_type, ack, acked_by, acked_comment, acked_date)
SELECT a.node_id, a.svc_id, a.action, a.status,
  IF(l.seq = 0, a.b, a.b + INTERVAL l.seq SECOND), a.b + INTERVAL a.took SECOND, IF(l.seq = 0, a.took, NULL),
  a.cron, a.sid,
  IF(l.seq = 0, CONCAT(a.svcname, ' instance ', a.action), CASE a.status
    WHEN 'ok' THEN CONCAT('instance: ', a.svcname, ': <<< done ', a.action, ' [/usr/bin/', a.command, '] in ', a.took, 's')
    WHEN 'err' THEN CONCAT('instance: ', a.svcname, ': app#0: exit code 1')
    ELSE CONCAT('instance: ', a.svcname, ': app#0: took longer than expected') END),
  a.command, a.origin, IF(l.seq = 0, 'status', ''),
  IF(a.status = 'err', a.acked, NULL),
  IF(a.acked, CONCAT(a.ns, '.admin@example.com'), NULL),
  IF(a.acked, CONCAT('known issue, fixed by ', IF(a.action <> 'restart', 'restart', 'hand')), NULL),
  IF(a.acked, a.b + INTERVAL (a.took + 3600) SECOND, NULL)
FROM (
  SELECT c.*, c.action = 'push resinfo' AS cron, CONCAT('om ', c.svcname, ' ', c.action) AS command,
    IF(c.action = 'push resinfo', 'daemon/scheduler', $origin) AS origin,
    c.status = 'err' AND (c.h2 DIV 1000000) % 10 < 4 AS acked
  FROM (
    SELECT x.*, $action AS action, $status AS status,
      FROM_UNIXTIME(UNIX_TIMESTAMP() - (x.d * 86400 + x.h2 % 86400)) AS b, 1 + (x.h2 DIV 86400) % 180 AS took, $sid AS sid
    FROM (
      SELECT s.svc_id, s.svcname, s.ns, i.node_id, d.seq AS d, k.seq AS k,
        CRC32(CONCAT_WS(':', 'a', s.svc_id, d.seq, k.seq)) AS h1, CRC32(CONCAT_WS(':', 'b', s.svc_id, d.seq, k.seq)) AS h2
      FROM zz_seed_s s JOIN seq_${lo}_to_${hi} d JOIN seq_1_to_$k k
      JOIN zz_seed_i i ON i.svc_id = s.svc_id AND i.rk = 1 + CRC32(CONCAT_WS(':', 'n', s.svc_id, d.seq, k.seq)) % s.n_inst
      WHERE s.kind IN ('svc', 'vol') AND CRC32(CONCAT_WS(':', 'a', s.svc_id, d.seq, k.seq)) % 10000 < $thr
    ) x
  ) c
) a
JOIN seq_0_to_$lines l
ORDER BY a.b, a.sid, l.seq""", lo=lo, hi=hi, k=k, thr=thr, lines=args.action_lines, action=action, status=status,
          origin=pick("c.h1 DIV 7", ["daemon/monitor", "user", "daemon/scheduler"]),
          sid=uuid_of("CONCAT_WS(':', 'sid', x.svc_id, x.d, x.k)"))

    # The collector log of the nodes and the services.
    kn, thr_n = rate_draw(args.node_logs_per_day)
    ks, thr_s = rate_draw(args.service_logs_per_day)
    node_log = variants("((x.h1 DIV 10000) % 100)", [
        (45, {"action": "'node.action'", "user": "CONCAT('root@', x.nodename)", "fmt": lit("run %(action)s"),
              "dict": "CONCAT('{\"action\": \"', %s, '\"}')" % pick(
                  "x.h2 DIV 86400", ["pushasset", "pushpkg", "pushdisks", "checks", "sysreport", "pushstats"]),
              "level": "'info'"}),
        (20, {"action": "'node.change'", "user": "CONCAT(LOWER(SUBSTRING_INDEX(x.team, ' ', 1)), '.admin@example.com')",
              "fmt": lit("change properties %(data)s"),
              "dict": "CONCAT('{\"data\": {\"loc_rack\": \"R', LPAD(1 + (x.h2 DIV 86400) % 40, 2, '0'), '\"}}')",
              "level": "'info'"}),
        (20, {"action": "'checks.alert'", "user": "'scheduler'", "fmt": lit("check %(t)s:%(i)s out of bounds"),
              "dict": lit('{"t": "fs_u", "i": "/var"}'), "level": "'warning'"}),
        (15, {"action": "'node.status'", "user": "'scheduler'", "fmt": lit("node %(node)s not updated for %(h)s hours"),
              "dict": "CONCAT('{\"node\": \"', x.nodename, '\", \"h\": ', 2 + (x.h2 DIV 86400) % 29, '}')",
              "level": "'error'"}),
    ])
    svc_log = variants("((x.h1 DIV 10000) % 100)", [
        (40, {"action": "'service.action'", "user": "CONCAT(x.ns, '.admin@example.com')", "fmt": lit("run %(action)s"),
              "dict": "CONCAT('{\"action\": \"', %s, '\"}')" % pick(
                  "x.h2 DIV 86400", ["start", "stop", "restart", "freeze", "thaw"]),
              "level": "'info'"}),
        (20, {"action": "'service.change'", "user": "CONCAT(x.ns, '.admin@example.com')",
              "fmt": lit("change properties %(data)s"),
              "dict": "CONCAT('{\"data\": {\"svc_sla\": ', %s, '}}')" % pick("x.h2 DIV 86400", ["99.5", "99.9"]),
              "level": "'info'"}),
        (20, {"action": "'service.status'", "user": "'scheduler'", "fmt": lit("service '%(svc)s' status changed to %(s)s"),
              "dict": "CONCAT('{\"svc\": \"', x.svcname, '\", \"s\": \"', %s, '\"}')" % pick(
                  "x.h2 DIV 86400", ["down", "warn", "up"]),
              "level": "'warning'"}),
        (20, {"action": "'service.config'", "user": "'scheduler'", "fmt": lit("service '%(svc)s' configuration updated"),
              "dict": "CONCAT('{\"svc\": \"', x.svcname, '\"}')", "level": "'info'"}),
    ])
    for lo, hi in day_chunks(1, ret, chunk):
        q.step("log: days %d to %d ago" % (hi, lo))
        q("""
INSERT INTO log (log_action, log_user, log_fmt, log_dict, log_date, svc_id, node_id, log_level)
SELECT u.* FROM (
  SELECT $na AS log_action, $nu AS log_user, $nf AS log_fmt, $nd AS log_dict,
    FROM_UNIXTIME(UNIX_TIMESTAMP() - (x.d * 86400 + x.h2 % 86400)) AS log_date, '' AS svc_id, x.node_id, $nl AS log_level
  FROM (SELECT n.node_id, n.nodename, n.team, d.seq AS d, CRC32(CONCAT_WS(':', 'nl', n.node_id, d.seq, k.seq)) AS h1,
      CRC32(CONCAT_WS(':', 'nm', n.node_id, d.seq, k.seq)) AS h2
    FROM zz_seed_n n JOIN seq_${lo}_to_${hi} d JOIN seq_1_to_$kn k
    WHERE CRC32(CONCAT_WS(':', 'nl', n.node_id, d.seq, k.seq)) % 10000 < $thr_n) x
  UNION ALL
  SELECT $sa, $su, $sf, $sd, FROM_UNIXTIME(UNIX_TIMESTAMP() - (x.d * 86400 + x.h2 % 86400)), x.svc_id, '', $sl
  FROM (SELECT s.svc_id, s.svcname, s.ns, d.seq AS d, CRC32(CONCAT_WS(':', 'sl', s.svc_id, d.seq, k.seq)) AS h1,
      CRC32(CONCAT_WS(':', 'sm', s.svc_id, d.seq, k.seq)) AS h2
    FROM zz_seed_s s JOIN seq_${lo}_to_${hi} d JOIN seq_1_to_$ks k
    WHERE CRC32(CONCAT_WS(':', 'sl', s.svc_id, d.seq, k.seq)) % 10000 < $thr_s) x
) u ORDER BY u.log_date""", lo=lo, hi=hi, kn=kn, thr_n=thr_n, ks=ks, thr_s=thr_s,
          na=node_log["action"], nu=node_log["user"], nf=node_log["fmt"], nd=node_log["dict"], nl=node_log["level"],
          sa=svc_log["action"], su=svc_log["user"], sf=svc_log["fmt"], sd=svc_log["dict"], sl=svc_log["level"])

    # Status periods of the instances and of their resources.
    segments(q, "svcmon_log", "node_id, svc_id, mon_overallstatus, mon_availstatus, mon_begin, mon_end",
             "SELECT node_id, svc_id FROM zz_seed_i WHERE kind IN ('svc', 'vol')", ["node_id", "svc_id"], 7, 60, 14400,
             "s.node_id, s.svc_id, IF(s.part = 0, IF(s.hb % 3 = 0, 'down', 'warn'), 'up'), "
             "IF(s.part = 0 AND s.hb % 3 = 0, 'down', 'up'), $begin, $end", ret, chunk * 2)
    segments(q, "resmon_log", "node_id, svc_id, rid, res_status, res_begin, res_end, res_log",
             "SELECT node_id, svc_id, rid, grp FROM zz_seed_r", ["node_id", "svc_id", "rid"], 20, 180, 14400,
             "s.node_id, s.svc_id, s.rid, IF(s.part = 0, IF(s.hb % 3 = 0, 'down', 'warn'), 'up'), $begin, $end, "
             "IF(s.part = 0, CASE s.grp WHEN 'fs' THEN 'fs is not mounted' WHEN 'app' THEN 'check: exit code 1' "
             "WHEN 'ip' THEN 'ip is not plumbed' WHEN 'container' THEN 'container is not running' "
             "ELSE 'not up' END, '')", ret, chunk * 2)

    # Alerts raised and cleared over the history, and their references.
    q.step("alerts")
    q("""CREATE TABLE zz_seed_ev (dash_md5 BINARY(32), dash_type VARCHAR(60), fmt VARCHAR(100), dict TEXT,
  node_id CHAR(36) CHARACTER SET ascii, svc_id CHAR(36) CHARACTER SET ascii, b DATETIME, e DATETIME)""")
    _, thr_na = rate_draw(args.node_alerts_per_month / 30)
    _, thr_sa = rate_draw(args.service_alerts_per_month / 30)
    for lo, hi in day_chunks(1, ret, chunk * 4):
        q("""
INSERT INTO zz_seed_ev
SELECT MD5(CONCAT(a.dash_type, CONCAT('$p:', IFNULL(a.inst, x.name)))), a.dash_type, a.fmt, a.dict, x.node_id,
  x.svc_id, x.b, LEAST(x.b + INTERVAL x.dur SECOND, NOW() - INTERVAL 60 SECOND)
FROM (
  SELECT 'node' AS scope, n.node_id, '' AS svc_id, n.nodename AS name,
    FROM_UNIXTIME(UNIX_TIMESTAMP() - (d.seq * 86400 + CRC32(CONCAT_WS(':', 'eb', n.node_id, d.seq)) % 86400)) AS b,
    300 + CRC32(CONCAT_WS(':', 'ed', n.node_id, d.seq)) % 259200 AS dur,
    (CRC32(CONCAT_WS(':', 'ev', n.node_id, d.seq)) DIV 10000) % 100 AS bucket
  FROM zz_seed_n n JOIN seq_${lo}_to_${hi} d WHERE CRC32(CONCAT_WS(':', 'ev', n.node_id, d.seq)) % 10000 < $thr_na
  UNION ALL
  SELECT 'svc', '', s.svc_id, s.svcname,
    FROM_UNIXTIME(UNIX_TIMESTAMP() - (d.seq * 86400 + CRC32(CONCAT_WS(':', 'eb', s.svc_id, d.seq)) % 86400)),
    300 + CRC32(CONCAT_WS(':', 'ed', s.svc_id, d.seq)) % 259200,
    (CRC32(CONCAT_WS(':', 'ev', s.svc_id, d.seq)) DIV 10000) % 100
  FROM zz_seed_s s JOIN seq_${lo}_to_${hi} d
  WHERE s.kind IN ('svc', 'vol') AND CRC32(CONCAT_WS(':', 'ev', s.svc_id, d.seq)) % 10000 < $thr_sa
) x JOIN zz_seed_at a ON a.scope = x.scope AND x.bucket >= a.lo AND x.bucket < a.hi""",
          lo=lo, hi=hi, thr_na=thr_na, thr_sa=thr_sa)
    q("""INSERT INTO dashboard_events (dash_md5, node_id, svc_id, dash_begin, dash_end)
SELECT dash_md5, node_id, svc_id, b, e FROM zz_seed_ev ORDER BY b""")
    q("""INSERT IGNORE INTO dashboard_ref (dash_md5, dash_type, dash_fmt, dash_dict)
SELECT dash_md5, MIN(dash_type), MIN(fmt), MIN(dict) FROM zz_seed_ev GROUP BY dash_md5""")

    # Backups of the nodes, every night.
    for lo, hi in day_chunks(1, ret, chunk * 4):
        q.step("saves: days %d to %d ago" % (hi, lo))
        q("""
INSERT INTO saves (save_name, save_group, save_size, save_date, save_retention, save_volume, save_level, save_server,
  save_app, save_id, node_id, svc_id)
SELECT x.nodename, CONCAT('$p-', LOWER(x.app)), IF(x.full, x.base, x.base DIV (8 + x.h % 8)), x.at,
  x.at + INTERVAL IF(x.full, 90, 30) DAY, CONCAT('pool', 1 + CRC32(CONCAT('bv', x.node_id)) % 4),
  IF(x.full, 'full', 'incr'), CONCAT('$p-bkp', 1 + CRC32(CONCAT('bsrv', x.node_id)) % 4), x.app,
  LEFT(MD5(CONCAT_WS(':', 'save', x.node_id, x.d)), 24), x.node_id, ''
FROM (SELECT n.node_id, n.nodename, n.app, d.seq AS d, CRC32(CONCAT_WS(':', 'bh', n.node_id, d.seq)) AS h,
    (d.seq + CRC32(CONCAT('bf', n.node_id))) % 7 = 0 AS full,
    (20 + CRC32(CONCAT('bs', n.node_id)) % 480) * 1073741824 AS base,
    FROM_UNIXTIME(UNIX_TIMESTAMP() - (d.seq * 86400 + CRC32(CONCAT('bt', n.node_id)) % 21600)) AS at
  FROM zz_seed_n n JOIN seq_${lo}_to_${hi} d) x
ORDER BY x.at""", lo=lo, hi=hi)
    q("""
INSERT INTO saves_last (id, save_name, save_group, save_size, save_date, save_retention, save_volume, save_level,
  save_server, save_app, save_id, chk_instance, save_resolved, node_id, svc_id)
SELECT s.id, s.save_name, s.save_group, s.save_size, s.save_date, s.save_retention, s.save_volume, s.save_level,
  s.save_server, s.save_app, s.save_id, s.chk_instance, s.save_resolved, s.node_id, s.svc_id
FROM saves s JOIN (SELECT c.node_id, MAX(c.id) AS id FROM saves c JOIN zz_seed_n n ON n.node_id = c.node_id
  GROUP BY c.node_id) l ON l.id = s.id""")

    # Daily statistics, over the whole history: they are never trimmed.
    for lo, hi in day_chunks(1, hist, chunk * 4):
        q.step("stat_day_svc: days %d to %d ago" % (hi, lo))
        q("""
INSERT INTO stat_day_svc (day, nb_action, nb_action_err, nb_action_warn, nb_action_ok, disk_size, ram_size,
  nb_cpu_core, nb_cpu_die, watt, rackunit, nb_nodes, local_disk_size, svc_id)
SELECT x.day, x.nb, x.err, x.warn, x.nb - x.err - x.warn, x.disk, x.n_inst * 4096 * (1 + x.hb % 16),
  x.n_inst * (2 + x.hb % 30), x.n_inst, x.n_inst * (150 + x.hb % 400), x.n_inst, x.n_inst, x.n_inst * 480, x.svc_id
FROM (SELECT y.*, IF(y.nb > 0 AND (y.hd DIV 4) % 20 = 0, 1, 0) AS err,
    IF(y.nb > 1 AND (y.hd DIV 80) % 30 = 0, 1, 0) AS warn
  FROM (SELECT s.svc_id, s.n_inst, CURDATE() - INTERVAL d.seq DAY AS day,
      CRC32(CONCAT_WS(':', 'sd', s.svc_id, d.seq)) AS hd, CRC32(CONCAT('sb', s.svc_id)) AS hb,
      IF(s.kind IN ('svc', 'vol'), CRC32(CONCAT_WS(':', 'sd', s.svc_id, d.seq)) % 4, 0) AS nb,
      (50 + CRC32(CONCAT('sb', s.svc_id)) % 2000) * ($hist * 2 - d.seq) DIV ($hist * 2) AS disk
    FROM zz_seed_s s JOIN seq_${lo}_to_${hi} d) y) x
ORDER BY x.day, x.svc_id""", lo=lo, hi=hi)
    q.step("stat_day_disk")
    if args.arrays:
        q("""
INSERT INTO stat_day_disk_array (day, array_name, disk_used, disk_size, reservable, reserved)
SELECT CURDATE() - INTERVAL d.seq DAY, x.array_name, x.size * (30 + 50 * ($hist - d.seq) DIV $hist) DIV 100, x.size,
  x.size DIV 2, x.size DIV 20
FROM (SELECT array_name, SUM(dg_size) AS size FROM zz_seed_dg GROUP BY array_name) x JOIN seq_1_to_$hist d
ORDER BY d.seq DESC, x.array_name""")
        q("""
INSERT INTO stat_day_disk_array_dg (day, array_name, array_dg, disk_used, disk_size, reserved, reservable)
SELECT CURDATE() - INTERVAL d.seq DAY, g.array_name, g.dg_name,
  g.dg_size * (20 + (CRC32(g.dg_name) % 30) + 40 * ($hist - d.seq) DIV $hist) DIV 100, g.dg_size, g.dg_size DIV 20,
  g.dg_size DIV 2
FROM zz_seed_dg g JOIN seq_1_to_$hist d ORDER BY d.seq DESC, g.idx""")
        q("""
INSERT INTO stat_day_disk_app_dg (day, dg_id, app, disk_used, quota)
SELECT CURDATE() - INTERVAL d.seq DAY, g.id, a.app,
  (1024 * (10 + CRC32(CONCAT(a.app, g.id)) % 500)) * (2 * $hist - d.seq) DIV (2 * $hist), 1024 * 600
FROM apps a JOIN `$reg` r ON r.kind = 'app' AND r.id = a.app
JOIN zz_seed_dg g ON g.idx IN (1 + CRC32(CONCAT('ad1', a.app)) % $n_dg, 1 + CRC32(CONCAT('ad2', a.app)) % $n_dg)
JOIN seq_1_to_$hist d ORDER BY d.seq DESC, a.app""", n_dg=args.arrays * len(DISK_GROUPS))
    q("""
INSERT INTO stat_day_disk_app (day, app, disk_used, quota)
SELECT CURDATE() - INTERVAL d.seq DAY, a.app,
  (1024 * (100 + CRC32(a.app) % 5000)) * (2 * $hist - d.seq) DIV (2 * $hist), 1024 * 6000
FROM apps a JOIN `$reg` r ON r.kind = 'app' AND r.id = a.app JOIN seq_1_to_$hist d ORDER BY d.seq DESC, a.app""")

    q.step("done")
    q("DROP TABLE IF EXISTS %s" % ", ".join(WORK_TABLES))
    out.write("SELECT 'seed_testdata: activity loaded' AS done;\n")


def seed_ids(kind):
    return "SELECT id FROM `%s` WHERE kind = '%s'" % (REGISTRY, kind)


def down(out):
    """Removes every row of the objects recorded in the registry, then the registry.

    By batches, each committed: interrupted, it removes the rest when run again,
    the registry going last."""
    out.write(
        "-- Removes the test data of seed_testdata.py, and only it.\n"
        "DROP PROCEDURE IF EXISTS zz_seed_down;\n"
        "DELIMITER //\n"
        "CREATE PROCEDURE zz_seed_down()\n"
        "BEGIN\n"
        "  DECLARE done INT DEFAULT 0;\n"
        "  DECLARE n INT;\n"
        "  DECLARE t VARCHAR(64);\n"
        "  DECLARE c VARCHAR(64);\n"
        # Every base table with a node_id or svc_id column, the test data's and
        # what the scheduler wrote about its objects since.
        "  DECLARE cur CURSOR FOR\n"
        "    SELECT col.table_name, col.column_name FROM information_schema.columns col\n"
        "    JOIN information_schema.tables tb ON tb.table_schema = col.table_schema\n"
        "      AND tb.table_name = col.table_name AND tb.table_type = 'BASE TABLE'\n"
        "    WHERE col.table_schema = DATABASE() AND col.column_name IN ('node_id', 'svc_id')\n"
        "      AND col.table_name NOT LIKE 'zz\\_seed\\_%%'\n"
        "    ORDER BY col.table_name = 'dashboard' DESC, col.table_name, col.column_name;\n"
        "  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;\n"
        "  IF (SELECT COUNT(*) FROM information_schema.tables\n"
        "      WHERE table_schema = DATABASE() AND table_name = '%(reg)s') = 0 THEN\n"
        "    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'no seed_testdata registry: nothing to remove';\n"
        "  END IF;\n"
        # The alert references the test data used, removed last if nothing else
        # uses them; kept in a table, for a removal run again.
        "  CREATE TABLE IF NOT EXISTS zz_seed_md5 (dash_md5 BINARY(32) PRIMARY KEY);\n"
        "  INSERT IGNORE INTO zz_seed_md5 SELECT dash_md5 FROM dashboard\n"
        "    WHERE node_id IN (%(nodes)s) OR svc_id IN (%(svcs)s);\n"
        "  INSERT IGNORE INTO zz_seed_md5 SELECT dash_md5 FROM dashboard_events\n"
        "    WHERE (node_id IN (%(nodes)s) OR svc_id IN (%(svcs)s)) AND dash_md5 IS NOT NULL;\n"
        "  COMMIT;\n"
        "  OPEN cur;\n"
        "  rows_loop: LOOP\n"
        "    FETCH cur INTO t, c;\n"
        "    IF done THEN LEAVE rows_loop; END IF;\n"
        "    SET @q = CONCAT('DELETE FROM `', t, '` WHERE `', c, '` IN (SELECT id FROM `%(reg)s` WHERE kind = ''',\n"
        "      IF(c = 'node_id', 'node', 'service'), ''') LIMIT 50000');\n"
        "    PREPARE stmt FROM @q;\n"
        "    REPEAT\n"
        "      EXECUTE stmt;\n"
        "      SET n = ROW_COUNT();\n"
        "      COMMIT;\n"
        "    UNTIL n < 50000 END REPEAT;\n"
        "    DEALLOCATE PREPARE stmt;\n"
        "  END LOOP;\n"
        "  CLOSE cur;\n"
        "  DELETE r FROM dashboard_ref r JOIN zz_seed_md5 m ON m.dash_md5 = r.dash_md5\n"
        "    WHERE NOT EXISTS (SELECT 1 FROM dashboard d WHERE d.dash_md5 = r.dash_md5)\n"
        "      AND NOT EXISTS (SELECT 1 FROM dashboard_events e WHERE e.dash_md5 = r.dash_md5);\n"
        # What activity loaded under names of its own: storage arrays, modulesets.
        "  DELETE FROM diskinfo WHERE disk_arrayid IN (%(arrays)s);\n"
        "  DELETE FROM stat_day_disk_array WHERE array_name IN (%(arrays)s);\n"
        "  DELETE FROM stat_day_disk_array_dg WHERE array_name IN (%(arrays)s);\n"
        "  DELETE FROM stat_day_disk_app WHERE app IN (%(apps)s);\n"
        "  DELETE FROM stat_day_disk_app_dg WHERE app IN (%(apps)s);\n"
        "  DELETE dg FROM stor_array_dg dg JOIN stor_array a ON CAST(a.id AS CHAR) = dg.array_id\n"
        "    WHERE a.array_name IN (%(arrays)s);\n"
        "  DELETE FROM stor_array WHERE array_name IN (%(arrays)s);\n"
        "  DELETE mm FROM comp_moduleset_modules mm JOIN comp_moduleset m ON m.id = mm.modset_id\n"
        "    WHERE m.modset_name IN (%(modsets)s);\n"
        "  DELETE FROM comp_moduleset WHERE modset_name IN (%(modsets)s);\n"
        "  DELETE FROM clusters WHERE cluster_id IN (%(clusters)s);\n"
        "  DELETE ar FROM apps_responsibles ar JOIN apps a ON a.id = ar.app_id WHERE a.app IN (%(apps)s);\n"
        "  DELETE FROM apps WHERE app IN (%(apps)s);\n"
        "  COMMIT;\n"
        "END //\n"
        "DELIMITER ;\n"
        "CALL zz_seed_down();\n"
        "DROP PROCEDURE zz_seed_down;\n"
        "DROP TABLE IF EXISTS zz_seed_md5, %(work)s;\n"
        "DROP TABLE `%(reg)s`;\n"
        "SELECT 'seed_testdata: removed' AS done;\n"
        % {"reg": REGISTRY, "nodes": seed_ids("node"), "svcs": seed_ids("service"),
           "clusters": seed_ids("cluster"), "apps": seed_ids("app"), "arrays": seed_ids("array"),
           "modsets": seed_ids("modset"), "work": ", ".join(WORK_TABLES)})


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
        # What activity loaded, scrubbed after a few days without update.
        "UPDATE node_hba SET updated = NOW() WHERE node_id IN (%(n)s);\n"
        "UPDATE svcdisks SET disk_updated = NOW() WHERE svc_id IN (%(s)s);\n"
        "UPDATE diskinfo SET disk_updated = NOW() WHERE disk_arrayid IN (%(a)s);\n"
        "UPDATE stor_array SET array_updated = NOW() WHERE array_name IN (%(a)s);\n"
        "UPDATE hbmon SET updated = NOW(), last_beating = NOW() WHERE node_id IN (%(n)s);\n"
        "UPDATE hbmon_log_last SET end = NOW() WHERE node_id IN (%(n)s);\n"
        % {"n": nodes, "s": svcs, "a": seed_ids("array")})


def status(out):
    """Counts the objects of the registry and the rows of the main tables they own."""
    nodes, svcs = seed_ids("node"), seed_ids("service")
    out.write("SELECT kind, COUNT(*) AS objects FROM `%s` GROUP BY kind;\n" % REGISTRY)
    parts = []
    for table, column in [("svcmon", "svc_id"), ("resmon", "svc_id"), ("resinfo", "svc_id"), ("svcactions", "svc_id"),
                          ("log", "svc_id"), ("dashboard", "svc_id"), ("dashboard_events", "svc_id"),
                          ("services_log", "svc_id"), ("svcmon_log", "svc_id"), ("resmon_log", "svc_id"),
                          ("svcdisks", "svc_id"), ("stat_day_svc", "svc_id"), ("checks_live", "node_id"),
                          ("packages", "node_id"), ("node_users", "node_id"), ("node_groups", "node_id"),
                          ("node_hw", "node_id"), ("comp_status", "node_id"), ("comp_log", "node_id"),
                          ("comp_log_daily", "node_id"), ("saves", "node_id"), ("hbmon_log", "node_id")]:
        ids = nodes if column == "node_id" else svcs
        parts.append("SELECT '%s' AS tbl, COUNT(*) AS seed_rows FROM `%s` WHERE `%s` IN (%s)" % (table, table, column, ids))
    parts.append("SELECT 'log (nodes)', COUNT(*) FROM log WHERE node_id IN (%s)" % nodes)
    parts.append("SELECT 'dashboard_events (nodes)', COUNT(*) FROM dashboard_events WHERE node_id IN (%s)" % nodes)
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
    up.add_argument("--nodes", type=int, default=0,
                    help="as many clusters as these nodes take, instead of --clusters (default: 0, unused)")
    up.add_argument("--resources", type=float, default=6,
                    help="mean resources of a svc object; vol objects have 2, cfg and sec none (default: 6)")
    up.add_argument("--no-history", action="store_true",
                    help="no actions, logs, nor instance and resource history: activity generates them")
    act = sub.add_parser("activity", help="SQL adding to the test data the activity of a long service life")
    act.add_argument("--prefix", default="sd", help="the prefix of the test data loaded (default: sd)")
    act.add_argument("--seed", type=int, default=42, help="random seed (default: 42)")
    act.add_argument("--days", type=int, default=730,
                     help="service life: the history of the tables never trimmed (default: 730)")
    act.add_argument("--retention", type=int, default=365,
                     help="history of the tables the scheduler trims, its retention (default: 365)")
    act.add_argument("--actions-per-day", type=float, default=0.5,
                     help="mean agent actions a day per svc or vol object (default: 0.5)")
    act.add_argument("--action-lines", type=int, default=1, help="log lines per action (default: 1)")
    act.add_argument("--node-logs-per-day", type=float, default=1.0,
                     help="mean collector log entries a day per node (default: 1)")
    act.add_argument("--service-logs-per-day", type=float, default=0.2,
                     help="mean collector log entries a day per service (default: 0.2)")
    act.add_argument("--node-alerts-per-month", type=float, default=2,
                     help="mean alerts raised a month per node (default: 2)")
    act.add_argument("--service-alerts-per-month", type=float, default=1,
                     help="mean alerts raised a month per svc or vol object (default: 1)")
    act.add_argument("--comp-interval", type=int, default=7,
                     help="days between two compliance checks of a node (default: 7)")
    act.add_argument("--arrays", type=int, default=12, help="storage arrays (default: 12)")
    act.add_argument("--chunk-days", type=int, default=15,
                     help="days of history per statement of the largest tables (default: 15)")
    act.add_argument("--parts", type=int, default=10,
                     help="statements the inventory tables are loaded by (default: 10)")
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
    elif args.command == "activity":
        if args.days < 3 or args.retention < 3:
            parser.error("--days and --retention must be 3 or more")
        if args.chunk_days < 1 or args.parts < 1 or args.comp_interval < 1:
            parser.error("--chunk-days, --parts and --comp-interval must be 1 or more")
        activity(args, out)
    elif args.command == "down":
        down(out)
    elif args.command == "refresh":
        refresh(out)
    else:
        status(out)


if __name__ == "__main__":
    main()
