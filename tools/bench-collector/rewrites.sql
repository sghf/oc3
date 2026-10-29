-- Current queries of the slowest requests of the benchmark, and their rewrites,
-- timed one by one: ./rewrites.sh. @node: a node of 15 instances, the visibility
-- that of the bench-ops team.
SET @node = '16803b20-3b2c-5f60-b6a8-72cad2a94dcb';

-- @@ node panel, actions badge (team user): count by status over 60 days
-- @current
SELECT COALESCE(svcactions.status, ''), COUNT(*) FROM svcactions LEFT JOIN nodes ON nodes.node_id = svcactions.node_id JOIN services ON services.svc_id = svcactions.svc_id WHERE services.svc_app IN (SELECT a.app FROM apps a JOIN apps_responsibles ar ON ar.app_id = a.id JOIN auth_group ag ON ag.id = ar.group_id WHERE ag.role IN ('Everybody','bench-ops')) AND svcactions.log_type = 'status' AND svcactions.begin >= NOW() - INTERVAL 60 DAY AND svcactions.node_id = @node GROUP BY 1 ORDER BY 2 DESC, 1;
-- @rewrite: svcactions first, by its node index
SELECT COALESCE(svcactions.status, ''), COUNT(*) FROM svcactions LEFT JOIN nodes ON nodes.node_id = svcactions.node_id STRAIGHT_JOIN services ON services.svc_id = svcactions.svc_id WHERE services.svc_app IN (SELECT a.app FROM apps a JOIN apps_responsibles ar ON ar.app_id = a.id JOIN auth_group ag ON ag.id = ar.group_id WHERE ag.role IN ('Everybody','bench-ops')) AND svcactions.log_type = 'status' AND svcactions.begin >= NOW() - INTERVAL 60 DAY AND svcactions.node_id = @node GROUP BY 1 ORDER BY 2 DESC, 1;

-- @@ packages list (team user), default sort
-- @current
SELECT packages.id, nodes.nodename, packages.pkg_name, packages.pkg_version FROM packages LEFT JOIN pkg_sig_provider ON pkg_sig_provider.sig_id = packages.pkg_sig JOIN nodes ON nodes.node_id = packages.node_id WHERE nodes.app IN (SELECT a.app FROM apps a JOIN apps_responsibles ar ON ar.app_id = a.id JOIN auth_group ag ON ag.id = ar.group_id WHERE ag.role IN ('Everybody','bench-ops')) ORDER BY nodes.nodename, packages.pkg_name, packages.pkg_arch, packages.id LIMIT 51;
-- @rewrite: nodes first, by name, their packages by idx3 (node_id, pkg_name, pkg_arch, ...)
SELECT packages.id, nodes.nodename, packages.pkg_name, packages.pkg_version FROM nodes STRAIGHT_JOIN packages ON packages.node_id = nodes.node_id LEFT JOIN pkg_sig_provider ON pkg_sig_provider.sig_id = packages.pkg_sig WHERE nodes.app IN (SELECT a.app FROM apps a JOIN apps_responsibles ar ON ar.app_id = a.id JOIN auth_group ag ON ag.id = ar.group_id WHERE ag.role IN ('Everybody','bench-ops')) ORDER BY nodes.nodename, packages.pkg_name, packages.pkg_arch, packages.id LIMIT 51;

-- @@ resources list (manager), default sort
-- @current
SELECT resmon.id, services.svcname, nodes.nodename, resmon.rid FROM resmon LEFT JOIN nodes ON nodes.node_id = resmon.node_id JOIN services ON services.svc_id = resmon.svc_id WHERE resmon.id > 0 ORDER BY services.svcname, nodes.nodename, resmon.vmname, resmon.rid, resmon.id LIMIT 51;
-- @rewrite: services first, by name
SELECT resmon.id, services.svcname, nodes.nodename, resmon.rid FROM services STRAIGHT_JOIN resmon ON resmon.svc_id = services.svc_id LEFT JOIN nodes ON nodes.node_id = resmon.node_id ORDER BY services.svcname, nodes.nodename, resmon.vmname, resmon.rid, resmon.id LIMIT 51;

-- @@ instances list (manager), default sort
-- @current
SELECT svcmon.svc_id, services.svcname, nodes.nodename FROM svcmon LEFT JOIN nodes ON nodes.node_id = svcmon.node_id JOIN services ON services.svc_id = svcmon.svc_id LEFT JOIN b_action_errors ON b_action_errors.svc_id = svcmon.svc_id AND b_action_errors.node_id = svcmon.node_id WHERE svcmon.ID > 0 ORDER BY services.svcname, nodes.nodename, svcmon.ID LIMIT 51;
-- @rewrite: services first, by name
SELECT svcmon.svc_id, services.svcname, nodes.nodename FROM services STRAIGHT_JOIN svcmon ON svcmon.svc_id = services.svc_id LEFT JOIN nodes ON nodes.node_id = svcmon.node_id LEFT JOIN b_action_errors ON b_action_errors.svc_id = svcmon.svc_id AND b_action_errors.node_id = svcmon.node_id ORDER BY services.svcname, nodes.nodename, svcmon.ID LIMIT 51;

-- @@ node panel, disks badge (manager)
-- @current
SELECT diskinfo.disk_id, svcdisks.disk_model, services.svcname FROM diskinfo LEFT JOIN svcdisks ON svcdisks.disk_id = diskinfo.disk_id LEFT JOIN nodes ON nodes.node_id = svcdisks.node_id LEFT JOIN services ON services.svc_id = svcdisks.svc_id LEFT JOIN apps ON apps.app = services.svc_app WHERE diskinfo.id > 0 AND svcdisks.node_id = @node ORDER BY services.svcname, diskinfo.disk_id;
-- @rewrite: the disks of the node first (svcdisks.k_node_id), no id > 0
SELECT diskinfo.disk_id, svcdisks.disk_model, services.svcname FROM svcdisks JOIN diskinfo ON diskinfo.disk_id = svcdisks.disk_id LEFT JOIN nodes ON nodes.node_id = svcdisks.node_id LEFT JOIN services ON services.svc_id = svcdisks.svc_id LEFT JOIN apps ON apps.app = services.svc_app WHERE svcdisks.node_id = @node ORDER BY services.svcname, diskinfo.disk_id;

-- @@ logs list total (meta.total, manager)
-- @current
SELECT COUNT(*) OVER () FROM log LEFT JOIN nodes ON nodes.node_id = log.node_id LEFT JOIN services ON services.svc_id = log.svc_id WHERE log.id > 0 LIMIT 1 OFFSET 0;
-- @rewrite: plain count, no useless left joins
SELECT COUNT(*) FROM log;
-- @rewrite: capped count ("more than 10000")
SELECT COUNT(*) FROM (SELECT 1 FROM log LIMIT 10001) t;

-- @@ package differences between the 3 nodes of a cluster (@nodes, @n)
-- @current
SELECT p.node_id, p.pkg_name, p.pkg_version, p.pkg_arch, COALESCE(p.pkg_type, '') FROM packages p JOIN ( SELECT pkg_name, pkg_version, pkg_arch, pkg_type FROM packages WHERE node_id IN (@nodes) GROUP BY pkg_name, pkg_version, pkg_arch, pkg_type HAVING COUNT(DISTINCT node_id) != @n ) u ON p.pkg_name = u.pkg_name AND p.pkg_version = u.pkg_version AND p.pkg_arch = u.pkg_arch AND p.pkg_type <=> u.pkg_type WHERE p.node_id IN (@nodes) ORDER BY p.pkg_name, p.pkg_arch, p.pkg_type, p.pkg_version;
-- @rewrite: one read of the packages of the nodes, a window count
SELECT node_id, pkg_name, pkg_version, pkg_arch, pkg_type FROM (SELECT p.node_id, p.pkg_name, p.pkg_version, p.pkg_arch, COALESCE(p.pkg_type, '') AS pkg_type, COUNT(*) OVER (PARTITION BY p.pkg_name, p.pkg_version, p.pkg_arch, p.pkg_type) AS n FROM packages p WHERE p.node_id IN (@nodes)) t WHERE n != @n ORDER BY pkg_name, pkg_arch, pkg_type, pkg_version;

-- @@ saves list (manager), default sort
-- @current
SELECT saves.id, saves.save_name, saves.save_date, nodes.nodename, services.svcname FROM saves LEFT JOIN nodes ON nodes.node_id = saves.node_id LEFT JOIN services ON services.svc_id = saves.svc_id WHERE saves.id > 0 ORDER BY saves.save_date DESC, saves.id LIMIT 51;
-- @rewrite: same direction tie-breaker
SELECT saves.id, saves.save_name, saves.save_date, nodes.nodename, services.svcname FROM saves LEFT JOIN nodes ON nodes.node_id = saves.node_id LEFT JOIN services ON services.svc_id = saves.svc_id ORDER BY saves.save_date DESC, saves.id DESC LIMIT 51;
