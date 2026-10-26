-- 2026-10-26: the last state of each heartbeat was recorded under the cluster
-- id instead of the node id (DBHeartbeat.AsLog). The feeder never found those
-- rows again: each report rewrote them, consuming an auto-increment value per
-- heartbeat, the heartbeats of the nodes of a cluster sharing a name overwrote
-- one another, and no state change reached hbmon_log. The rows go: the next
-- report of each node records them under its node id. Nothing left to delete
-- when run again.
DELETE FROM hbmon_log_last WHERE node_id = cluster_id;
