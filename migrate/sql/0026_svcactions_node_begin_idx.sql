-- 2026-10-29: an index on svcactions for the actions of a node over a period,
-- by status: the actions badge of a node panel (stats of the last 60 days),
-- its actions tab and the count of its unacknowledged errors. Without it, the
-- access check of a team user drove the plan from its services, then read their
-- actions, whole rows, by the thousands: under the load of 12 users on 3000
-- nodes, 4.6 M actions (tools/bench-collector), the badge reached its 5s limit
-- and the node panel a p95 of 5.0s; with it, 1.6s, the actions tab 31 ms instead
-- of 258. Built in place, the table kept writable: 16s on 5 GB in dev.
ALTER TABLE svcactions ADD INDEX IF NOT EXISTS k_node_type_begin (node_id, log_type, begin, status), ALGORITHM=INPLACE, LOCK=NONE;
