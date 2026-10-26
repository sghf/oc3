package cdb

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type (
	// DBHeartbeat struct is for heartbeat table, it describes the heartbeat of a node.
	//
	// CREATE TABLE `hbmon` (
	//  `id` bigint(20) NOT NULL AUTO_INCREMENT,
	//  `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `peer_node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `driver` varchar(32) DEFAULT '',
	//  `name` varchar(64) DEFAULT '',
	//  `desc` varchar(64) DEFAULT '',
	//  `state` enum('running','stopped','failed','unknown') DEFAULT '',
	//  `beating` tinyint(1) DEFAULT 0,
	// `last_beating` datetime NOT NULL,
	// `updated` timestamp NOT NULL DEFAULT current_timestamp(),
	//  PRIMARY KEY (`id`),
	//	KEY `k_cluster_id` (`cluster_id`),
	//  UNIQUE KEY `node_id` (`node_id`,`peer_node_id`,`name`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci |
	DBHeartbeat struct {
		ID        int64
		ClusterID string
		NodeID    string
		Driver    string
		Name      string
		Desc      string
		State     string

		// PeerNodeID when defined, it means the heartbeat is from a peer node,
		// so `beating` and `lastBeating`
		PeerNodeID string
		// Beating: 0: n/a, 1: Beating, 2: not Beating
		Beating     int8
		LastBeating time.Time

		Updated string
	}

	/*
		CREATE TABLE `hbmon_log` (
		 `id` bigint(20) NOT NULL AUTO_INCREMENT,
		 `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		 `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		 `peer_node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		 `name` varchar(64) DEFAULT '',
		 `desc` varchar(64) DEFAULT '',
		 `state` enum('running','stopped','failed','unknown', '') DEFAULT '',
		 `beating` tinyint(1) DEFAULT 0,
		 `begin` datetime NOT NULL,
		 `end` datetime NOT NULL,
		 `updated` timestamp NOT NULL DEFAULT current_timestamp(),
		 PRIMARY KEY (`id`),
		 KEY `k_cluster_id` (`cluster_id`),
		 UNIQUE KEY `k_node_peer_name_begin_end` (`node_id`,`peer_node_id`,`name`,`begin`, `end`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci

		CREATE TABLE `hbmon_log_last` (
		 `id` bigint(20) NOT NULL AUTO_INCREMENT,
		 `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		 `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		 `peer_node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		 `name` varchar(64) DEFAULT '',
		 `state` enum('running','stopped','failed','unknown', '') DEFAULT '',
		 `beating` tinyint(1) DEFAULT 0,
		 `begin` datetime NOT NULL,
		 `end` datetime NOT NULL,
		 `updated` timestamp NOT NULL DEFAULT current_timestamp(),
		 PRIMARY KEY (`id`),
		 KEY `k_cluster_id` (`cluster_id`),
		 UNIQUE KEY `k_node_peer_name` (`node_id`,`peer_node_id`,`name`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	*/
	DBHeartbeatLog struct {
		ID        int64
		ClusterID string
		NodeID    string
		Name      string

		// PeerNodeID when defined, it means the heartbeat is from a peer node,
		// so `beating` and `lastBeating`
		PeerNodeID string

		State string

		// Beating: 0: n/a, 1: Beating, 2: not Beating
		Beating int8

		Begin time.Time
		End   time.Time
	}
)

func (oDb *DB) hbByClusterID(ctx context.Context, clusterID string) ([]DBHeartbeat, error) {
	const (
		query = "SELECT `id`, `cluster_id`, `node_id`, `peer_node_id`, `driver`, `name`, `desc`, `state`, `beating`, `updated`" +
			" FROM `hbmon`" +
			" WHERE `cluster_id` = ?"
	)
	var (
		l []DBHeartbeat
	)
	rows, err := oDb.DB.QueryContext(ctx, query, clusterID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ()
	for rows.Next() {
		var hb DBHeartbeat
		if err = rows.Scan(&hb.ID, &hb.ClusterID, &hb.NodeID, &hb.PeerNodeID, &hb.Driver, &hb.Name, &hb.Desc, &hb.State, &hb.Beating, &hb.Updated); err != nil {
			return nil, err
		}
		l = append(l, hb)
	}
	return l, rows.Err()
}

func (oDb *DB) HBUpdate(ctx context.Context, l ...*DBHeartbeat) error {
	defer logDuration("HBUpdate", time.Now())
	const (
		insertColList         = "(`cluster_id`, `node_id`, `peer_node_id`, `driver`, `name`, `desc`, `state`, `beating`, `last_beating`, `updated`)"
		valueList             = "(?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())"
		onDuplicateAssignment = "`cluster_id` = VALUES(`cluster_id`),`driver`=VALUES(`driver`),`desc`=VALUES(`desc`),`state`=VALUES(`state`),`beating`=VALUES(`beating`),`last_beating`=VALUES(`last_beating`),`updated`=VALUES(`updated`)"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList
	args := make([]any, 0, 9*len(l))
	for _, v := range l {
		args = append(args, v.ClusterID, v.NodeID, v.PeerNodeID, v.Driver, v.Name, v.Desc, v.State, v.Beating, v.LastBeating)
	}

	query := fmt.Sprintf("INSERT INTO `hbmon` %s VALUES %s ON DUPLICATE KEY UPDATE %s",
		insertColList, placeholders, onDuplicateAssignment)

	// TODO check vs v2
	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("hbmon")
		oDb.Metrics.HbStatusUpdate.Add(float64(count))
	}
	return nil
}

func (oDb *DB) HBDeleteOutDatedByClusterID(ctx context.Context, clusterID string, maxTime time.Time) error {
	defer logDuration("hbDeleteOutDated cluster id:"+clusterID, time.Now())
	const (
		query = "" +
			"DELETE FROM `hbmon` WHERE `cluster_id` = ? AND `updated` < ?"
	)
	if count, err := oDb.execCountContext(ctx, query, clusterID, maxTime); err != nil {
		return fmt.Errorf("hbDeleteOutDatedByClusterID: %w", err)
	} else if count > 0 {
		oDb.SetChange("hbmon")
	}
	return nil
}

func (oDb *DB) HBLogLastFromClusterID(ctx context.Context, clusterID string) ([]*DBHeartbeatLog, error) {
	var (
		query = "SELECT `node_id`, `peer_node_id`, `name`, `state`, `beating`, `begin`, `end` FROM `hbmon_log_last` WHERE `cluster_id` = ?"

		l = make([]*DBHeartbeatLog, 0)
	)

	rows, err := oDb.DB.QueryContext(ctx, query, clusterID)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, fmt.Errorf("got nil rows result")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		o := DBHeartbeatLog{ClusterID: clusterID}
		if err := rows.Scan(&o.NodeID, &o.PeerNodeID, &o.Name, &o.State, &o.Beating, &o.Begin, &o.End); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		l = append(l, &o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return l, nil
}

func (oDb *DB) HBLogLastUpdate(ctx context.Context, l ...*DBHeartbeatLog) error {
	defer logDuration("HBLogLastUpdate", time.Now())
	const (
		insertColList         = "(`cluster_id`, `node_id`, `peer_node_id`, `name`, `state`, `beating`, `begin`, `end`)"
		valueList             = "(?, ?, ?, ?, ?, ?, NOW(), NOW())"
		onDuplicateAssignment = "`cluster_id`= VALUES(`cluster_id`), `state` = VALUES(`state`), `beating` = VALUES(`beating`), `begin` = VALUES(`begin`), `end` = VALUES(`end`)"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `hbmon_log_last` %s VALUES %s ON DUPLICATE KEY UPDATE %s", insertColList, placeholders, onDuplicateAssignment)
	args := make([]any, 0, 6*len(l))
	for _, v := range l {
		args = append(args, v.ClusterID, v.NodeID, v.PeerNodeID, v.Name, v.State, v.Beating)
	}

	_, err := oDb.ExecContext(ctx, query, args...)
	return err
}

func (oDb *DB) HBLogLastExtend(ctx context.Context, l ...*DBHeartbeatLog) error {
	defer logDuration("HBLogLastExtend", time.Now())
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat("(?,?,?),", len(l)-1) + "(?,?,?)"

	query := fmt.Sprintf("UPDATE `hbmon_log_last` SET `end` = NOW() WHERE (`node_id`,`peer_node_id`,`name`) IN (%s)", placeholders)
	args := make([]any, 0, 3*len(l))
	for _, v := range l {
		args = append(args, v.NodeID, v.PeerNodeID, v.Name)
	}

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.HbStatusLogExtend.Add(float64(count))
	}
	return nil
}

func (oDb *DB) HBLogUpdate(ctx context.Context, l ...*DBHeartbeatLog) error {
	defer logDuration("HBLogUpdate", time.Now())
	const (
		insertColList = "(`cluster_id`,`node_id`,`peer_node_id`,`name`,`state`,`beating`,`begin`,`end`)"
		valueList     = "(?, ?, ?, ?, ?, ?, ?, NOW())"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `hbmon_log` %s VALUES %s", insertColList, placeholders)
	args := make([]any, 0, 7*len(l))
	for _, v := range l {
		args = append(args, v.ClusterID, v.NodeID, v.PeerNodeID, v.Name, v.State, v.Beating, v.Begin)
	}

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.HbStatusLogChange.Add(float64(count))
	}
	return nil
}

func (o *DBHeartbeat) SameAsLog(logStatus *DBHeartbeatLog) bool {
	if logStatus == nil {
		return false
	}
	return o.State == logStatus.State && o.Beating == logStatus.Beating
}

func (o *DBHeartbeat) AsLog() *DBHeartbeatLog {
	return &DBHeartbeatLog{
		ClusterID:  o.ClusterID,
		NodeID:     o.NodeID,
		Name:       o.Name,
		PeerNodeID: o.PeerNodeID,
		State:      o.State,
		Beating:    o.Beating,
	}
}
