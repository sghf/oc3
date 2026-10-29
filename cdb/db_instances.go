package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/opensvc/oc3/feeder"
)

type (
	InstanceID struct {
		nodeID string
		svcID  string
	}

	DBInstance struct {
		SvcID  string
		NodeID string
		Frozen int64
	}

	// DBInstanceStatus
	//
	// CREATE TABLE `svcmon` (
	//  `mon_svctype` varchar(10) DEFAULT NULL,
	//  `mon_ipstatus` varchar(10) DEFAULT 'undef',
	//  `mon_fsstatus` varchar(10) DEFAULT 'undef',
	//  `mon_updated` datetime DEFAULT NULL,
	//  `ID` int(11) NOT NULL AUTO_INCREMENT,
	//  `mon_frozen` int(11) DEFAULT NULL,
	//  `mon_changed` timestamp NOT NULL DEFAULT current_timestamp(),
	//  `mon_diskstatus` varchar(10) DEFAULT 'undef',
	//  `mon_containerstatus` varchar(10) DEFAULT 'undef',
	//  `mon_overallstatus` varchar(10) DEFAULT 'undef',
	//  `mon_syncstatus` varchar(10) DEFAULT 'undef',
	//  `mon_appstatus` varchar(10) DEFAULT 'undef',
	//  `mon_hbstatus` varchar(10) DEFAULT NULL,
	//  `mon_availstatus` varchar(10) DEFAULT 'undef',
	//  `mon_vmname` varchar(50) DEFAULT '',
	//  `mon_guestos` varchar(30) DEFAULT NULL,
	//  `mon_vmem` int(11) DEFAULT 0,
	//  `mon_vcpus` float DEFAULT 0,
	//  `mon_containerpath` varchar(512) DEFAULT NULL,
	//  `mon_vmtype` varchar(10) DEFAULT NULL,
	//  `mon_sharestatus` varchar(10) DEFAULT 'undef',
	//  `node_id` char(36) CHARACTER SET ascii DEFAULT '',
	//  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
	//  `mon_smon_status` varchar(32) DEFAULT NULL,
	//  `mon_smon_global_expect` varchar(32) DEFAULT NULL,
	//  PRIMARY KEY (`ID`),
	//  UNIQUE KEY `uk_svcmon` (`node_id`,`svc_id`,`mon_vmname`),
	//  KEY `mon_vmname` (`mon_vmname`),
	//  KEY `k_node_id` (`node_id`),
	//  KEY `k_svc_id` (`svc_id`)
	//) ENGINE=InnoDB AUTO_INCREMENT=28468 DEFAULT CHARSET=utf8
	DBInstanceStatus struct {
		ID                  int64
		NodeID              string
		SvcID               string
		MonVmName           string
		MonSmonStatus       string
		MonSmonGlobalExpect string
		MonAvailStatus      string
		MonOverallStatus    string
		MonIpStatus         string
		MonDiskStatus       string
		MonFsStatus         string
		MonShareStatus      string
		MonContainerStatus  string
		MonAppStatus        string
		MonSyncStatus       string
		MonFrozen           int
		MonFrozenAt         time.Time
		MonVmType           string
		MonUpdated          string
	}

	/*
		    CREATE TABLE `svcmon_log` (
		      `id` int(11) NOT NULL AUTO_INCREMENT,
		      `mon_overallstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_ipstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_fsstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_diskstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_containerstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_syncstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_appstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_begin` datetime NOT NULL,
		      `mon_end` datetime NOT NULL,
		      `mon_hbstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_availstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		      `mon_sharestatus` varchar(10) DEFAULT 'undef',
		      `node_id` char(36) CHARACTER SET ascii DEFAULT '',
		      `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
		      PRIMARY KEY (`id`),
		      KEY `mon_overallstatus` (`mon_overallstatus`),
		      KEY `mon_begin` (`mon_begin`,`mon_end`),
		      KEY `k_node_id` (`node_id`),
		      KEY `k_svc_id` (`svc_id`)
		    ) ENGINE=InnoDB AUTO_INCREMENT=6749866 DEFAULT CHARSET=utf8

		    CREATE TABLE `svcmon_log_last` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `mon_overallstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_ipstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_fsstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_diskstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_containerstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_syncstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_appstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_begin` datetime NOT NULL,
			  `mon_end` datetime NOT NULL,
			  `mon_hbstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_availstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
			  `mon_sharestatus` varchar(10) DEFAULT 'undef',
			  `node_id` char(36) CHARACTER SET ascii DEFAULT '',
			  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
			  PRIMARY KEY (`id`),
			  UNIQUE KEY `uk` (`node_id`,`svc_id`),
			  KEY `mon_overallstatus` (`mon_overallstatus`),
			  KEY `mon_begin` (`mon_begin`,`mon_end`),
			  KEY `k_node_id` (`node_id`),
			  KEY `k_svc_id` (`svc_id`)
			) ENGINE=InnoDB AUTO_INCREMENT=15639 DEFAULT CHARSET=utf8
	*/
	DBInstanceStatusLog struct {
		ID     int64
		NodeID string
		SvcID  string

		MonAvailStatus     string
		MonOverallStatus   string
		MonIpStatus        string
		MonDiskStatus      string
		MonFsStatus        string
		MonShareStatus     string
		MonContainerStatus string
		MonAppStatus       string
		MonSyncStatus      string

		MonBeginAt time.Time
		MonEndAt   time.Time
	}
)

// SvcID is the id of the service of the instance.
func (i InstanceID) SvcID() string { return i.svcID }

// NodeID is the id of the node of the instance.
func (i InstanceID) NodeID() string { return i.nodeID }

func (i *InstanceID) String() string {
	return fmt.Sprintf("instance{obj_id:%s, node_id:%s}", i.svcID, i.nodeID)
}

func (oDb *DB) InstancesFromObjectIDs(ctx context.Context, objectIDs ...string) ([]*DBInstance, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}
	var (
		query  = `SELECT svc_id, node_id, mon_frozen FROM svcmon WHERE svc_id IN (?`
		values = make([]any, len(objectIDs))

		instances = make([]*DBInstance, 0)
	)

	values[0] = objectIDs[0]
	for i := 1; i < len(values); i++ {
		values[i] = objectIDs[i]
		query += ", ?"
	}
	query += ")"

	rows, err := oDb.DB.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, fmt.Errorf("instancesFromObjectIDs query: %w", err)
	}
	if rows == nil {
		return nil, fmt.Errorf("instancesFromObjectIDs query returns nil rows")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o DBInstance
		var frozen sql.NullInt64
		if err := rows.Scan(&o.SvcID, &o.NodeID, &frozen); err != nil {
			return nil, fmt.Errorf("instancesFromObjectIDs scan: %w", err)
		}
		o.Frozen = frozen.Int64
		instances = append(instances, &o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("instancesFromObjectIDs query rows: %w", err)
	}
	return instances, nil
}

// SvcmonRefreshTimestamp updates svcmon.mon_updated, svcmon_log_last.mon_end for object ids with node id.
func (oDb *DB) SvcmonRefreshTimestamp(ctx context.Context, nodeID string, objectIDs ...string) (updates bool, err error) {
	defer logDuration("SvcmonRefreshTimestamp", time.Now())
	const (
		qUpdateSvcmon        = "UPDATE `svcmon` SET `mon_updated` = NOW() WHERE `node_id` = ? AND `svc_id` IN (%s)"
		qUpdateSvcmonLogLast = "UPDATE `svcmon_log_last` SET `mon_end` = NOW() WHERE `node_id` = ? AND `svc_id` IN (%s)"
	)
	var (
		count int64
		query string
	)
	begin := time.Now()
	if len(objectIDs) == 0 {
		return
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"
	args := make([]any, len(objectIDs)+1)
	args[0] = nodeID
	for i, v := range objectIDs {
		args[i+1] = v
	}

	query = fmt.Sprintf(qUpdateSvcmon, placeholders)
	if count, err = oDb.execCountContext(ctx, query, args...); err != nil {
		return
	} else if count > 0 {
		updates = true
		oDb.SetChange("svcmon")
	}

	query = fmt.Sprintf(qUpdateSvcmonLogLast, placeholders)
	if _, err = oDb.ExecContext(ctx, query, args...); err != nil {
		return
	}

	slog.Debug(fmt.Sprintf("SvcmonRefreshTimestamp %s", time.Since(begin)))
	return
}

// InstancePingFromNodeID refreshes svcmon.mon_updated, svcmon_log_last.mon_end,
// resmon.updated and resmon_log_last.res_end of the node_id rows, when older
// than 30s.
func (oDb *DB) InstancePingFromNodeID(ctx context.Context, nodeID string) (updates bool, err error) {
	defer logDuration("instancePing "+nodeID, time.Now())
	const (
		qUpdateSvcmon = `UPDATE svcmon SET mon_updated = NOW()
			WHERE node_id = ? AND mon_updated < DATE_SUB(NOW(), INTERVAL 30 SECOND)`

		qUpdateSvcmonLogLast = `UPDATE svcmon_log_last SET mon_end = NOW()
			WHERE node_id = ? AND mon_end < DATE_SUB(NOW(), INTERVAL 30 SECOND)`

		qUpdateResmon = `UPDATE resmon SET updated = NOW()
			WHERE node_id = ? AND updated < DATE_SUB(NOW(), INTERVAL 30 SECOND)`

		// Only the resources the node still has: res_end of a resource gone
		// stays when it went, which the scrub_resmon task purges by.
		qUpdateResmonLogLast = `UPDATE resmon_log_last l
			JOIN resmon r ON r.svc_id = l.svc_id AND r.node_id = l.node_id AND r.rid = l.rid
			SET l.res_end = NOW()
			WHERE l.node_id = ? AND l.res_end < DATE_SUB(NOW(), INTERVAL 30 SECOND)`
	)
	var (
		count int64
	)

	// svcmon and resmon are refreshed independently: svcmon.mon_updated may
	// have been refreshed by another path (daemon status) without refreshing
	// all resmon rows, so skipping resmon when svcmon has no update would let
	// resmon.updated age until the resources are scrubbed.
	if count, err = oDb.execCountContext(ctx, qUpdateSvcmon, nodeID); err != nil {
		return
	} else if count > 0 {
		updates = true
		oDb.SetChange("svcmon")
	}

	if _, err = oDb.ExecContext(ctx, qUpdateSvcmonLogLast, nodeID); err != nil {
		return
	}

	if count, err = oDb.execCountContext(ctx, qUpdateResmon, nodeID); err != nil {
		return
	} else if count > 0 {
		updates = true
		oDb.SetChange("resmon")
	}

	_, err = oDb.ExecContext(ctx, qUpdateResmonLogLast, nodeID)
	return
}

func (oDb *DB) DeleteNodeIDSvcmonInstances(ctx context.Context, nodeID string, objectIDs ...string) error {
	defer logDuration("DeleteNodeIDSvcmonInstances", time.Now())
	if len(objectIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"
	query := fmt.Sprintf("DELETE FROM `svcmon` WHERE `node_id` = ? AND `svc_id` IN (%s)", placeholders)

	args := make([]any, len(objectIDs)+1)
	args[0] = nodeID
	for i, v := range objectIDs {
		args[i+1] = v
	}
	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return fmt.Errorf("DeleteNodeIDSvcmonInstances %s [%v]: %w", nodeID, objectIDs, err)
	} else if count > 0 {
		oDb.SetChange("svcmon")
	}
	return nil
}

func (oDb *DB) SvcmonLogLastFromObjectIDs(ctx context.Context, objectIDs ...string) ([]*DBInstanceStatusLog, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}
	var (
		querySelect = "SELECT `svc_id`, `node_id`, `mon_begin`, `id`, `mon_availstatus`, `mon_overallstatus`, `mon_syncstatus`, `mon_ipstatus`, `mon_fsstatus`," +
			" `mon_diskstatus`, `mon_sharestatus`, `mon_containerstatus`, `mon_appstatus`" +
			"FROM `svcmon_log_last` WHERE `svc_id` IN (%s)"
		args = make([]any, len(objectIDs))

		l = make([]*DBInstanceStatusLog, 0)
	)

	if len(objectIDs) == 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"
	query := fmt.Sprintf(querySelect, placeholders)

	for i, v := range objectIDs {
		args[i] = v
	}

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("InstancesStatusLogFromObjectIDs query: %w", err)
	}
	if rows == nil {
		return nil, fmt.Errorf("InstancesStatusLogFromObjectIDs query: got nil rows result")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var r DBInstanceStatusLog
		if err := rows.Scan(&r.SvcID, &r.NodeID, &r.MonBeginAt, &r.ID,
			&r.MonAvailStatus, &r.MonOverallStatus, &r.MonSyncStatus, &r.MonIpStatus, &r.MonFsStatus,
			&r.MonDiskStatus, &r.MonShareStatus, &r.MonContainerStatus, &r.MonAppStatus); err != nil {
			return nil, fmt.Errorf("InstancesStatusLogFromObjectIDs scan: %w", err)
		}
		l = append(l, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("InstancesStatusLogFromObjectIDs query rows: %w", err)
	}
	return l, nil
}

func (oDb *DB) SvcmonLogLastUpdate(ctx context.Context, l ...*DBInstanceStatusLog) error {
	defer logDuration("SvcmonLogLastUpdate", time.Now())
	if len(l) == 0 {
		return nil
	}
	columns := []string{"svc_id", "node_id", "mon_availstatus", "mon_overallstatus", "mon_syncstatus", "mon_ipstatus", "mon_fsstatus",
		"mon_diskstatus", "mon_sharestatus", "mon_containerstatus", "mon_appstatus"}
	rows := make([][]any, len(l))
	for i, v := range l {
		rows[i] = []any{v.SvcID, v.NodeID, v.MonAvailStatus, v.MonOverallStatus, v.MonSyncStatus, v.MonIpStatus,
			v.MonFsStatus, v.MonDiskStatus, v.MonShareStatus, v.MonContainerStatus, v.MonAppStatus}
	}
	given, args := givenRows(columns, rows)
	update := []string{"mon_begin = s.mon_begin", "mon_end = s.mon_end"}
	for _, c := range columns[2:] {
		update = append(update, c+" = s."+c)
	}
	_, _, err := oDb.upsertRows(ctx, rowsUpsert{
		table:   "svcmon_log_last",
		source:  "SELECT r.*, NOW() AS mon_begin, NOW() AS mon_end FROM (" + given + ") r",
		args:    args,
		columns: append(columns, "mon_begin", "mon_end"),
		// The unique key uk.
		match:  "t.node_id = s.node_id AND t.svc_id = s.svc_id",
		update: update,
	})
	return err
}

func (oDb *DB) SvcmonLogLastExtend(ctx context.Context, nodeID string, objectIDs ...string) error {
	defer logDuration("SvcmonLogLastExtend", time.Now())
	if len(objectIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"

	query := fmt.Sprintf("UPDATE `svcmon_log_last` SET `mon_end` = NOW() WHERE `node_id` = ? AND `svc_id` in (%s)", placeholders)
	args := make([]any, len(objectIDs)+1)
	args[0] = nodeID
	for i, v := range objectIDs {
		args[i+1] = v
	}

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.InstanceStatusLogExtend.Add(float64(count))
	}
	return nil
}

func (oDb *DB) SvcmonLogUpdate(ctx context.Context, l ...*DBInstanceStatusLog) error {
	defer logDuration("instanceStatusLogUpdate", time.Now())
	const (
		insertColList = "(`svc_id`, `node_id`, `mon_availstatus`, `mon_overallstatus`, `mon_syncstatus`, `mon_ipstatus`, `mon_fsstatus`," +
			" `mon_diskstatus`, `mon_sharestatus`, `mon_containerstatus`, `mon_appstatus`, `mon_begin`, `mon_end`)"
		valueList = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `svcmon_log` %s VALUES %s", insertColList, placeholders)
	args := make([]any, 0, 12*len(l))
	for _, v := range l {
		args = append(args, v.SvcID, v.NodeID, v.MonAvailStatus, v.MonOverallStatus, v.MonSyncStatus, v.MonIpStatus,
			v.MonFsStatus, v.MonDiskStatus, v.MonShareStatus, v.MonContainerStatus, v.MonAppStatus, v.MonBeginAt)
	}

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.InstanceStatusLogChange.Add(float64(count))
	}
	return nil
}

func (oDb *DB) SvcmonUpdate(ctx context.Context, l ...*DBInstanceStatus) error {
	defer logDuration("SvcmonUpdate", time.Now())
	if len(l) == 0 {
		return nil
	}
	columns := []string{"svc_id", "node_id", "mon_vmname",
		"mon_smon_status", "mon_smon_global_expect", "mon_availstatus",
		"mon_overallstatus", "mon_ipstatus", "mon_diskstatus", "mon_fsstatus",
		"mon_sharestatus", "mon_containerstatus", "mon_appstatus", "mon_syncstatus",
		"mon_frozen", "mon_frozen_at", "mon_vmtype"}
	rows := make([][]any, len(l))
	for i, v := range l {
		rows[i] = []any{v.SvcID, v.NodeID, v.MonVmName,
			v.MonSmonStatus, v.MonSmonGlobalExpect, v.MonAvailStatus,
			v.MonOverallStatus, v.MonIpStatus, v.MonDiskStatus, v.MonFsStatus,
			v.MonShareStatus, v.MonContainerStatus, v.MonAppStatus, v.MonSyncStatus,
			v.MonFrozen, v.MonFrozenAt, v.MonVmType}
	}
	given, args := givenRows(columns, rows)
	update := make([]string, 0, len(columns))
	for _, c := range columns[3:] {
		update = append(update, c+" = s."+c)
	}
	changed, inserted, err := oDb.upsertRows(ctx, rowsUpsert{
		table:   "svcmon",
		source:  "SELECT r.*, NOW() AS mon_updated FROM (" + given + ") r",
		args:    args,
		columns: append(columns, "mon_updated"),
		// The unique key uk_svcmon.
		match:  "t.node_id = s.node_id AND t.svc_id = s.svc_id AND t.mon_vmname = s.mon_vmname",
		update: append(update, "mon_updated = s.mon_updated"),
	})
	if err != nil {
		return fmt.Errorf("instance status count %d: %w", len(l), err)
	}
	if count := changed + inserted; count > 0 {
		oDb.SetChange("svcmon")
		oDb.Metrics.InstanceStatusUpdate.Add(float64(count))
	}
	return nil
}

func (oDb *DB) DeleteNodeIDResmonInstances(ctx context.Context, nodeID string, objectIDs ...string) error {
	defer logDuration("DeleteNodeIDResmonInstances", time.Now())
	if len(objectIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"
	query := fmt.Sprintf("DELETE FROM `resmon` WHERE `node_id` = ? AND `svc_id` IN (%s)", placeholders)

	args := make([]any, len(objectIDs)+1)
	args[0] = nodeID
	for i, v := range objectIDs {
		args[i+1] = v
	}
	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return fmt.Errorf("DeleteNodeIDResmonInstances %s [%v]: %w", nodeID, objectIDs, err)
	} else if count > 0 {
		oDb.SetChange("resmon")
		oDb.Metrics.ResourceStatusDelete.Add(float64(count))
	}
	return nil
}

func (oDb *DB) InstanceResourcesDeleteObsolete(ctx context.Context, svcID, nodeID string, maxTime time.Time) error {
	defer logDuration("InstanceResourcesDeleteObsolete "+svcID+"@"+nodeID+":", time.Now())
	const (
		queryPurge = "" +
			"DELETE FROM `resmon`" +
			"WHERE `svc_id` = ? AND `node_id` = ? AND `updated` < ?"
	)
	if count, err := oDb.execCountContext(ctx, queryPurge, svcID, nodeID, maxTime); err != nil {
		return fmt.Errorf("InstanceResourcesDeleteObsolete: %w", err)
	} else {
		slog.Debug(fmt.Sprintf("InstanceResourcesDeleteObsolete %s@%s: %d", svcID, nodeID, count))
		if count > 0 {
			oDb.Metrics.ResourceStatusDelete.Add(float64(count))
		}
	}
	return nil
}

func (oDb *DB) InstanceResourceInfoUpdate(ctx context.Context, svcID, nodeID string, data feeder.InstanceResourceInfo) error {
	defer logDuration("InstanceResourceInfoUpdate "+svcID+"@"+nodeID, time.Now())
	const (
		insertColList         = "(`svc_id`, `node_id`, `rid`, `res_key`, `topology`, `res_value`, `updated`)"
		valueList             = "(?, ?, ?, ?, ?, ?, NOW())"
		onDuplicateAssignment = "`topology`=VALUES(`topology`),`res_value`=VALUES(`res_value`),`updated`=NOW()"
	)
	if len(data.Info) == 0 {
		return nil
	}
	var (
		args     []any
		value    string
		resCount int
	)
	for _, info := range data.Info {
		for _, key := range info.Keys {
			switch v := key.Value.(type) {
			case string:
				value = v
			case float64:
				value = fmt.Sprintf("%f", v)
			default:
				continue
			}
			if len(value) > 255 {
				value = value[:255]
			}
			resCount++
			args = append(args, svcID, nodeID, info.Rid, key.Key, data.Topology, value)
		}
	}
	if resCount == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+",", resCount-1) + valueList
	query := fmt.Sprintf("INSERT INTO `resinfo` %s VALUES %s ON DUPLICATE KEY UPDATE %s",
		insertColList, placeholders, onDuplicateAssignment)

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("resinfo")
		oDb.Metrics.ResourceInfoUpdate.Add(float64(count))
	}
	return nil
}

func (oDb *DB) InstanceResourceInfoDelete(ctx context.Context, svcID, nodeID string, maxTime time.Time) error {
	defer logDuration("InstanceResourceInfoDelete "+svcID+"@"+nodeID, time.Now())
	const query = "DELETE FROM `resinfo` WHERE `svc_id` = ? AND `node_id` = ? AND `updated` < ?"
	if count, err := oDb.execCountContext(ctx, query, svcID, nodeID, maxTime); err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	} else if count > 0 {
		oDb.SetChange("resinfo")
		oDb.Metrics.ResourceInfoDelete.Add(float64(count))
	}
	return nil
}

// GetOrphanInstances returns list of InstanceID defined on svcmon for nodeIDs but where
// the associated services.svcname is not into objectNames
//
//	SELECT `svcmon`.`svc_id`, `svcmon`.`node_id`
//	FROM `services`, `svcmon`
//	WHERE
//			`svcmon`.`node_id` IN ('nodeID1','nodeID2')
//		AND
//	    	`svcmon`.`svc_id` = `services`.`svc_id`
//		AND
//			`services`.`svcname` NOT IN ('obj1','obj2',...)
func (oDb *DB) GetOrphanInstances(ctx context.Context, nodeIDs, objectNames []string) (instanceIDs []InstanceID, err error) {
	var (
		query = "SELECT `svcmon`.`svc_id`, `svcmon`.`node_id` FROM `services`, `svcmon`"

		queryArgs []any

		rows *sql.Rows
	)

	if len(nodeIDs) == 0 || len(objectNames) == 0 {
		return
	}
	query += " WHERE `svcmon`.`node_id` IN (?"
	queryArgs = append(queryArgs, nodeIDs[0])
	for i := 1; i < len(nodeIDs); i++ {
		query += ", ?"
		queryArgs = append(queryArgs, nodeIDs[i])
	}
	query += " ) AND `svcmon`.`svc_id` = `services`.`svc_id`"
	query += " AND `services`.`svcname` NOT IN ( ?"
	queryArgs = append(queryArgs, objectNames[0])
	for i := 1; i < len(objectNames); i++ {
		query += ", ?"
		queryArgs = append(queryArgs, objectNames[i])
	}
	query += " )"
	rows, err = oDb.DB.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var instanceID InstanceID
		if err = rows.Scan(&instanceID.svcID, &instanceID.nodeID); err != nil {
			return
		}
		instanceIDs = append(instanceIDs, instanceID)
	}
	err = rows.Err()
	return
}

func (oDb *DB) PurgeInstance(ctx context.Context, id InstanceID) error {
	const (
		where = "WHERE `svc_id` = ? and `node_id` = ?"
	)

	var (
		tables = []string{
			"svcmon", "dashboard", "dashboard_events", "svcdisks", "resmon",
			"checks_live", "comp_status", "action_queue", "resinfo", "saves",
		}

		err error
	)
	slog.Debug(fmt.Sprintf("purging %s", id))
	for _, tableName := range tables {
		request := fmt.Sprintf("DELETE FROM %s WHERE `svc_id` = ? and `node_id` = ?", tableName)
		if count, err1 := oDb.execCountContext(ctx, request, id.svcID, id.nodeID); err1 != nil {
			err = errors.Join(err, fmt.Errorf("delete from %s: %w", tableName, err1))
			continue
		} else if count > 0 {
			slog.Debug(fmt.Sprintf("purged %s from table %s", id, tableName))
			oDb.SetChange(tableName)
		}
	}
	return err
}

// InstancesOutdated returns the svcmon instance ids not updated since maxAge.
func (oDb *DB) InstancesOutdated(ctx context.Context, maxAge time.Duration) (instanceIDs []InstanceID, err error) {
	var rows *sql.Rows
	query := "SELECT `svc_id`, `node_id` " +
		"FROM `svcmon` " +
		"WHERE `mon_updated` < DATE_SUB(NOW(), INTERVAL ? SECOND)"
	rows, err = oDb.DB.QueryContext(ctx, query, maxAgeSeconds(maxAge))
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var instanceID InstanceID
		if err = rows.Scan(&instanceID.svcID, &instanceID.nodeID); err != nil {
			return
		}
		instanceIDs = append(instanceIDs, instanceID)
	}
	err = rows.Err()
	return
}

func (oDb *DB) LogInstancesNotUpdated(ctx context.Context, maxAge time.Duration) error {
	request := fmt.Sprintf(`INSERT IGNORE INTO log (log_action, log_user, log_fmt, log_dict, log_date, svc_id,
                 log_gtalk_sent, log_email_sent, log_entry_id, log_level, node_id)
               SELECT
                      "service.status",
                      "scheduler",
                      "instance status not updated for more than %s (%%(date)s)",
                      CONCAT('{"date": "', mon_updated, '"}'),
                      NOW(),
                      svc_id,
                      0,
                      0,
                      MD5(CONCAT("service.status.notupdated",node_id,svc_id,mon_updated)),
                      "warning",
                      node_id
               from svcmon
               where mon_updated<DATE_SUB(NOW(), INTERVAL ? SECOND)`, FormatMaxAge(maxAge))
	if count, err := oDb.execCountContext(ctx, request, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		slog.Debug(fmt.Sprintf("alert: instance outdated: %d", count))
		oDb.SetChange("log")
	} else {
		slog.Debug("alert: no instance outdated")
	}
	return nil
}

func (iStatus *DBInstanceStatus) SameAsLog(logStatus *DBInstanceStatusLog) bool {
	if logStatus == nil {
		return false
	}
	return iStatus.MonAvailStatus == logStatus.MonAvailStatus &&
		iStatus.MonOverallStatus == logStatus.MonOverallStatus &&
		iStatus.MonSyncStatus == logStatus.MonSyncStatus &&
		iStatus.MonIpStatus == logStatus.MonIpStatus &&
		iStatus.MonFsStatus == logStatus.MonFsStatus &&
		iStatus.MonDiskStatus == logStatus.MonDiskStatus &&
		iStatus.MonShareStatus == logStatus.MonShareStatus &&
		iStatus.MonContainerStatus == logStatus.MonContainerStatus &&
		iStatus.MonAppStatus == logStatus.MonAppStatus
}

func (iStatus *DBInstanceStatus) AsLog() *DBInstanceStatusLog {
	return &DBInstanceStatusLog{
		NodeID:             iStatus.NodeID,
		SvcID:              iStatus.SvcID,
		MonAvailStatus:     iStatus.MonAvailStatus,
		MonOverallStatus:   iStatus.MonOverallStatus,
		MonIpStatus:        iStatus.MonIpStatus,
		MonDiskStatus:      iStatus.MonDiskStatus,
		MonFsStatus:        iStatus.MonFsStatus,
		MonShareStatus:     iStatus.MonShareStatus,
		MonContainerStatus: iStatus.MonContainerStatus,
		MonAppStatus:       iStatus.MonAppStatus,
		MonSyncStatus:      iStatus.MonSyncStatus,
	}
}
