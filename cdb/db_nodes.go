package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type (
	DBNode struct {
		Nodename      string
		Frozen        string
		NodeID        string
		ClusterID     string
		App           string
		NodeEnv       string
		LocAddr       string
		LocCountry    string
		LocCity       string
		locZip        string
		LocBuilding   string
		LocFloor      string
		LocRoom       string
		LocRack       string
		EnclosureSlot string
		Enclosure     string
		Hv            string
		Tz            string
	}
)

func (n *DBNode) String() string {
	return fmt.Sprintf("node: {nodename: %s, node_id: %s, cluster_id: %s, app: %s}", n.Nodename, n.NodeID, n.ClusterID, n.App)
}

func (oDb *DB) NodeByNodeID(ctx context.Context, nodeID string) (*DBNode, error) {
	defer logDuration("nodeByNodeID", time.Now())
	if nodeID == "" {
		return nil, fmt.Errorf("nodeByNodeID: called with empty node id")
	}
	var (
		query = `SELECT nodename, cluster_id, node_env, app, hv, node_frozen,
				loc_country, loc_city, loc_addr, loc_building, loc_floor, loc_room,
				loc_rack, loc_zip, enclosure, enclosureslot, tz
			FROM nodes WHERE node_id = ? LIMIT 1`

		nodename, clusterID, nodeEnv, app, hv, frozen, locCountry sql.NullString
		locCity, locAddr, locBuilding, locFloor, locRoom, locRack sql.NullString
		locZip, enclosure, enclosureSlot, tz                      sql.NullString
	)
	err := oDb.DB.
		QueryRowContext(ctx, query, nodeID).
		Scan(
			&nodename, &clusterID, &nodeEnv, &app, &hv, &frozen,
			&locCountry, &locCity, &locAddr, &locBuilding, &locFloor, &locRoom,
			&locRack, &locZip, &enclosure, &enclosureSlot, &tz)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	default:
		node := DBNode{
			Nodename:      nodename.String,
			Frozen:        frozen.String,
			NodeID:        nodeID,
			ClusterID:     clusterID.String,
			App:           app.String,
			NodeEnv:       nodeEnv.String,
			LocAddr:       locAddr.String,
			LocCountry:    locCountry.String,
			LocCity:       locCity.String,
			locZip:        locZip.String,
			LocBuilding:   locBuilding.String,
			LocFloor:      locFloor.String,
			LocRoom:       locRoom.String,
			LocRack:       locRack.String,
			Enclosure:     enclosure.String,
			EnclosureSlot: enclosureSlot.String,
			Hv:            hv.String,
			Tz:            tz.String,
		}
		return &node, nil
	}
}

func (oDb *DB) NodeByNodeIDOrNodename(ctx context.Context, nodeIdOrName string) (*DBNode, error) {
	defer logDuration("nodeByNodeIDOrNodename", time.Now())
	if nodeIdOrName == "" {
		return nil, fmt.Errorf("nodeByNodeIDOrNodename: called with empty node ID or name")
	}

	// Valid UUID : should be a node_id
	if _, err := uuid.Parse(nodeIdOrName); err == nil {
		n, err := oDb.NodeByNodeID(ctx, nodeIdOrName)
		if err != nil {
			return nil, err
		}
		return n, nil
	}

	// Otherwise treat it as a nodename and resolve the node_id first.
	const query = `SELECT node_id FROM nodes WHERE nodename = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, nodeIdOrName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var nodeIDs []string
	for rows.Next() {
		var nodeID sql.NullString
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		if nodeID.Valid {
			nodeIDs = append(nodeIDs, nodeID.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	switch len(nodeIDs) {
	case 0:
		return nil, fmt.Errorf("node %s not found", nodeIdOrName)
	case 1:
		n, err := oDb.NodeByNodeID(ctx, nodeIDs[0])
		if err != nil {
			return nil, err
		}
		return n, nil
	default:
		return nil, fmt.Errorf("nodeByNodeIDOrNodename: multiple node_ids found for nodename %s", nodeIdOrName)
	}
}

func (oDb *DB) ClusterNodesFromNodeID(ctx context.Context, nodeID string) (dbNodes []*DBNode, err error) {
	defer logDuration("ClusterNodesFromNodeID", time.Now())
	if nodeID == "" {
		err = fmt.Errorf("ClusterNodesFromNodeID: called with empty node id")
		return
	}
	var (
		rows *sql.Rows

		query = `SELECT nodename, node_id, cluster_id, node_env, app, hv, node_frozen,
				loc_country, loc_city, loc_addr, loc_building, loc_floor, loc_room, loc_rack, loc_zip,
				enclosure, enclosureslot
			FROM nodes
			WHERE cluster_id IN (SELECT cluster_id FROM nodes WHERE node_id = ?)`
	)

	rows, err = oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			nodename, nodeID, clusterID, nodeEnv, app, hv, frozen, locCountry sql.NullString
			locCity, locAddr, locBuilding, locFloor, locRoom, locRack, locZip sql.NullString
			enclosure, enclosureSlot                                          sql.NullString
		)
		err = rows.Scan(
			&nodename, &nodeID, &clusterID, &nodeEnv, &app, &hv, &frozen,
			&locCountry, &locCity, &locAddr, &locBuilding, &locFloor, &locRoom, &locRack, &locZip,
			&enclosure, &enclosureSlot)
		if err != nil {
			return
		}

		dbNodes = append(dbNodes, &DBNode{
			Nodename:      nodename.String,
			Frozen:        frozen.String,
			NodeID:        nodeID.String,
			ClusterID:     clusterID.String,
			App:           app.String,
			NodeEnv:       nodeEnv.String,
			LocAddr:       locAddr.String,
			LocCountry:    locCountry.String,
			LocCity:       locCity.String,
			locZip:        locZip.String,
			LocBuilding:   locBuilding.String,
			LocFloor:      locFloor.String,
			LocRoom:       locRoom.String,
			LocRack:       locRack.String,
			Enclosure:     enclosure.String,
			EnclosureSlot: enclosureSlot.String,
			Hv:            hv.String,
		})
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

func (oDb *DB) NodesFromClusterIDWithNodenames(ctx context.Context, clusterID string, nodes []string) (dbNodes []*DBNode, err error) {
	defer logDuration("nodesFromClusterIDWithNodenames", time.Now())
	if len(nodes) == 0 {
		err = fmt.Errorf("nodesFromClusterIDWithNodenames: need nodes")
		return
	}
	var (
		rows *sql.Rows

		query = `SELECT nodename, node_id, node_env, app, hv, node_frozen,
			loc_country, loc_city, loc_addr, loc_building, loc_floor, loc_room, loc_rack, loc_zip,
		    enclosure, enclosureslot
		FROM nodes
		WHERE cluster_id = ? AND nodename IN (?`
	)
	args := []any{clusterID, nodes[0]}
	for i := 1; i < len(nodes); i++ {
		query += ", ?"
		args = append(args, nodes[i])
	}
	query += ")"

	rows, err = oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			nodename, nodeID, nodeEnv, app, hv, frozen, locCountry            sql.NullString
			locCity, locAddr, locBuilding, locFloor, locRoom, locRack, locZip sql.NullString
			enclosure, enclosureSlot                                          sql.NullString
		)
		err = rows.Scan(
			&nodename, &nodeID, &nodeEnv, &app, &hv, &frozen,
			&locCountry, &locCity, &locAddr, &locBuilding, &locFloor, &locRoom, &locRack, &locZip,
			&enclosure, &enclosureSlot)
		if err != nil {
			return
		}

		dbNodes = append(dbNodes, &DBNode{
			Nodename:      nodename.String,
			Frozen:        frozen.String,
			NodeID:        nodeID.String,
			ClusterID:     clusterID,
			App:           app.String,
			NodeEnv:       nodeEnv.String,
			LocAddr:       locAddr.String,
			LocCountry:    locCountry.String,
			LocCity:       locCity.String,
			locZip:        locZip.String,
			LocBuilding:   locBuilding.String,
			LocFloor:      locFloor.String,
			LocRoom:       locRoom.String,
			LocRack:       locRack.String,
			Enclosure:     enclosure.String,
			EnclosureSlot: enclosureSlot.String,
			Hv:            hv.String,
		})
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

func (oDb *DB) NodeContainerUpdateFromParentNode(ctx context.Context, cName, cApp string, pn *DBNode) error {
	const queryUpdate = `UPDATE nodes
    	SET updated = NOW(),
    	    loc_addr = ?, loc_country = ?, loc_zip = ?, loc_city = ?, loc_building = ?,
    	    loc_floor = ?, loc_room = ?, loc_rack = ?, hv = ?, enclosure = ?, enclosureslot = ?
    	`
	const queryWhere1 = ` WHERE nodename = ? AND app in (?, ?)`

	result, err := oDb.DB.ExecContext(ctx, queryUpdate+queryWhere1,
		pn.LocAddr, pn.LocCountry, pn.locZip, pn.LocCity, pn.LocBuilding,
		pn.LocFloor, pn.LocRoom, pn.LocRack, pn.Hv, pn.Enclosure, pn.EnclosureSlot,
		cName, pn.App, cApp)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("nodes")
		return nil
	} else {
		apps, err := oDb.ResponsibleAppsForNode(ctx, pn.NodeID)
		if err != nil {
			return err
		}
		if len(apps) == 0 {
			slog.Debug(fmt.Sprintf("nodeContainerUpdateFromParentNode responsibleAppsForNode hostname %s on %s no apps",
				cName, pn.NodeID))
			return nil
		}
		var queryWhere2 = ` WHERE nodename = ? AND app in (?`
		var args = []any{
			pn.LocAddr, pn.LocCountry, pn.locZip, pn.LocCity, pn.LocBuilding,
			pn.LocFloor, pn.LocRoom, pn.LocRack, pn.Hv, pn.Enclosure, pn.EnclosureSlot,
			cName, apps[0]}
		for i := 1; i < len(apps); i++ {
			queryWhere2 += `, ?`
			args = append(args, apps[i])
		}
		queryWhere2 += `)`
		result, err := oDb.DB.ExecContext(ctx, queryUpdate+queryWhere2, args...)
		if err != nil {
			return err
		}
		if count, err := result.RowsAffected(); err != nil {
			return err
		} else if count > 0 {
			oDb.SetChange("nodes")
			return nil
		}
	}
	return nil
}

func (oDb *DB) NodeUpdateFrozen(ctx context.Context, nodeID, frozen string) error {
	const query = `UPDATE nodes SET node_frozen = ? WHERE node_id = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, frozen, nodeID); err != nil {
		return fmt.Errorf("nodeUpdateFrozen: %w", err)
	}
	oDb.SetChange("nodes")
	return nil
}

// NodeUpdateClusterIDForNodeID update cluster_id value on nodes with node_id. the returned bool indicate table has been updated
func (oDb *DB) NodeUpdateClusterIDForNodeID(ctx context.Context, nodeID, clusterID string) (bool, error) {
	const (
		querySearch = `SELECT cluster_id FROM nodes WHERE node_id = ? and cluster_id = ? LIMIT 1`
		queryUpdate = `UPDATE nodes SET cluster_id = ? WHERE node_id = ?`
	)
	var (
		s string
	)
	row := oDb.DB.QueryRowContext(ctx, querySearch, nodeID, clusterID)
	err := row.Scan(&s)
	switch err {
	case nil:
		// found node with nodeID and clusterID
		return false, nil
	case sql.ErrNoRows:
		if result, err := oDb.DB.ExecContext(ctx, queryUpdate, clusterID, nodeID); err != nil {
			return false, fmt.Errorf("NodeUpdateClusterIDForNodeID update: %w", err)
		} else if count, err := result.RowsAffected(); err != nil {
			return false, fmt.Errorf("NodeUpdateClusterIDForNodeID count updated: %w", err)
		} else if count > 0 {
			oDb.SetChange("nodes")
			return true, nil
		} else {
			return false, nil
		}
	default:
		return false, fmt.Errorf("NodeUpdateClusterIDForNodeID check cluster_id: %w", err)
	}
}

func (oDb *DB) PurgeNodeHBAsOutdated(ctx context.Context) error {
	request := fmt.Sprintf("DELETE FROM `node_hba` WHERE `updated` < DATE_SUB(NOW(), INTERVAL 7 DAY)")
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		slog.Info(fmt.Sprintf("purged %d entries from table node_hba", rowAffected))
		oDb.SetChange("node_hba")
	}
	return nil
}

func (oDb *DB) AlertMACDup(ctx context.Context) error {
	request := `
		-- Step 1: Find duplicates and prepare data for insert/update
		INSERT INTO dashboard (
		  dash_type, dash_severity, node_id, svc_id,
		  dash_fmt, dash_dict, dash_created, dash_env, dash_updated
		)
		SELECT
		  "mac duplicate" AS dash_type,
		  IF(nodes.node_env = "PRD", 4, 3) AS dash_severity,
		  nodes.node_id,
		  "" AS svc_id,
		  CONCAT("mac ", duplicates.mac, " reported by nodes ", duplicates.node_names) AS dash_fmt,
		  CONCAT('{"mac": "', duplicates.mac, '", "nodes": "', duplicates.node_names, '"}') AS dash_dict,
		  NOW() AS dash_created,
		  nodes.node_env AS dash_env,
		  NOW() AS dash_updated
		FROM (
		  -- Subquery to find duplicate MACs and their associated node_ids
		  SELECT
		    t.mac,
		    GROUP_CONCAT(DISTINCT t.node_id ORDER BY t.node_id) AS node_ids,
		    GROUP_CONCAT(DISTINCT t.nodename ORDER BY t.nodename) AS node_names
		  FROM (
		      SELECT
		        mac,
			node_ip.node_id AS node_id,
			nodes.nodename AS nodename
		      FROM node_ip
		      JOIN nodes ON nodes.node_id = node_ip.node_id
		      WHERE
		        node_ip.intf NOT LIKE "%:%" AND
		        node_ip.intf NOT LIKE "usbecm%" AND
		        node_ip.intf NOT LIKE "docker%" AND
		        node_ip.mac != "00:00:00:00:00:00" AND
		        node_ip.mac != "2:21:28:57:47:17" AND
		        node_ip.mac != "0:0:0:0:0:0" AND
		        node_ip.mac != "00:16:3e:00:00:00" AND
		        node_ip.mac != "0" AND
		        node_ip.mac != "" AND
		        node_ip.updated > DATE_SUB(NOW(), INTERVAL 1 DAY)
		      GROUP BY mac, node_ip.node_id
		  ) AS t
		  GROUP BY t.mac
		  HAVING COUNT(t.mac) > 1
		) AS duplicates
		JOIN nodes ON FIND_IN_SET(nodes.node_id, duplicates.node_ids)
		ON DUPLICATE KEY UPDATE
		  dash_fmt = VALUES(dash_fmt),
		  dash_dict = VALUES(dash_dict),
		  dash_env = VALUES(dash_env),
		  dash_updated = VALUES(dash_updated)
		`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}

	request = `DELETE FROM dashboard
		   WHERE
		     dash_type = "mac duplicate" AND
		     dash_updated < DATE_SUB(NOW(), INTERVAL 1 DAY)`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}

	return nil
}

func (oDb *DB) UpdateVirtualAssets(ctx context.Context) error {
	request := `UPDATE svcmon m, nodes n, nodes n2
          SET
           n2.loc_addr=n.loc_addr,
           n2.loc_city=n.loc_city,
           n2.loc_zip=n.loc_zip,
           n2.loc_room=n.loc_room,
           n2.loc_building=n.loc_building,
           n2.loc_floor=n.loc_floor,
           n2.loc_rack=n.loc_rack,
           n2.loc_country=n.loc_country,
           n2.power_cabinet1=n.power_cabinet1,
           n2.power_cabinet2=n.power_cabinet2,
           n2.power_supply_nb=n.power_supply_nb,
           n2.power_protect=n.power_protect,
           n2.power_protect_breaker=n.power_protect_breaker,
           n2.power_breaker1=n.power_breaker1,
           n2.power_breaker2=n.power_breaker2,
           n2.enclosure=n.enclosure
          WHERE
           m.node_id=n.node_id AND
           m.mon_vmname=n2.nodename AND
           m.mon_vmtype IN ('ldom', 'hpvm', 'kvm', 'xen', 'vbox', 'ovm', 'esx', 'zone', 'lxc', 'jail', 'vz', 'srp') and
           m.mon_containerstatus IN ("up", "stdby up", "warn")`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("nodes")
	}
	return nil
}

func (oDb *DB) UpdateVirtualAsset(ctx context.Context, svcID, nodeID string) error {
	request := `
	UPDATE nodes n
        JOIN (
            SELECT
                svcmon.mon_vmname AS vmname,
                services.svc_app AS svc_app,
                nodes.app AS node_app,
                nodes.loc_addr,
                nodes.loc_city,
                nodes.loc_zip,
                nodes.loc_room,
                nodes.loc_building,
                nodes.loc_floor,
                nodes.loc_rack,
                nodes.power_cabinet1,
                nodes.power_cabinet2,
                nodes.power_supply_nb,
                nodes.power_protect,
                nodes.power_protect_breaker,
                nodes.power_breaker1,
                nodes.power_breaker2,
                nodes.loc_country,
                nodes.enclosure
            FROM svcmon
            JOIN services ON svcmon.svc_id = services.svc_id
            JOIN nodes ON svcmon.node_id = nodes.node_id
            WHERE svcmon.svc_id = ? AND svcmon.node_id = ?
        ) AS source
        SET
            n.loc_addr = COALESCE(NULLIF(source.loc_addr, ''), n.loc_addr),
            n.loc_city = COALESCE(NULLIF(source.loc_city, ''), n.loc_city),
            n.loc_zip = COALESCE(NULLIF(source.loc_zip, ''), n.loc_zip),
            n.loc_room = COALESCE(NULLIF(source.loc_room, ''), n.loc_room),
            n.loc_building = COALESCE(NULLIF(source.loc_building, ''), n.loc_building),
            n.loc_floor = COALESCE(NULLIF(source.loc_floor, ''), n.loc_floor),
            n.loc_rack = COALESCE(NULLIF(source.loc_rack, ''), n.loc_rack),
            n.power_cabinet1 = COALESCE(NULLIF(source.power_cabinet1, ''), n.power_cabinet1),
            n.power_cabinet2 = COALESCE(NULLIF(source.power_cabinet2, ''), n.power_cabinet2),
            n.power_supply_nb = COALESCE(NULLIF(source.power_supply_nb, ''), n.power_supply_nb),
            n.power_protect = COALESCE(NULLIF(source.power_protect, ''), n.power_protect),
            n.power_protect_breaker = COALESCE(NULLIF(source.power_protect_breaker, ''), n.power_protect_breaker),
            n.power_breaker1 = COALESCE(NULLIF(source.power_breaker1, ''), n.power_breaker1),
            n.power_breaker2 = COALESCE(NULLIF(source.power_breaker2, ''), n.power_breaker2),
            n.loc_country = COALESCE(NULLIF(source.loc_country, ''), n.loc_country),
            n.enclosure = COALESCE(NULLIF(source.enclosure, ''), n.enclosure)
        WHERE
            n.nodename = source.vmname AND
            n.app IN (source.svc_app, source.node_app)`
	result, err := oDb.DB.ExecContext(ctx, request, svcID, nodeID)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("nodes")
	}
	return nil
}
