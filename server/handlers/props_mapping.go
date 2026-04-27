package serverhandlers

import (
	"fmt"

	"github.com/opensvc/oc3/schema"
)

type propDef struct {
	Col     *schema.Col
	SQLExpr string
	Kind    string
}

func (p propDef) selectExpr() string {
	if p.SQLExpr != "" {
		return p.SQLExpr
	}
	if p.Col != nil {
		return p.Col.Qualified()
	}
	return ""
}

func col(c *schema.Col) propDef {
	return propDef{Col: c}
}

func colStr(c *schema.Col) propDef {
	return propDef{Col: c, SQLExpr: fmt.Sprintf("COALESCE(%s, '')", c.Qualified()), Kind: "string"}
}

func colInt(c *schema.Col) propDef {
	return propDef{Col: c, SQLExpr: fmt.Sprintf("COALESCE(%s, 0)", c.Qualified()), Kind: "int64"}
}

type JoinDef struct {
	MappingKey string
}

type propMapping struct {
	Available []string
	// Default is the subset of Available returned when no props are requested.
	// If nil, all Available props are returned by default.
	Default   []string
	Blacklist map[string]struct{}
	// Props maps prop names to their propDef (column reference + optional SQL override).
	// Enables column pushdown: only requested columns are fetched from DB.
	Props map[string]propDef
	// Joins declares joinable tables. A prop "table.column" is valid when "table"
	// is a key in Joins and "column" is listed in JoinDef.Columns.
	Joins map[string]JoinDef
}

var propsMapping = map[string]propMapping{
	"node": {
		Available: []string{
			"node_id", "nodename", "app", "node_env", "cluster_id",
			"loc_country", "loc_city", "loc_addr", "loc_building", "loc_floor", "loc_room", "loc_rack", "loc_zip",
			"cpu_freq", "cpu_cores", "cpu_dies", "cpu_vendor", "cpu_model", "cpu_threads",
			"mem_banks", "mem_slots", "mem_bytes",
			"os_name", "os_release", "os_update", "os_segment", "os_arch", "os_vendor", "os_kernel", "os_concat",
			"team_responsible", "team_integ", "team_support",
			"serial", "model", "manufacturer", "type", "assetname", "asset_env",
			"warranty_end", "maintenance_end",
			"status", "role", "sec_zone",
			"power_cabinet1", "power_cabinet2", "power_supply_nb", "power_protect", "power_protect_breaker", "power_breaker1", "power_breaker2",
			"blade_cabinet", "enclosure", "enclosureslot",
			"hv", "hvpool", "hvvdc",
			"fqdn", "connect_to", "listener_port", "version", "collector", "sp_version", "bios_version",
			"tz", "last_boot", "last_comm",
			"node_frozen", "node_frozen_at",
			"snooze_till", "notifications", "action_type",
			"hw_obs_warn_date", "hw_obs_alert_date", "os_obs_warn_date", "os_obs_alert_date",
			"updated",
		},
		Props: map[string]propDef{
			"node_id":               colStr(schema.NodesNodeID),
			"nodename":              colStr(schema.NodesNodename),
			"app":                   colStr(schema.NodesApp),
			"node_env":              colStr(schema.NodesNodeEnv),
			"cluster_id":            colStr(schema.NodesClusterID),
			"loc_country":           colStr(schema.NodesLocCountry),
			"loc_city":              colStr(schema.NodesLocCity),
			"loc_addr":              colStr(schema.NodesLocAddr),
			"loc_building":          colStr(schema.NodesLocBuilding),
			"loc_floor":             colStr(schema.NodesLocFloor),
			"loc_room":              colStr(schema.NodesLocRoom),
			"loc_rack":              colStr(schema.NodesLocRack),
			"loc_zip":               colStr(schema.NodesLocZip),
			"cpu_freq":              colStr(schema.NodesCPUFreq),
			"cpu_cores":             colInt(schema.NodesCPUCores),
			"cpu_dies":              colInt(schema.NodesCPUDies),
			"cpu_vendor":            colStr(schema.NodesCPUVendor),
			"cpu_model":             colStr(schema.NodesCPUModel),
			"cpu_threads":           colInt(schema.NodesCPUThreads),
			"mem_banks":             colInt(schema.NodesMEMBanks),
			"mem_slots":             colInt(schema.NodesMEMSlots),
			"mem_bytes":             colInt(schema.NodesMEMBytes),
			"os_name":               colStr(schema.NodesOSName),
			"os_release":            colStr(schema.NodesOSRelease),
			"os_update":             colStr(schema.NodesOSUpdate),
			"os_segment":            colStr(schema.NodesOSSegment),
			"os_arch":               colStr(schema.NodesOSArch),
			"os_vendor":             colStr(schema.NodesOSVendor),
			"os_kernel":             colStr(schema.NodesOSKernel),
			"os_concat":             colStr(schema.NodesOSConcat),
			"team_responsible":      colStr(schema.NodesTeamResponsible),
			"team_integ":            colStr(schema.NodesTeamInteg),
			"team_support":          colStr(schema.NodesTeamSupport),
			"serial":                colStr(schema.NodesSerial),
			"model":                 colStr(schema.NodesModel),
			"manufacturer":          colStr(schema.NodesManufacturer),
			"type":                  colStr(schema.NodesType),
			"assetname":             colStr(schema.NodesAssetname),
			"asset_env":             colStr(schema.NodesAssetEnv),
			"warranty_end":          colStr(schema.NodesWarrantyEnd),
			"maintenance_end":       colStr(schema.NodesMaintenanceEnd),
			"status":                colStr(schema.NodesStatus),
			"role":                  colStr(schema.NodesRole),
			"sec_zone":              colStr(schema.NodesSecZone),
			"power_cabinet1":        colStr(schema.NodesPowerCabinet1),
			"power_cabinet2":        colStr(schema.NodesPowerCabinet2),
			"power_supply_nb":       colInt(schema.NodesPowerSupplyNb),
			"power_protect":         colStr(schema.NodesPowerProtect),
			"power_protect_breaker": colStr(schema.NodesPowerProtectBreaker),
			"power_breaker1":        colStr(schema.NodesPowerBreaker1),
			"power_breaker2":        colStr(schema.NodesPowerBreaker2),
			"blade_cabinet":         colStr(schema.NodesBladeCabinet),
			"enclosure":             colStr(schema.NodesEnclosure),
			"enclosureslot":         colStr(schema.NodesEnclosureslot),
			"hv":                    colStr(schema.NodesHv),
			"hvpool":                colStr(schema.NodesHvpool),
			"hvvdc":                 colStr(schema.NodesHvvdc),
			"fqdn":                  colStr(schema.NodesFqdn),
			"connect_to":            colStr(schema.NodesConnectTo),
			"listener_port":         colInt(schema.NodesListenerPort),
			"version":               colStr(schema.NodesVersion),
			"collector":             colStr(schema.NodesCollector),
			"sp_version":            colStr(schema.NodesSpVersion),
			"bios_version":          colStr(schema.NodesBiosVersion),
			"tz":                    colStr(schema.NodesTz),
			"last_boot":             colStr(schema.NodesLastBoot),
			"last_comm":             colStr(schema.NodesLastComm),
			"node_frozen":           colStr(schema.NodesNodeFrozen),
			"node_frozen_at":        colStr(schema.NodesNodeFrozenAt),
			"snooze_till":           colStr(schema.NodesSnoozeTill),
			"notifications":         colStr(schema.NodesNotifications),
			"action_type":           colStr(schema.NodesActionType),
			"hw_obs_warn_date":      colStr(schema.NodesHWObsWarnDate),
			"hw_obs_alert_date":     colStr(schema.NodesHWObsAlertDate),
			"os_obs_warn_date":      colStr(schema.NodesOSObsWarnDate),
			"os_obs_alert_date":     colStr(schema.NodesOSObsAlertDate),
			"updated":               colStr(schema.NodesUpdated),
		},
	},
	"disk": {
		Available: []string{
			"disk_id", "disk_name", "disk_devid", "disk_vendor", "disk_model",
			"disk_size", "disk_used", "disk_alloc", "disk_raid", "disk_group",
			"disk_level", "disk_arrayid", "disk_dg", "disk_region",
			"node_id", "nodename", "svc_id", "svcname", "app", "updated",
		},
		Props: map[string]propDef{
			"disk_id":      col(schema.DiskinfoDiskID),
			"disk_name":    colStr(schema.DiskinfoDiskName),
			"disk_devid":   colStr(schema.DiskinfoDiskDevid),
			"disk_vendor":  colStr(schema.SvcdisksDiskVendor),
			"disk_model":   colStr(schema.SvcdisksDiskModel),
			"disk_size":    colInt(schema.DiskinfoDiskSize),
			"disk_used":    colInt(schema.SvcdisksDiskUsed),
			"disk_alloc":   colInt(schema.DiskinfoDiskAlloc),
			"disk_raid":    colStr(schema.DiskinfoDiskRaid),
			"disk_group":   colStr(schema.DiskinfoDiskGroup),
			"disk_level":   colInt(schema.DiskinfoDiskLevel),
			"disk_arrayid": colStr(schema.DiskinfoDiskArrayid),
			"disk_dg":      colStr(schema.SvcdisksDiskDG),
			"disk_region":  colStr(schema.SvcdisksDiskRegion),
			"node_id":      colStr(schema.SvcdisksNodeID),
			"nodename":     colStr(schema.NodesNodename),
			"svc_id":       colStr(schema.SvcdisksSvcID),
			"svcname":      colStr(schema.ServicesSvcname),
			"app":          colStr(schema.AppsApp),
			"updated":      colStr(schema.DiskinfoDiskUpdated),
		},
	},
	"node_ip": {
		Available: []string{
			"id", "node_id", "nodename", "intf", "mac", "type", "addr", "mask", "updated", "flag_deprecated",
			"net_id", "net_name", "net_network", "net_netmask", "net_broadcast", "net_gateway",
			"net_begin", "net_end", "net_pvid", "net_prio", "net_comment", "net_team_responsible",
		},
		Default: []string{"node_id", "intf", "addr", "mask", "net_name", "net_network", "net_netmask", "net_gateway"},
		Props: map[string]propDef{
			"id":                   {SQLExpr: "id", Kind: "int64"},
			"node_id":              {SQLExpr: "COALESCE(node_id, '') AS node_id", Kind: "string"},
			"nodename":             {SQLExpr: "COALESCE(nodename, '') AS nodename", Kind: "string"},
			"intf":                 {SQLExpr: "COALESCE(intf, '') AS intf", Kind: "string"},
			"mac":                  {SQLExpr: "COALESCE(mac, '') AS mac", Kind: "string"},
			"type":                 {SQLExpr: "COALESCE(addr_type, '') AS type", Kind: "string"},
			"addr":                 {SQLExpr: "COALESCE(addr, '') AS addr", Kind: "string"},
			"mask":                 {SQLExpr: "COALESCE(mask, '') AS mask", Kind: "string"},
			"updated":              {SQLExpr: "COALESCE(addr_updated, '') AS updated", Kind: "string"},
			"flag_deprecated":      {SQLExpr: "COALESCE(flag_deprecated, 0) AS flag_deprecated", Kind: "int64"},
			"net_id":               {SQLExpr: "net_id", Kind: "int64"},
			"net_name":             {SQLExpr: "COALESCE(net_name, '') AS net_name", Kind: "string"},
			"net_network":          {SQLExpr: "COALESCE(net_network, '') AS net_network", Kind: "string"},
			"net_netmask":          {SQLExpr: "COALESCE(net_netmask, '') AS net_netmask", Kind: "string"},
			"net_broadcast":        {SQLExpr: "COALESCE(net_broadcast, '') AS net_broadcast", Kind: "string"},
			"net_gateway":          {SQLExpr: "COALESCE(net_gateway, '') AS net_gateway", Kind: "string"},
			"net_begin":            {SQLExpr: "COALESCE(net_begin, '') AS net_begin", Kind: "string"},
			"net_end":              {SQLExpr: "COALESCE(net_end, '') AS net_end", Kind: "string"},
			"net_pvid":             {SQLExpr: "COALESCE(net_pvid, '') AS net_pvid", Kind: "string"},
			"net_prio":             {SQLExpr: "COALESCE(net_prio, 0) AS net_prio", Kind: "int64"},
			"net_comment":          {SQLExpr: "COALESCE(net_comment, '') AS net_comment", Kind: "string"},
			"net_team_responsible": {SQLExpr: "COALESCE(net_team_responsible, '') AS net_team_responsible", Kind: "string"},
		},
	},
	"node_interface": {
		Available: []string{"id", "node_id", "intf", "mac", "type", "addr", "mask", "updated", "flag_deprecated"},
		Default:   []string{"id", "node_id", "intf", "mac", "updated", "flag_deprecated"},
		Blacklist: map[string]struct{}{"type": {}, "addr": {}, "mask": {}},
		Props: map[string]propDef{
			"id":              col(schema.NodeIPID),
			"node_id":         colStr(schema.NodeIPNodeID),
			"intf":            colStr(schema.NodeIPIntf),
			"mac":             colStr(schema.NodeIPMac),
			"type":            colStr(schema.NodeIPType),
			"addr":            colStr(schema.NodeIPAddr),
			"mask":            colStr(schema.NodeIPMask),
			"updated":         colStr(schema.NodeIPUpdated),
			"flag_deprecated": colInt(schema.NodeIPFlagDeprecated),
		},
		Joins: map[string]JoinDef{
			"nodes": {
				MappingKey: "node",
			},
		},
	},
	"node_hw": {
		Available: []string{"id", "node_id", "hw_type", "hw_path", "hw_class", "hw_description", "hw_driver", "updated"},
		Default:   []string{"node_id", "hw_type", "hw_path", "hw_class", "hw_description", "hw_driver", "updated"},
		Props: map[string]propDef{
			"id":             col(schema.NodeHWID),
			"node_id":        colStr(schema.NodeHWNodeID),
			"hw_type":        colStr(schema.NodeHWHWType),
			"hw_path":        colStr(schema.NodeHWHWPath),
			"hw_class":       colStr(schema.NodeHWHWClass),
			"hw_description": colStr(schema.NodeHWHWDescription),
			"hw_driver":      colStr(schema.NodeHWHWDriver),
			"updated":        colStr(schema.NodeHWUpdated),
		},
	},
	"hba": {
		Available: []string{"id", "node_id", "hba_id", "hba_type", "updated"},
		Props: map[string]propDef{
			"id":       col(schema.NodeHBAID),
			"node_id":  colStr(schema.NodeHBANodeID),
			"hba_id":   colStr(schema.NodeHBAHBAID),
			"hba_type": colStr(schema.NodeHBAHBAType),
			"updated":  colStr(schema.NodeHBAUpdated),
		},
	},
	"array": {
		Available: []string{
			"id", "array_name", "array_comment", "array_model",
			"array_firmware", "array_cache", "array_updated", "array_level",
		},
		Props: map[string]propDef{
			"id":             col(schema.StorArrayID),
			"array_name":     colStr(schema.StorArrayArrayName),
			"array_comment":  colStr(schema.StorArrayArrayComment),
			"array_model":    colStr(schema.StorArrayArrayModel),
			"array_firmware": colStr(schema.StorArrayArrayFirmware),
			"array_cache":    colInt(schema.StorArrayArrayCache),
			"array_updated":  colStr(schema.StorArrayArrayUpdated),
			"array_level":    colInt(schema.StorArrayArrayLevel),
		},
	},
	"app": {
		Available: []string{"id", "app", "updated", "app_domain", "app_team_ops", "description"},
		Props: map[string]propDef{
			"id":           col(schema.AppsID),
			"app":          col(schema.AppsApp),
			"updated":      colStr(schema.AppsUpdated),
			"app_domain":   colStr(schema.AppsAppDomain),
			"app_team_ops": colStr(schema.AppsAppTeamOps),
			"description":  colStr(schema.AppsDescription),
		},
	},
	"auth_group": {
		Available: []string{"id", "role", "privilege", "description"},
	},
	"tag": {
		Available: []string{"id", "tag_name", "tag_created", "tag_exclude", "tag_data", "tag_id"},
		Blacklist: map[string]struct{}{"id": {}},
		Props: map[string]propDef{
			"id":          col(schema.TagsID),
			"tag_name":    colStr(schema.TagsTagName),
			"tag_created": colStr(schema.TagsTagCreated),
			"tag_exclude": colStr(schema.TagsTagExclude),
			"tag_data":    col(schema.TagsTagData),
			"tag_id":      colStr(schema.TagsTagID),
		},
	},
	"node_tag": {
		Available: []string{"id", "created", "node_id", "tag_id", "tag_attach_data"},
		Blacklist: map[string]struct{}{"id": {}},
		Props: map[string]propDef{
			"id":              col(schema.NodeTagsID),
			"created":         colStr(schema.NodeTagsCreated),
			"node_id":         colStr(schema.NodeTagsNodeID),
			"tag_id":          colStr(schema.NodeTagsTagID),
			"tag_attach_data": colStr(schema.NodeTagsTagAttachData),
		},
	},
	"svc_tag": {
		Available: []string{"id", "created", "svc_id", "tag_id", "tag_attach_data"},
		Blacklist: map[string]struct{}{"id": {}},
		Props: map[string]propDef{
			"id":              col(schema.SvcTagsID),
			"created":         colStr(schema.SvcTagsCreated),
			"svc_id":          colStr(schema.SvcTagsSvcID),
			"tag_id":          colStr(schema.SvcTagsTagID),
			"tag_attach_data": colStr(schema.SvcTagsTagAttachData),
		},
	},
	"service": {
		Available: []string{
			"svc_id", "svcname", "cluster_id",
			"svc_status", "svc_availstatus",
			"svc_app", "svc_env", "svc_ha",
			"svc_topology", "svc_frozen", "svc_placement", "svc_provisioned",
			"svc_flex_min_nodes", "svc_flex_max_nodes",
			"svc_flex_cpu_low_threshold", "svc_flex_cpu_high_threshold",
			"svc_autostart",
			"svc_nodes", "svc_drpnode", "svc_drpnodes",
			"svc_comment", "svc_created", "svc_status_updated",
			"svc_notifications", "svc_snooze_till",
			"updated",
		},
		Default: []string{"svc_id", "svcname", "cluster_id", "svc_status", "svc_availstatus", "svc_app", "svc_env", "updated"},
		Props: map[string]propDef{
			"svc_id":                      colStr(schema.ServicesSvcID),
			"svcname":                     colStr(schema.ServicesSvcname),
			"cluster_id":                  colStr(schema.ServicesClusterID),
			"svc_status":                  colStr(schema.ServicesSvcStatus),
			"svc_availstatus":             colStr(schema.ServicesSvcAvailstatus),
			"svc_app":                     colStr(schema.ServicesSvcApp),
			"svc_env":                     colStr(schema.ServicesSvcEnv),
			"svc_ha":                      colStr(schema.ServicesSvcHa),
			"svc_topology":                colStr(schema.ServicesSvcTopology),
			"svc_frozen":                  colStr(schema.ServicesSvcFrozen),
			"svc_placement":               colStr(schema.ServicesSvcPlacement),
			"svc_provisioned":             colStr(schema.ServicesSvcProvisioned),
			"svc_flex_min_nodes":          colInt(schema.ServicesSvcFlexMinNodes),
			"svc_flex_max_nodes":          colInt(schema.ServicesSvcFlexMaxNodes),
			"svc_flex_cpu_low_threshold":  colInt(schema.ServicesSvcFlexCPULowThreshold),
			"svc_flex_cpu_high_threshold": colInt(schema.ServicesSvcFlexCPUHighThreshold),
			"svc_autostart":               colStr(schema.ServicesSvcAutostart),
			"svc_nodes":                   colStr(schema.ServicesSvcNodes),
			"svc_drpnode":                 colStr(schema.ServicesSvcDrpnode),
			"svc_drpnodes":                colStr(schema.ServicesSvcDrpnodes),
			"svc_comment":                 colStr(schema.ServicesSvcComment),
			"svc_created":                 colStr(schema.ServicesSvcCreated),
			"svc_status_updated":          colStr(schema.ServicesSvcStatusUpdated),
			"svc_notifications":           colStr(schema.ServicesSvcNotifications),
			"svc_snooze_till":             colStr(schema.ServicesSvcSnoozeTill),
			"updated":                     colStr(schema.ServicesUpdated),
		},
	},
	"instance": {
		Available: []string{
			"svc_id", "node_id",
			"mon_svctype",
			"mon_availstatus", "mon_overallstatus",
			"mon_smon_status", "mon_smon_global_expect",
			"mon_ipstatus", "mon_fsstatus", "mon_diskstatus", "mon_containerstatus",
			"mon_sharestatus", "mon_syncstatus", "mon_appstatus", "mon_hbstatus",
			"mon_frozen", "mon_frozen_at", "mon_encap_frozen_at",
			"mon_vmname", "mon_vmtype", "mon_guestos", "mon_vcpus", "mon_vmem",
			"mon_updated", "mon_changed",
		},
		Default: []string{
			"svc_id", "node_id",
			"mon_availstatus", "mon_overallstatus", "mon_smon_status",
			"mon_frozen", "mon_updated",
		},
		Props: map[string]propDef{
			"svc_id":                 colStr(schema.SvcmonSvcID),
			"node_id":                colStr(schema.SvcmonNodeID),
			"mon_svctype":            colStr(schema.SvcmonMonSvctype),
			"mon_availstatus":        colStr(schema.SvcmonMonAvailstatus),
			"mon_overallstatus":      colStr(schema.SvcmonMonOverallstatus),
			"mon_smon_status":        colStr(schema.SvcmonMonSmonStatus),
			"mon_smon_global_expect": colStr(schema.SvcmonMonSmonGlobalExpect),
			"mon_ipstatus":           colStr(schema.SvcmonMonIpstatus),
			"mon_fsstatus":           colStr(schema.SvcmonMonFsstatus),
			"mon_diskstatus":         colStr(schema.SvcmonMonDiskstatus),
			"mon_containerstatus":    colStr(schema.SvcmonMonContainerstatus),
			"mon_sharestatus":        colStr(schema.SvcmonMonSharestatus),
			"mon_syncstatus":         colStr(schema.SvcmonMonSyncstatus),
			"mon_appstatus":          colStr(schema.SvcmonMonAppstatus),
			"mon_hbstatus":           colStr(schema.SvcmonMonHbstatus),
			"mon_frozen":             colStr(schema.SvcmonMonFrozen),
			"mon_frozen_at":          colStr(schema.SvcmonMonFrozenAt),
			"mon_encap_frozen_at":    colStr(schema.SvcmonMonEncapFrozenAt),
			"mon_vmname":             colStr(schema.SvcmonMonVmname),
			"mon_vmtype":             colStr(schema.SvcmonMonVmtype),
			"mon_guestos":            colStr(schema.SvcmonMonGuestos),
			"mon_vcpus":              colInt(schema.SvcmonMonVcpus),
			"mon_vmem":               colInt(schema.SvcmonMonVmem),
			"mon_updated":            colStr(schema.SvcmonMonUpdated),
			"mon_changed":            colStr(schema.SvcmonMonChanged),
		},
	},
	"instance_status_log": {
		Available: []string{
			"id",
			"svc_id", "node_id",
			"mon_begin", "mon_end",
			"mon_availstatus", "mon_overallstatus",
			"mon_ipstatus", "mon_fsstatus", "mon_diskstatus",
			"mon_sharestatus", "mon_containerstatus",
			"mon_syncstatus", "mon_hbstatus", "mon_appstatus",
		},
		Default: []string{
			"svc_id", "node_id",
			"mon_begin", "mon_end",
			"mon_availstatus", "mon_overallstatus",
		},
		Props: map[string]propDef{
			"id":                  col(schema.SvcmonLogID),
			"svc_id":              colStr(schema.SvcmonLogSvcID),
			"node_id":             colStr(schema.SvcmonLogNodeID),
			"mon_begin":           colStr(schema.SvcmonLogMonBegin),
			"mon_end":             colStr(schema.SvcmonLogMonEnd),
			"mon_availstatus":     colStr(schema.SvcmonLogMonAvailstatus),
			"mon_overallstatus":   colStr(schema.SvcmonLogMonOverallstatus),
			"mon_ipstatus":        colStr(schema.SvcmonLogMonIpstatus),
			"mon_fsstatus":        colStr(schema.SvcmonLogMonFsstatus),
			"mon_diskstatus":      colStr(schema.SvcmonLogMonDiskstatus),
			"mon_sharestatus":     colStr(schema.SvcmonLogMonSharestatus),
			"mon_containerstatus": colStr(schema.SvcmonLogMonContainerstatus),
			"mon_syncstatus":      colStr(schema.SvcmonLogMonSyncstatus),
			"mon_hbstatus":        colStr(schema.SvcmonLogMonHbstatus),
			"mon_appstatus":       colStr(schema.SvcmonLogMonAppstatus),
		},
	},
	"moduleset": {
		Available: []string{"id", "modset_name", "modset_author", "modset_updated"},
	},
	"ruleset": {
		Available: []string{"id", "ruleset_name", "ruleset_type", "ruleset_public"},
	},
}
