package serverhandlers

import "github.com/opensvc/oc3/server"

type nodeBodyFields struct {
	Nodename            *string
	TeamResponsible     *string
	App                 *string
	ClusterId           *string
	WarrantyEnd         *string
	MaintenanceEnd      *string
	Status              *string
	Role                *string
	ListenerPort        *int
	Version             *string
	Collector           *string
	ConnectTo           *string
	Tz                  *string
	AssetEnv            *string
	Type                *string
	Fqdn                *string
	Manufacturer        *string
	LocAddr             *string
	LocCity             *string
	LocZip              *string
	LocRack             *string
	LocFloor            *string
	LocCountry          *string
	LocBuilding         *string
	LocRoom             *string
	PowerSupplyNb       *int
	PowerCabinet1       *string
	PowerCabinet2       *string
	PowerProtect        *string
	PowerProtectBreaker *string
	PowerBreaker1       *string
	PowerBreaker2       *string
	Enclosure           *string
	Enclosureslot       *string
	Assetname           *string
	SecZone             *string
	ActionType          *string
	Hvpool              *string
	Hvvdc               *string
	Hv                  *string
	HwObsWarnDate       *string
	HwObsAlertDate      *string
	OsObsWarnDate       *string
	OsObsAlertDate      *string
	Notifications       *bool
	SnoozeTill          *string
	NodeFrozen          *bool
	NodeFrozenAt        *string
	BiosVersion         *string
	CpuCores            *int
	CpuDies             *int
	CpuFreq             *string
	CpuModel            *string
	CpuThreads          *int
	CpuVendor           *string
	LastBoot            *string
	LastComm            *string
	MemBanks            *int
	MemBytes            *int
	MemSlots            *int
	Model               *string
	NodeEnv             *string
	OsArch              *string
	OsConcat            *string
	OsKernel            *string
	OsName              *string
	OsRelease           *string
	OsVendor            *string
	Serial              *string
	SpVersion           *string
	TeamInteg           *string
	TeamSupport         *string
	Updated             *string
}

func (f nodeBodyFields) toFields() map[string]any {
	m := map[string]any{}
	setStr := func(key string, v *string) {
		if v != nil {
			m[key] = *v
		}
	}
	setInt := func(key string, v *int) {
		if v != nil {
			m[key] = *v
		}
	}
	setBool := func(key string, v *bool) {
		if v != nil {
			m[key] = *v
		}
	}
	setStr("nodename", f.Nodename)
	setStr("team_responsible", f.TeamResponsible)
	setStr("app", f.App)
	setStr("cluster_id", f.ClusterId)
	setStr("warranty_end", f.WarrantyEnd)
	setStr("maintenance_end", f.MaintenanceEnd)
	setStr("status", f.Status)
	setStr("role", f.Role)
	setInt("listener_port", f.ListenerPort)
	setStr("version", f.Version)
	setStr("collector", f.Collector)
	setStr("connect_to", f.ConnectTo)
	setStr("tz", f.Tz)
	setStr("asset_env", f.AssetEnv)
	setStr("type", f.Type)
	setStr("fqdn", f.Fqdn)
	setStr("manufacturer", f.Manufacturer)
	setStr("loc_addr", f.LocAddr)
	setStr("loc_city", f.LocCity)
	setStr("loc_zip", f.LocZip)
	setStr("loc_rack", f.LocRack)
	setStr("loc_floor", f.LocFloor)
	setStr("loc_country", f.LocCountry)
	setStr("loc_building", f.LocBuilding)
	setStr("loc_room", f.LocRoom)
	setInt("power_supply_nb", f.PowerSupplyNb)
	setStr("power_cabinet1", f.PowerCabinet1)
	setStr("power_cabinet2", f.PowerCabinet2)
	setStr("power_protect", f.PowerProtect)
	setStr("power_protect_breaker", f.PowerProtectBreaker)
	setStr("power_breaker1", f.PowerBreaker1)
	setStr("power_breaker2", f.PowerBreaker2)
	setStr("enclosure", f.Enclosure)
	setStr("enclosureslot", f.Enclosureslot)
	setStr("assetname", f.Assetname)
	setStr("sec_zone", f.SecZone)
	setStr("action_type", f.ActionType)
	setStr("hvpool", f.Hvpool)
	setStr("hvvdc", f.Hvvdc)
	setStr("hv", f.Hv)
	setStr("hw_obs_warn_date", f.HwObsWarnDate)
	setStr("hw_obs_alert_date", f.HwObsAlertDate)
	setStr("os_obs_warn_date", f.OsObsWarnDate)
	setStr("os_obs_alert_date", f.OsObsAlertDate)
	setBool("notifications", f.Notifications)
	setStr("snooze_till", f.SnoozeTill)
	setBool("node_frozen", f.NodeFrozen)
	setStr("node_frozen_at", f.NodeFrozenAt)
	setStr("bios_version", f.BiosVersion)
	setInt("cpu_cores", f.CpuCores)
	setInt("cpu_dies", f.CpuDies)
	setStr("cpu_freq", f.CpuFreq)
	setStr("cpu_model", f.CpuModel)
	setInt("cpu_threads", f.CpuThreads)
	setStr("cpu_vendor", f.CpuVendor)
	setStr("last_boot", f.LastBoot)
	setStr("last_comm", f.LastComm)
	setInt("mem_banks", f.MemBanks)
	setInt("mem_bytes", f.MemBytes)
	setInt("mem_slots", f.MemSlots)
	setStr("model", f.Model)
	setStr("node_env", f.NodeEnv)
	setStr("os_arch", f.OsArch)
	setStr("os_concat", f.OsConcat)
	setStr("os_kernel", f.OsKernel)
	setStr("os_name", f.OsName)
	setStr("os_release", f.OsRelease)
	setStr("os_vendor", f.OsVendor)
	setStr("serial", f.Serial)
	setStr("sp_version", f.SpVersion)
	setStr("team_integ", f.TeamInteg)
	setStr("team_support", f.TeamSupport)
	setStr("updated", f.Updated)
	return m
}

func nodeBodyFieldsFromPostNodes(b server.PostNodesJSONRequestBody) nodeBodyFields {
	var actionType *string
	if b.ActionType != nil {
		s := string(*b.ActionType)
		actionType = &s
	}
	return nodeBodyFields{
		Nodename: b.Nodename, TeamResponsible: b.TeamResponsible, App: b.App,
		ClusterId: b.ClusterId, WarrantyEnd: b.WarrantyEnd, MaintenanceEnd: b.MaintenanceEnd,
		Status: b.Status, Role: b.Role, ListenerPort: b.ListenerPort, Version: b.Version,
		Collector: b.Collector, ConnectTo: b.ConnectTo, Tz: b.Tz, AssetEnv: b.AssetEnv,
		Type: b.Type, Fqdn: b.Fqdn, Manufacturer: b.Manufacturer, LocAddr: b.LocAddr,
		LocCity: b.LocCity, LocZip: b.LocZip, LocRack: b.LocRack, LocFloor: b.LocFloor,
		LocCountry: b.LocCountry, LocBuilding: b.LocBuilding, LocRoom: b.LocRoom,
		PowerSupplyNb: b.PowerSupplyNb, PowerCabinet1: b.PowerCabinet1, PowerCabinet2: b.PowerCabinet2,
		PowerProtect: b.PowerProtect, PowerProtectBreaker: b.PowerProtectBreaker,
		PowerBreaker1: b.PowerBreaker1, PowerBreaker2: b.PowerBreaker2,
		Enclosure: b.Enclosure, Enclosureslot: b.Enclosureslot, Assetname: b.Assetname,
		SecZone: b.SecZone, ActionType: actionType, Hvpool: b.Hvpool, Hvvdc: b.Hvvdc, Hv: b.Hv,
		HwObsWarnDate: b.HwObsWarnDate, HwObsAlertDate: b.HwObsAlertDate,
		OsObsWarnDate: b.OsObsWarnDate, OsObsAlertDate: b.OsObsAlertDate,
		Notifications: b.Notifications, SnoozeTill: b.SnoozeTill,
		NodeFrozen: b.NodeFrozen, NodeFrozenAt: b.NodeFrozenAt,
		BiosVersion: b.BiosVersion, CpuCores: b.CpuCores, CpuDies: b.CpuDies,
		CpuFreq: b.CpuFreq, CpuModel: b.CpuModel, CpuThreads: b.CpuThreads, CpuVendor: b.CpuVendor,
		LastBoot: b.LastBoot, LastComm: b.LastComm,
		MemBanks: b.MemBanks, MemBytes: b.MemBytes, MemSlots: b.MemSlots,
		Model: b.Model, NodeEnv: b.NodeEnv,
		OsArch: b.OsArch, OsConcat: b.OsConcat, OsKernel: b.OsKernel, OsName: b.OsName,
		OsRelease: b.OsRelease, OsVendor: b.OsVendor,
		Serial: b.Serial, SpVersion: b.SpVersion,
		TeamInteg: b.TeamInteg, TeamSupport: b.TeamSupport, Updated: b.Updated,
	}
}

func nodeBodyFieldsFromPostNode(b server.PostNodeJSONRequestBody) nodeBodyFields {
	var actionType *string
	if b.ActionType != nil {
		s := string(*b.ActionType)
		actionType = &s
	}
	return nodeBodyFields{
		Nodename: b.Nodename, TeamResponsible: b.TeamResponsible, App: b.App,
		ClusterId: b.ClusterId, WarrantyEnd: b.WarrantyEnd, MaintenanceEnd: b.MaintenanceEnd,
		Status: b.Status, Role: b.Role, ListenerPort: b.ListenerPort, Version: b.Version,
		Collector: b.Collector, ConnectTo: b.ConnectTo, Tz: b.Tz, AssetEnv: b.AssetEnv,
		Type: b.Type, Fqdn: b.Fqdn, Manufacturer: b.Manufacturer, LocAddr: b.LocAddr,
		LocCity: b.LocCity, LocZip: b.LocZip, LocRack: b.LocRack, LocFloor: b.LocFloor,
		LocCountry: b.LocCountry, LocBuilding: b.LocBuilding, LocRoom: b.LocRoom,
		PowerSupplyNb: b.PowerSupplyNb, PowerCabinet1: b.PowerCabinet1, PowerCabinet2: b.PowerCabinet2,
		PowerProtect: b.PowerProtect, PowerProtectBreaker: b.PowerProtectBreaker,
		PowerBreaker1: b.PowerBreaker1, PowerBreaker2: b.PowerBreaker2,
		Enclosure: b.Enclosure, Enclosureslot: b.Enclosureslot, Assetname: b.Assetname,
		SecZone: b.SecZone, ActionType: actionType, Hvpool: b.Hvpool, Hvvdc: b.Hvvdc, Hv: b.Hv,
		HwObsWarnDate: b.HwObsWarnDate, HwObsAlertDate: b.HwObsAlertDate,
		OsObsWarnDate: b.OsObsWarnDate, OsObsAlertDate: b.OsObsAlertDate,
		Notifications: b.Notifications, SnoozeTill: b.SnoozeTill,
		NodeFrozen: b.NodeFrozen, NodeFrozenAt: b.NodeFrozenAt,
		BiosVersion: b.BiosVersion, CpuCores: b.CpuCores, CpuDies: b.CpuDies,
		CpuFreq: b.CpuFreq, CpuModel: b.CpuModel, CpuThreads: b.CpuThreads, CpuVendor: b.CpuVendor,
		LastBoot: b.LastBoot, LastComm: b.LastComm,
		MemBanks: b.MemBanks, MemBytes: b.MemBytes, MemSlots: b.MemSlots,
		Model: b.Model, NodeEnv: b.NodeEnv,
		OsArch: b.OsArch, OsConcat: b.OsConcat, OsKernel: b.OsKernel, OsName: b.OsName,
		OsRelease: b.OsRelease, OsVendor: b.OsVendor,
		Serial: b.Serial, SpVersion: b.SpVersion,
		TeamInteg: b.TeamInteg, TeamSupport: b.TeamSupport, Updated: b.Updated,
	}
}
