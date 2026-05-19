package serverhandlers

import "github.com/opensvc/oc3/server"

type serviceBodyFields struct {
	Svcname                *string
	SvcApp                 *string
	SvcEnv                 *string
	SvcComment             *string
	SvcNodes               *string
	SvcDrpnode             *string
	SvcDrpnodes            *string
	SvcAutostart           *string
	SvcDrptype             *string
	SvcDrnoaction          *string
	SvcMetrocluster        *string
	SvcWave                *int
	SvcTopology            *string
	SvcFlexMinNodes        *int
	SvcFlexMaxNodes        *int
	SvcFlexCpuLowThresh    *int
	SvcFlexCpuHighThresh   *int
	SvcHa                  *string
	SvcFrozen              *string
	SvcProvisioned         *string
	SvcPlacement           *string
	SvcNotifications       *bool
	SvcSnoozeTill          *string
}

func (f serviceBodyFields) toFields() map[string]any {
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
	setStr("svcname", f.Svcname)
	setStr("svc_app", f.SvcApp)
	setStr("svc_env", f.SvcEnv)
	setStr("svc_comment", f.SvcComment)
	setStr("svc_nodes", f.SvcNodes)
	setStr("svc_drpnode", f.SvcDrpnode)
	setStr("svc_drpnodes", f.SvcDrpnodes)
	setStr("svc_autostart", f.SvcAutostart)
	setStr("svc_drptype", f.SvcDrptype)
	setStr("svc_drnoaction", f.SvcDrnoaction)
	setStr("svc_metrocluster", f.SvcMetrocluster)
	setInt("svc_wave", f.SvcWave)
	setStr("svc_topology", f.SvcTopology)
	setInt("svc_flex_min_nodes", f.SvcFlexMinNodes)
	setInt("svc_flex_max_nodes", f.SvcFlexMaxNodes)
	setInt("svc_flex_cpu_low_threshold", f.SvcFlexCpuLowThresh)
	setInt("svc_flex_cpu_high_threshold", f.SvcFlexCpuHighThresh)
	setStr("svc_ha", f.SvcHa)
	setStr("svc_frozen", f.SvcFrozen)
	setStr("svc_provisioned", f.SvcProvisioned)
	setStr("svc_placement", f.SvcPlacement)
	setBool("svc_notifications", f.SvcNotifications)
	setStr("svc_snooze_till", f.SvcSnoozeTill)
	return m
}

func serviceBodyFieldsFromPostService(b server.PostServiceJSONRequestBody) serviceBodyFields {
	return serviceBodyFields{
		Svcname: b.Svcname, SvcApp: b.SvcApp, SvcEnv: b.SvcEnv, SvcComment: b.SvcComment,
		SvcNodes: b.SvcNodes, SvcDrpnode: b.SvcDrpnode, SvcDrpnodes: b.SvcDrpnodes,
		SvcAutostart: b.SvcAutostart, SvcDrptype: b.SvcDrptype, SvcDrnoaction: b.SvcDrnoaction,
		SvcMetrocluster: b.SvcMetrocluster, SvcWave: b.SvcWave, SvcTopology: b.SvcTopology,
		SvcFlexMinNodes: b.SvcFlexMinNodes, SvcFlexMaxNodes: b.SvcFlexMaxNodes,
		SvcFlexCpuLowThresh: b.SvcFlexCpuLowThreshold, SvcFlexCpuHighThresh: b.SvcFlexCpuHighThreshold,
		SvcHa: b.SvcHa, SvcFrozen: b.SvcFrozen, SvcProvisioned: b.SvcProvisioned,
		SvcPlacement: b.SvcPlacement, SvcNotifications: b.SvcNotifications, SvcSnoozeTill: b.SvcSnoozeTill,
	}
}

func serviceBodyFieldsFromPostServices(b server.PostServicesJSONRequestBody) serviceBodyFields {
	return serviceBodyFields{
		Svcname: b.Svcname, SvcApp: b.SvcApp, SvcEnv: b.SvcEnv, SvcComment: b.SvcComment,
		SvcNodes: b.SvcNodes, SvcDrpnode: b.SvcDrpnode, SvcDrpnodes: b.SvcDrpnodes,
		SvcAutostart: b.SvcAutostart, SvcDrptype: b.SvcDrptype, SvcDrnoaction: b.SvcDrnoaction,
		SvcMetrocluster: b.SvcMetrocluster, SvcWave: b.SvcWave, SvcTopology: b.SvcTopology,
		SvcFlexMinNodes: b.SvcFlexMinNodes, SvcFlexMaxNodes: b.SvcFlexMaxNodes,
		SvcFlexCpuLowThresh: b.SvcFlexCpuLowThreshold, SvcFlexCpuHighThresh: b.SvcFlexCpuHighThreshold,
		SvcHa: b.SvcHa, SvcFrozen: b.SvcFrozen, SvcProvisioned: b.SvcProvisioned,
		SvcPlacement: b.SvcPlacement, SvcNotifications: b.SvcNotifications, SvcSnoozeTill: b.SvcSnoozeTill,
	}
}
