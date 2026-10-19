package serverhandlers

import (
	"maps"
	"slices"

	"github.com/opensvc/oc3/schema"
)

// The lists of the compliance attachments, one row per node or service and
// moduleset or ruleset: the props of the node or of the service, as their own
// lists name them, and those of the object attached. The object props are NULL on
// the row of a target that has none, so that a filter on "empty" finds them.
func init() {
	propsMapping["compModulesetNode"] = compAttachmentMapping(propsMapping["node"], schema.NodesID,
		schema.CompNodeModulesetID, "modset_id", schema.CompNodeModulesetModsetID,
		"modset_name", schema.CompModulesetModsetName, nil)
	propsMapping["compRulesetNode"] = compAttachmentMapping(propsMapping["node"], schema.NodesID,
		schema.CompRulesetsNodesID, "ruleset_id", schema.CompRulesetsNodesRulesetID,
		"ruleset_name", schema.CompRulesetsRulesetName, nil)
	propsMapping["compModulesetService"] = compAttachmentMapping(propsMapping["service"], schema.ServicesID,
		schema.CompModulesetsServicesID, "modset_id", schema.CompModulesetsServicesModsetID,
		"modset_name", schema.CompModulesetModsetName, schema.CompModulesetsServicesSlave)
	propsMapping["compRulesetService"] = compAttachmentMapping(propsMapping["service"], schema.ServicesID,
		schema.CompRulesetsServicesID, "ruleset_id", schema.CompRulesetsServicesRulesetID,
		"ruleset_name", schema.CompRulesetsRulesetName, schema.CompRulesetsServicesSlave)
}

// compAttachmentMapping is the mapping of a target, plus the id and the name of the
// object attached and, for a service, whether the attachment is to its
// encapsulated part (slave, "T" or "F"). The default props name the target and
// the object.
func compAttachmentMapping(target propMapping, targetID, attachID *schema.Col, idProp string, idCol *schema.Col,
	nameProp string, nameCol *schema.Col, slaveCol *schema.Col) propMapping {
	m := propMapping{
		Available: slices.Clone(target.Available),
		Props:     maps.Clone(target.Props),
		Joins:     maps.Clone(target.Joins),
		Blacklist: maps.Clone(target.Blacklist),
		// A target row and its attachment: one order, whichever sort is asked.
		PrimaryKey: []*schema.Col{targetID, attachID},
	}
	if targetID == schema.NodesID {
		m.Default = []string{"node_id", "nodename", "app"}
	} else {
		m.Default = []string{"svc_id", "svcname", "svc_app"}
	}
	m.Available = append(m.Available, idProp, nameProp)
	m.Props[idProp] = col(idCol)
	m.Props[nameProp] = col(nameCol)
	m.Default = append(m.Default, idProp, nameProp)
	if slaveCol != nil {
		m.Available = append(m.Available, "slave")
		m.Props["slave"] = col(slaveCol)
		m.Default = append(m.Default, "slave")
	}
	return m
}
