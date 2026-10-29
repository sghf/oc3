package schema

func init() {
	AppsPublicationsAppID.Ref = AppsID
	AppsPublicationsGroupID.Ref = AuthGroupID
	AppsResponsiblesAppID.Ref = AppsID
	AppsResponsiblesGroupID.Ref = AuthGroupID

	SvcdisksDiskID.Ref = DiskinfoDiskID
	SvcdisksNodeID.Ref = NodesNodeID
	SvcdisksSvcID.Ref = ServicesSvcID

	SavesNodeID.Ref = NodesNodeID
	SavesSvcID.Ref = ServicesSvcID

	ServicesSvcApp.Ref = AppsApp

	NodeIPNodeID.Ref = NodesNodeID

	NodesClusterID.Ref = ClustersClusterID

	ActionQueueNodeID.Ref = NodesNodeID
	ActionQueueSvcID.Ref = ServicesSvcID

	PackagesNodeID.Ref = NodesNodeID
	NodeHWNodeID.Ref = NodesNodeID
	PackagesPkgSig.Ref = PkgSigProviderSigID

	SvcmonSvcID.Ref = ServicesSvcID
	SvcmonNodeID.Ref = NodesNodeID
	ResmonSvcID.Ref = ServicesSvcID
	ResmonNodeID.Ref = NodesNodeID
	SvcactionsSvcID.Ref = ServicesSvcID
	SvcactionsNodeID.Ref = NodesNodeID
	SvcmonLogSvcID.Ref = ServicesSvcID
	ServicesLogSvcID.Ref = ServicesSvcID
}
