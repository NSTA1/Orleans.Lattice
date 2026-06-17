namespace Orleans.Lattice;

/// <summary>
/// Centralized Orleans serialization alias constants for all grain state
/// and serializable types. Each alias is a short, fixed string
/// that provides a stable wire-format identity independent of CLR type names.
/// Every constant must use the <c>ol.</c> prefix, be at most 6 characters,
/// and be unique - these invariants are enforced by <c>TypeAliasesTests</c>.
/// </summary>
internal static class TypeAliases
{
    // Primitives
    internal const string HybridLogicalClock = "ol.hlc";
    internal const string LwwValue = "ol.lwv";
    internal const string SplitState = "ol.sps";
    internal const string StateDelta = "ol.sd";
    internal const string VersionVector = "ol.vv";
    internal const string OrSet = "ol.ors";
    internal const string OrSetDot = "ol.osd";
    internal const string PnCounter = "ol.pnc";
    internal const string MvRegister = "ol.mvr";
    internal const string MvRegisterDot = "ol.mvd";
    internal const string OrMap = "ol.orm";
    internal const string OrMapEntry = "ol.ome";
    internal const string Rga = "ol.rga";
    internal const string RgaNode = "ol.rgn";
    internal const string RgaDelta = "ol.rgd";
    internal const string RgaDeltaNode = "ol.rgi";
    internal const string OrFlag = "ol.orf";
    internal const string RwFlag = "ol.rwf";
    internal const string LeafDeliveryCursor = "ol.ldc";

    // Opt-in idempotency-key surface (carried on the ambient
    // RequestContext via LatticeIdempotencyContext so retries of the
    // same logical operation collapse through the existing WAL HWM /
    // LWW / PnCounter dedup paths).
    internal const string LatticeIdempotencyKey = "ol.idk";

    // BPlusTree State
    internal const string ChildEntry = "ol.ce";
    internal const string InternalNodeState = "ol.ins";
    internal const string LeafNodeState = "ol.lns";
    internal const string LeafSnapshotBlob = "ol.lsb";
    internal const string LeafSnapshotRow = "ol.lsr";
    internal const string ShardRootState = "ol.srs";
    internal const string DirtyLeavesSnapshot = "ol.dls";
    internal const string PendingBulkGraft = "ol.pbg";
    internal const string GraftEntry = "ol.ge";
    internal const string TombstoneCompactionState = "ol.tcs";
    internal const string TreeDeletionState = "ol.tds";
    internal const string TreeResizeState = "ol.trs";
    internal const string ResizePhase = "ol.rp";
    internal const string TreeRegistryEntry = "ol.tre";
    internal const string TreeSnapshotState = "ol.tss";
    internal const string SnapshotPhase = "ol.snp";
    internal const string RoutingTableSnapshot = "ol.rts";
    internal const string SnapshotMode = "ol.snm";
    internal const string TreeMergeState = "ol.tms";
    internal const string HotShardMonitorState = "ol.hms";

    // BPlusTree
    internal const string SplitResult = "ol.sr";
    internal const string KeysPage = "ol.kp";
    internal const string GetOrSetResult = "ol.gsr";
    internal const string EntriesPage = "ol.ep";
    internal const string CasResult = "ol.cas";
    internal const string CrdtApplyResult = "ol.cap";
    internal const string Versioned = "ol.ver";
    internal const string VersionedValue = "ol.vvl";
    internal const string ShardHotness = "ol.sh";
    internal const string ShardMap = "ol.sm";
    internal const string RoutingInfo = "ol.ri";
    internal const string ShardCountResult = "ol.scr";

    // Adaptive shard splitting
    internal const string TreeShardSplitState = "ol.sss";
    internal const string ShardSplitInProgress = "ol.sip";
    internal const string ShardSplitPhase = "ol.sph";
    internal const string StaleShardRouting = "ol.ssr";
    internal const string ShardActivationTimeout = "ol.sat";

    // Lattice host / silo shutdown back-pressure surface. Thrown by
    // any public ILattice operator that cannot complete because the
    // owning silo's WalCommitLogWriter is draining (host shutdown).
    internal const string LatticeShuttingDown = "ol.lsd";

    // Lattice steady-state saturation back-pressure surface. Thrown by
    // the WAL writer admission gate when the per-tree saturation signal
    // reports Saturated for longer than WalAdmissionSaturationWaitBudget
    // and by the atomic-write saga when the same regime persists past
    // the saga's quiesce budget. Distinct from LatticeShuttingDown:
    // saturation is a transient regime the operator can recover from by
    // reducing offered load or scaling storage; shutdown is a one-way
    // transition for that silo activation.
    internal const string LatticeSaturated = "ol.lsa";

    // Online reshard
    internal const string TreeReshardState = "ol.txs";
    internal const string ReshardPhase = "ol.rxp";

    // Online resize - shadow-forwarding primitive
    internal const string ShadowForwardState = "ol.sfs";
    internal const string ShadowForwardPhase = "ol.sfp";
    internal const string StaleTreeRouting = "ol.str";

    // Range delete
    internal const string RangeDeleteResult = "ol.rdr";

    // Conditional bulk write (guard predicate against existing value)
    internal const string ConditionalSetManyResult = "ol.csm";

    // Server-side predicate push-down IR (the allowlisted, wire-stable
    // lowering of a client Expression<Func<T,bool>> evaluated against a
    // value's JSON document view inside the leaf scan).
    internal const string LatticePredicateNode = "ol.pn";
    internal const string LatticePredicateNodeKind = "ol.pnk";
    internal const string LatticeConstant = "ol.pc";
    internal const string LatticeConstantKind = "ol.pck";
    internal const string LatticeComparisonOperator = "ol.pco";
    internal const string LatticeBooleanOperator = "ol.pbo";
    internal const string LatticeStringMethod = "ol.psm";

    // Raw-entry bulk load (snapshot TTL preservation)
    internal const string LwwEntry = "ol.lwe";

    // Retroactive shadow-forward of in-flight prepared mutations.
    // Per-(txid, key) snapshot of a leaf's pending-tx map filtered by
    // migrating virtual slot, replayed into the destination shard at the
    // start of a shard split's BeginShadowWrite phase.
    internal const string PendingMutationSnapshot = "ol.pms";

    // Atomic multi-key writes
    internal const string AtomicWriteState = "ol.aws";
    internal const string AtomicWritePhase = "ol.awp";
    internal const string AtomicPreValue = "ol.apv";
    internal const string AtomicWriteOutcome = "ol.awo";
    internal const string TxRegistryState = "ol.txr";
    internal const string TxStatus = "ol.txo";
    internal const string TerminalTallyResult = "ol.ttr";
    internal const string SnapshotPin = "ol.sp";
    internal const string TxRegistrySnapshot = "ol.tsn";

    // Cross-tree (multi-tree) atomic writes
    internal const string LatticeTreeBatch = "ol.ltb";
    internal const string CrossTreeAtomicWriteOutcome = "ol.cto";
    internal const string CrossTreeTxState = "ol.cts";
    internal const string CrossTreeTxPhase = "ol.ctp";
    internal const string CrossTreeParticipant = "ol.ctc";
    internal const string CrossTreePrepareVote = "ol.ctv";
    internal const string ILatticeCrossTreeTxGrain = "ol.gct";
    // Receiver-side cross-tree visibility barrier.
    internal const string ILatticeCrossTreeReceiverGrain = "ol.gcr";
    internal const string CrossTreeReceiverState = "ol.crs";
    internal const string CrossTreeReceiverTerminal = "ol.crt";
    internal const string CrossTreeReceiverDecision = "ol.crd";
    internal const string CrossTreeReceiverTreeFinalize = "ol.crf";
    // Ambient producer-side cross-tree terminal metadata (RequestContext value).
    internal const string CrossTreeTerminalInfo = "ol.cti";

    // Stateful cursor / iterator
    internal const string LatticeCursorKind = "ol.lck";
    internal const string LatticeCursorSpec = "ol.lcu";
    internal const string LatticeCursorPhase = "ol.lcp";
    internal const string LatticeCursorState = "ol.lcs";
    internal const string LatticeCursorKeysPage = "ol.lkp";
    internal const string LatticeCursorEntriesPage = "ol.lep";
    internal const string LatticeCursorDeleteProgress = "ol.ldp";

    // Zero-observable-writes snapshot cursor: coordinate captured at
    // OpenSnapshot*Async time and persisted on the cursor state;
    // transient per-shard snapshot leaf grain that materialises the
    // projection by replaying the WAL prefix.
    internal const string LatticeSnapshotCoordinate = "ol.lsc";
    internal const string ISnapshotLeafGrain = "ol.slg";

    // Diagnostics
    internal const string TreeDiagnosticReport = "ol.tdr";
    internal const string ShardDiagnosticReport = "ol.sdr";
    internal const string RecentSplit = "ol.rsp";
    internal const string LeafStats = "ol.lst";

    // Per-shard byte-accurate storage usage rollup (leaf-state + snapshot
    // bytes summed across a shard's leaf chain in a single walk).
    internal const string ShardStorageUsage = "ol.ssu";

    // Per-leaf byte footprint published by leaves to the shard root and
    // folded into ShardRootState.LeafStateBytesTotal / SnapshotBytesTotal.
    internal const string LeafByteFootprint = "ol.lbf";

    // Storage-usage accounting (byte-accurate retained footprint)
    internal const string TreeStorageUsageReport = "ol.tsu";
    internal const string ClusterStorageUsageReport = "ol.csu";
    internal const string TreeWalUsageReport = "ol.twu";

    // Event stream
    internal const string LatticeTreeEvent = "ol.lte";
    internal const string LatticeTreeEventKind = "ol.ltk";

    // Mutation observer hook
    internal const string LatticeMutation = "ol.mut";
    internal const string MutationKind = "ol.muk";
    internal const string MutationCategory = "ol.muc";
    internal const string LatticeDeltaCarry = "ol.dlc";

    // Typed CRDT delta DTOs - the public on-wire author-delta contract
    // for replicable CRDT primitives. The producer-side accessors emit
    // these directly into WalRecord.Delta; receivers deserialise the
    // matching DTO based on LatticeMergeMode and call MergeDelta on
    // the loaded primitive. Single source of truth for CRDT replication
    // (previously: dual-path with internal CrdtDeltaPayloads + full-state
    // JSON in WalRecord.Value).
    internal const string LwwRegisterDelta = "ol.lwd";
    internal const string OrSetDelta = "ol.osa";
    internal const string OrSetDeltaDot = "ol.osc";
    internal const string PnCounterDelta = "ol.pcd";
    internal const string VersionVectorDelta = "ol.vvd";
    internal const string MvRegisterDelta = "ol.mvg";
    internal const string OrMapDelta = "ol.omd";
    internal const string OrMapDeltaEntry = "ol.omx";
    internal const string OrMapDeltaTombstone = "ol.omt";
    internal const string OrFlagDelta = "ol.ofd";
    internal const string RwFlagDelta = "ol.rwd";

    // Tag index (associate tags with keys and query by tag). The membership
    // rows live in an ordinary sibling Lattice tree resolved as
    // tag-{indexName}; these aliases cover the public value/report types the
    // tag-index surface returns.
    internal const string TaggedKey = "ol.tgk";
    internal const string TagReconcileReport = "ol.tgr";
    internal const string TagConsistency = "ol.tgc";

    // Tag-index background reconciliation coordinator: persisted cursor state
    // and its phase enum for the per-index digest-gated sweep coordinator.
    internal const string TagIndexReconcileState = "ol.tir";
    internal const string TagIndexReconcilePhase = "ol.tip";

    // Grain interfaces - each must be applied via [Alias(...)] on the
    // interface declaration so the Orleans manifest carries a stable,
    // short wire-format identity independent of CLR type names.
    internal const string ILattice = "ol.gl";
    internal const string ISystemLattice = "ol.gsl";
    internal const string ILatticeRegistry = "ol.glr";
    internal const string ILatticeCursorGrain = "ol.glc";
    internal const string IAtomicWriteGrain = "ol.gaw";
    internal const string IBPlusInternalGrain = "ol.gbi";
    internal const string IBPlusLeafGrain = "ol.gbl";
    internal const string IHotShardMonitorGrain = "ol.ghm";
    internal const string ILeafCacheGrain = "ol.glf";
    internal const string IShardRootGrain = "ol.gsh";
    internal const string ITombstoneCompactionGrain = "ol.gtc";
    internal const string ITreeDeletionGrain = "ol.gtd";
    internal const string ITreeMergeGrain = "ol.gtm";
    internal const string ITreeResizeGrain = "ol.gtr";
    internal const string ITreeShardSplitGrain = "ol.gss";
    internal const string ITreeSnapshotGrain = "ol.gsn";
    internal const string ITreeReshardGrain = "ol.gtx";
    internal const string ITagIndexReconcileGrain = "ol.gti";
    internal const string ILatticeStats = "ol.gls";
    internal const string ILatticeStorageUsage = "ol.gsu";
    internal const string ILatticeWalUsage = "ol.gwu";
    internal const string ILatticeAdmin = "ol.gad";
    internal const string IReplicationApplyGrain = "ol.gra";
    internal const string ILeafReplayCoordinatorGrain = "ol.grc";
    internal const string ITxRegistryGrain = "ol.gxr";
    internal const string ILeafSnapshotStorageGrain = "ol.gsx";
    internal const string ILatticeQueueGrain = "ol.glq";

    // Cluster-internal FIFO queue (ILatticeQueue<T>) grain wire-return shape:
    // a single parked entry's monotonic id plus its opaque serialized payload.
    internal const string LatticeQueueByteEntry = "ol.qbe";

    // Replication apply DTO (batch path)
    internal const string ApplyMergeItem = "ol.ami";

    // Replication apply DTO (typed-CRDT delta batch path)
    internal const string ApplyCrdtDeltaItem = "ol.acd";

    // Leaf-projection replay coordinator slice DTO
    internal const string CommitLogSliceEntry = "ol.cls";

    // Leaf projection digest (cross-silo determinism check)
    internal const string LeafProjectionDigest = "ol.lpd";

    // Per-child digest snapshot folded into an internal node's
    // SubtreeProjectionHash (chained-internal-fold value type carried
    // by IBPlusInternalGrain.OnChildDigestPublishedAsync).
    internal const string ChildDigestSnapshot = "ol.cds";

    // Read-only shard-root node reference (root grain id + leaf flag)
    // surfaced by IShardRootGrain.GetRootNodeRefAsync for anti-entropy
    // drift-localisation traversal that descends the internal-node tree.
    internal const string ShardRootNodeRef = "ol.snr";

    // Batched leaf-split sibling-initialization payload. Collapses the
    // five separate gated metadata-setter RPCs the donor used to issue
    // against a freshly-created sibling (tree id, shard index, key
    // range, next/prev sibling pointers) into one
    // IBPlusLeafGrain.InitializeSiblingAsync round-trip.
    internal const string SiblingInitialization = "ol.sib";

    // Write-ahead-log durability seam (consumed by the replication
    // package today; foreground commit-log adapter tomorrow)
    internal const string WalEntry = "ol.we";

    // WAL saturation back-pressure surface (push + poll + await
    // shapes exposed to callers driving offered load into ILattice;
    // see IWalSaturationSignal / IWalSaturationObserver).
    internal const string WalSaturationState = "ol.wss";
    internal const string WalSaturationStateChange = "ol.wsc";

    // WAL grain & wire-record surface (ship-time envelope and per-shard
    // sequenced storage shape). Originally declared with the legacy
    // <c>olr.</c> prefix from the replication package; preserved
    // verbatim here after the WAL adapter move so the Orleans manifest
    // wire format stays compatible with rolling-upgrade peers that
    // were registered against the old assembly. The former
    // <c>WalOp</c> enum (alias <c>olr.ro</c>) was collapsed into the
    // core <see cref="Orleans.Lattice.MutationKind"/> enum during the
    // WAL-to-core move; that alias slot is therefore intentionally
    // retired and not reused.
    internal const string WalRecord = "olr.re";
    internal const string LatticeMergeMode = "olr.rm";
    internal const string IWalShardGrain = "olr.gw";
    internal const string WalShardSequencedEntry = "olr.we";
    internal const string WalShardPage = "olr.wp";
    internal const string WalShardShippingEntry = "olr.ws";
    internal const string WalShardShippingPage = "olr.wg";

    // Per-tree pinned WAL placement + ILatticeAdmin managed-move surface.
    // The pin is durable registry state; the move DTOs are the public
    // admin contract for auditing and relocating a partition's log between
    // storage backends registered in the IWalStorageProviderCatalog.
    internal const string WalPlacementPin = "ol.wpp";
    internal const string WalPlacement = "ol.wpl";
    internal const string WalPlacementAudit = "ol.wpa";
    internal const string WalPartitionPlacement = "ol.wpe";
    internal const string WalMovePlan = "ol.wmp";
    internal const string WalMoveReceipt = "ol.wmr";
    internal const string WalMoveBatchPlan = "ol.wbp";
    internal const string WalMoveBatchReceipt = "ol.wbr";
    internal const string WalMoveOptions = "ol.wmo";
    internal const string WalMoveOutcome = "ol.wmc";
    internal const string WalMoveQuiesceResult = "ol.wqr";
    internal const string LatticeWalProviderMissing = "ol.wpm";
    internal const string LatticeWalQuiescing = "ol.wqx";

    // Materialised views (Phase 1): the projected-write value type and its
    // effect-kind enum are the core serializable surface; the view
    // maintainer's durable checkpoint state lives in the replication package
    // (see ReplicationTypeAliases).
    internal const string ViewWrite = "ol.vw";
    internal const string ViewWriteKind = "ol.vwk";

    // Materialised views (Phase 3): the aggregation contribution value type and
    // its discriminator enums. Aggregation projections are services (never
    // serialized), but the contribution value carries serializer metadata for
    // forward compatibility and convention parity with ViewWrite.
    internal const string AggregationKind = "ol.agk";
    internal const string AggregationContribution = "ol.agc";
    internal const string AggregationContributionKind = "ol.ack";

    // Materialised views (Phase 5): the order-independent content fingerprint a
    // view's drift digest / ReconcileAsync compares between the live view and a
    // fresh source re-projection.
    internal const string ViewDigest = "ol.vdg";
    // Materialised views: the view maintainer grain interface and its durable
    // per-view checkpoint state; plus the view-side cross-tree coordinator grain
    // that rendezvouses every participating view's ready slice and flips them
    // jointly, its durable decision state, and the per-call readiness / decision
    // / slice DTOs. Values keep the "olr.v*" wire form they were introduced with.

    /// <summary>Alias for the view maintainer grain interface.</summary>
    internal const string IViewMaintainerGrain = "olr.vm";

    /// <summary>Alias for the view maintainer's durable checkpoint state.</summary>
    internal const string ViewCheckpointState = "olr.vc";

    /// <summary>Alias for the view-side cross-tree coordinator grain interface.</summary>
    internal const string IViewCrossTreeCoordinatorGrain = "olr.vx";

    /// <summary>Alias for the view-side cross-tree coordinator's durable decision state.</summary>
    internal const string ViewCrossTreeCoordinatorState = "olr.vs";

    /// <summary>Alias for one participating view's cross-tree readiness registration.</summary>
    internal const string ViewCrossTreeReadiness = "olr.vr";

    /// <summary>Alias for the cross-tree coordinator's per-registration decision.</summary>
    internal const string ViewCrossTreeDecision = "olr.vd";

    /// <summary>Alias for one participating view's recorded ready slice.</summary>
    internal const string ViewCrossTreeSlice = "olr.vl";
}
