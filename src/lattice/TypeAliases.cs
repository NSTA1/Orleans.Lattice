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

    // Online reshard
    internal const string TreeReshardState = "ol.txs";
    internal const string ReshardPhase = "ol.rxp";

    // Online resize - shadow-forwarding primitive
    internal const string ShadowForwardState = "ol.sfs";
    internal const string ShadowForwardPhase = "ol.sfp";
    internal const string StaleTreeRouting = "ol.str";

    // Range delete
    internal const string RangeDeleteResult = "ol.rdr";

    // Raw-entry bulk load ( snapshot TTL preservation)
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
    internal const string TxRegistryState = "ol.txr";
    internal const string TxStatus = "ol.txo";
    internal const string TerminalTallyResult = "ol.ttr";
    internal const string SnapshotPin = "ol.sp";

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

    // Event stream
    internal const string LatticeTreeEvent = "ol.lte";
    internal const string LatticeTreeEventKind = "ol.ltk";

    // Mutation observer hook
    internal const string LatticeMutation = "ol.mut";
    internal const string MutationKind = "ol.muk";
    internal const string MutationCategory = "ol.muc";
    internal const string LatticeDeltaCarry = "ol.dlc";

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
    internal const string ILatticeStats = "ol.gls";
    internal const string IReplicationApplyGrain = "ol.gra";
    internal const string ILeafReplayCoordinatorGrain = "ol.grc";
    internal const string ITxRegistryGrain = "ol.gxr";

    // Replication apply DTO (batch path)
    internal const string ApplyMergeItem = "ol.ami";

    // Leaf-projection replay coordinator slice DTO
    internal const string CommitLogSliceEntry = "ol.cls";

    // Leaf projection digest (cross-silo determinism check)
    internal const string LeafProjectionDigest = "ol.lpd";

    // Per-child digest snapshot folded into an internal node's
    // SubtreeProjectionHash (chained-internal-fold value type carried
    // by IBPlusInternalGrain.OnChildDigestPublishedAsync).
    internal const string ChildDigestSnapshot = "ol.cds";

    // Write-ahead-log durability seam (consumed by the replication
    // package today; foreground commit-log adapter tomorrow)
    internal const string WalEntry = "ol.we";

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
}
