namespace Orleans.Lattice;

/// <summary>
/// Centralized Orleans serialization alias constants for all grain state
/// and serializable types. Each alias is a short, fixed string
/// that provides a stable wire-format identity independent of CLR type names.
/// Every constant must use the <c>ol.</c> prefix, be at most 6 characters,
/// and be unique — these invariants are enforced by <c>TypeAliasesTests</c>.
/// </summary>
internal static class TypeAliases
{
    // Primitives
    internal const string HybridLogicalClock = "ol.hlc";
    internal const string LwwValue = "ol.lwv";
    internal const string SplitState = "ol.sps";
    internal const string StateDelta = "ol.sd";
    internal const string VersionVector = "ol.vv";

    // BPlusTree State
    internal const string ChildEntry = "ol.ce";
    internal const string InternalNodeState = "ol.ins";
    internal const string LeafNodeState = "ol.lns";
    internal const string ShardRootState = "ol.srs";
    internal const string PendingBulkGraft = "ol.pbg";
    internal const string GraftEntry = "ol.ge";
    internal const string TombstoneCompactionState = "ol.tcs";
    internal const string TreeDeletionState = "ol.tds";
    internal const string TreeResizeState = "ol.trs";
    internal const string ResizePhase = "ol.rp";
    internal const string TreeRegistryEntry = "ol.tre";
    internal const string TreeSnapshotState = "ol.tss";
    internal const string SnapshotPhase = "ol.snp";
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

    // Adaptive shard splitting (F-011)
    internal const string TreeShardSplitState = "ol.sss";
    internal const string ShardSplitInProgress = "ol.sip";
    internal const string ShardSplitPhase = "ol.sph";
    internal const string StaleShardRouting = "ol.ssr";

    // Range delete (FX-011)
    internal const string RangeDeleteResult = "ol.rdr";

    // Atomic multi-key writes (F-031)
    internal const string AtomicWriteState = "ol.aws";
    internal const string AtomicWritePhase = "ol.awp";
    internal const string AtomicPreValue = "ol.apv";

    // Stateful cursor / iterator (F-033)
    internal const string LatticeCursorKind = "ol.lck";
    internal const string LatticeCursorSpec = "ol.lcu";
    internal const string LatticeCursorPhase = "ol.lcp";
    internal const string LatticeCursorState = "ol.lcs";
    internal const string LatticeCursorKeysPage = "ol.lkp";
    internal const string LatticeCursorEntriesPage = "ol.lep";
    internal const string LatticeCursorDeleteProgress = "ol.ldp";

    // Grain interfaces — each must be applied via [Alias(...)] on the
    // interface declaration so the Orleans manifest carries a stable,
    // short wire-format identity independent of CLR type names.
    internal const string ILattice = "ol.gl";
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
}
