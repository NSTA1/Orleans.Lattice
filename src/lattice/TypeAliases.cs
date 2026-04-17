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

    // BPlusTree
    internal const string SplitResult = "ol.sr";
    internal const string KeysPage = "ol.kp";
    internal const string GetOrSetResult = "ol.gsr";
    internal const string EntriesPage = "ol.ep";
}
