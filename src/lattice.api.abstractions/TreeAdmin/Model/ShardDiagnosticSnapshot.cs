namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A single physical shard's diagnostic snapshot, carried as one row of a
/// <see cref="TreeAdminDiagnosticReport"/>. Combines structural facts (tree depth, root
/// shape, live/tombstone counts) with volatile activity counters and in-flight
/// maintenance flags.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.ShardDiagnosticSnapshot)]
[Immutable]
public sealed record ShardDiagnosticSnapshot
{
    /// <summary>Zero-based physical shard index.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>The depth of the shard's B+ tree (1 when the root is a leaf).</summary>
    [Id(1)] public int Depth { get; init; }

    /// <summary><see langword="true"/> when the shard root is a single leaf (no inner nodes).</summary>
    [Id(2)] public bool RootIsLeaf { get; init; }

    /// <summary>Count of live (non-tombstoned) keys in the shard.</summary>
    [Id(3)] public long LiveKeys { get; init; }

    /// <summary>Count of tombstoned keys awaiting compaction in the shard.</summary>
    [Id(4)] public long Tombstones { get; init; }

    /// <summary>
    /// Ratio of tombstones to total entries in the shard, in <c>[0, 1]</c>. A high
    /// ratio signals compaction pressure.
    /// </summary>
    [Id(5)] public double TombstoneRatio { get; init; }

    /// <summary>Observed operations-per-second for the shard over its hotness window.</summary>
    [Id(6)] public double OpsPerSecond { get; init; }

    /// <summary>Read operations processed since the shard grain activated.</summary>
    [Id(7)] public long Reads { get; init; }

    /// <summary>Write operations processed since the shard grain activated.</summary>
    [Id(8)] public long Writes { get; init; }

    /// <summary>Wall-clock seconds over which the activity counters accumulated.</summary>
    [Id(9)] public double WindowSeconds { get; init; }

    /// <summary><see langword="true"/> when a shard split is in progress.</summary>
    [Id(10)] public bool SplitInProgress { get; init; }

    /// <summary><see langword="true"/> when a bulk operation is pending on the shard.</summary>
    [Id(11)] public bool BulkOperationPending { get; init; }
}
