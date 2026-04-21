namespace Orleans.Lattice;

/// <summary>
/// Per-shard health snapshot returned as part of <see cref="TreeDiagnosticReport.Shards"/>.
/// Values are a point-in-time sample; counters (<see cref="Reads"/>, <see cref="Writes"/>)
/// are volatile and reset on shard-grain deactivation.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardDiagnosticReport)]
[Immutable]
public readonly record struct ShardDiagnosticReport
{
    /// <summary>Zero-based physical shard index.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>
    /// B+ tree depth for this shard — <c>1</c> when the root is a leaf,
    /// <c>2</c> with one internal level, and so on.
    /// </summary>
    [Id(1)] public int Depth { get; init; }

    /// <summary>Whether the shard''s root node is currently a leaf (<c>true</c>) or an internal node.</summary>
    [Id(2)] public bool RootIsLeaf { get; init; }

    /// <summary>Number of live (non-tombstoned, non-expired) keys owned by this shard.</summary>
    [Id(3)] public long LiveKeys { get; init; }

    /// <summary>
    /// Number of tombstoned or expired entries still held by this shard''s leaves.
    /// Only populated when <see cref="ILattice.DiagnoseAsync"/> is called with
    /// <c>deep: true</c>; otherwise <c>0</c>.
    /// </summary>
    [Id(4)] public long Tombstones { get; init; }

    /// <summary>
    /// Tombstones divided by <c>(LiveKeys + Tombstones)</c>. Returns <c>0</c> when
    /// the shard is empty or when <see cref="Tombstones"/> was not computed.
    /// </summary>
    [Id(5)] public double TombstoneRatio { get; init; }

    /// <summary>
    /// Observed operations-per-second for this shard, computed as
    /// <c>(Reads + Writes) / HotnessWindow.TotalSeconds</c>.
    /// </summary>
    [Id(6)] public double OpsPerSecond { get; init; }

    /// <summary>Volatile read-operation count since shard activation.</summary>
    [Id(7)] public long Reads { get; init; }

    /// <summary>Volatile write-operation count since shard activation.</summary>
    [Id(8)] public long Writes { get; init; }

    /// <summary>Wall-clock duration over which <see cref="Reads"/> and <see cref="Writes"/> accumulated.</summary>
    [Id(9)] public TimeSpan HotnessWindow { get; init; }

    /// <summary>Whether the shard is currently participating in an adaptive split as the source.</summary>
    [Id(10)] public bool SplitInProgress { get; init; }

    /// <summary>Whether a bulk-load graft is pending on this shard.</summary>
    [Id(11)] public bool BulkOperationPending { get; init; }
}
