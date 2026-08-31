namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Immutable snapshot of the split-admission thresholds, resolved once per
/// monitor pass from <see cref="LatticeOptions"/> so the decision core never
/// touches the options monitor and stays a pure function of its arguments.
/// </summary>
/// <remarks>
/// This is a plain value type with no Orleans serialization attributes: it never
/// crosses a grain boundary, it is passed by <see langword="in"/> reference into
/// <see cref="ShardSplitAdmissionCore"/>, and it is the shared vocabulary the
/// split trigger and the consolidation trigger both reason against.
/// </remarks>
internal readonly record struct ShardSplitAdmissionPolicy
{
    /// <summary>
    /// Operations per second at or above which a shard counts as hot. Mirrors
    /// <see cref="LatticeOptions.HotShardOpsPerSecondThreshold"/>.
    /// </summary>
    public int OpsPerSecondThreshold { get; init; }

    /// <summary>
    /// Ratio of the hottest shard's rate to the tree's median shard rate at or
    /// above which the tree's load counts as skewed. Mirrors
    /// <see cref="LatticeOptions.HotShardMinSkewRatio"/>. A value at or below
    /// <c>1.0</c> disables the skew gate.
    /// </summary>
    public double MinSkewRatio { get; init; }

    /// <summary>
    /// Ratio at or below which the tree's load counts as uniform, and the tree
    /// is therefore a consolidation candidate rather than a split candidate.
    /// Mirrors <see cref="LatticeOptions.HotShardConsolidationSkewRatio"/>.
    /// Strictly less than <see cref="MinSkewRatio"/>; the interval between the
    /// two is the hysteresis dead band.
    /// </summary>
    public double ConsolidationSkewRatio { get; init; }

    /// <summary>
    /// Minimum live-entry count a shard must hold to be worth splitting.
    /// Mirrors <see cref="LatticeOptions.HotShardMinShardEntries"/>. Zero
    /// disables the occupancy floor and its per-candidate probe.
    /// </summary>
    public int MinShardEntries { get; init; }

    /// <summary>
    /// Absolute ceiling on the tree's physical shard count for autonomic
    /// growth. Mirrors <see cref="LatticeOptions.MaxPhysicalShardsPerTree"/>.
    /// Zero or less means no ceiling.
    /// </summary>
    public int MaxPhysicalShards { get; init; }

    /// <summary>
    /// Projects the split-admission knobs out of a resolved
    /// <see cref="LatticeOptions"/> snapshot. Allocation-free: the result is a
    /// value type built on the caller's stack.
    /// </summary>
    /// <param name="options">The per-tree options snapshot to read.</param>
    /// <returns>The policy the decision core evaluates against.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public static ShardSplitAdmissionPolicy FromOptions(LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new ShardSplitAdmissionPolicy
        {
            OpsPerSecondThreshold = options.HotShardOpsPerSecondThreshold,
            MinSkewRatio = options.HotShardMinSkewRatio,
            ConsolidationSkewRatio = options.HotShardConsolidationSkewRatio,
            MinShardEntries = options.HotShardMinShardEntries,
            MaxPhysicalShards = options.MaxPhysicalShardsPerTree,
        };
    }
}
