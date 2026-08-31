namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One sweep's observation of a tree's shape and load, as measured by the
/// healing orchestrator and handed to <see cref="ShardHealingDecisionCore"/>.
/// <para>
/// A value type carrying only primitives, so a sweep's decision can be
/// reproduced exactly under test from the numbers alone - no silo, no grains,
/// no clock.
/// </para>
/// </summary>
internal readonly record struct ShardHealingSample
{
    /// <summary>Physical shards the tree's routing map currently references.</summary>
    public int PhysicalShardCount { get; init; }

    /// <summary>
    /// The tree's configured base physical shard count (its registry-pinned
    /// <c>ShardCount</c>). Zero or less means unknown, which reports the tree
    /// not over-split rather than guessing.
    /// </summary>
    public int BaseShardCount { get; init; }

    /// <summary>
    /// The tree's load-skew ratio for this sweep, computed with
    /// <see cref="ShardSplitAdmissionCore.ComputeMedianRate"/> and
    /// <see cref="ShardSplitAdmissionCore.ComputeSkewRatio"/> - the same two
    /// functions the splitter uses, so the two loops cannot disagree about
    /// what the tree's shape is.
    /// </summary>
    public double SkewRatio { get; init; }

    /// <summary>
    /// The tree's median shard rate in operations per second. The median
    /// rather than the sum, because a sum scales with the shard count and
    /// would make the badly over-split tree - the one that most needs healing
    /// - look busiest.
    /// </summary>
    public double MedianShardOpsPerSecond { get; init; }

    /// <summary>Folds already in flight against this tree.</summary>
    public int InFlightConsolidations { get; init; }

    /// <summary>Whether any physical shard is the source of an unfinished adaptive split.</summary>
    public bool IsSplitting { get; init; }

    /// <summary>
    /// Whether a resize, reshard, merge, snapshot, or pending bulk graft is in
    /// flight on the tree.
    /// </summary>
    public bool InTreeMaintenance { get; init; }

    /// <summary>Whether the tree is inside its post-split anti-oscillation window.</summary>
    public bool InCooldown { get; init; }
}
