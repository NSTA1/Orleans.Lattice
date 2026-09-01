namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Point-in-time report of a tree's automatic over-split healing, returned by
/// <see cref="IShardHealingOrchestratorGrain.GetHealingReportAsync"/>.
/// <para>
/// The programmatic companion to the two published instruments
/// (<c>orleans.lattice.shard.healing.backlog</c> and
/// <c>orleans.lattice.shard.healing.decisions</c>): the same facts, readable
/// without a metrics pipeline, which is what makes healing assertable from a
/// test and inspectable on a box whose exporter is not wired up.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardHealingReport)]
[Immutable]
internal readonly record struct ShardHealingReport
{
    /// <summary>The decision the most recent sweep reached.</summary>
    [Id(0)] public ShardHealingDecision Decision { get; init; }

    /// <summary>Physical shards the tree's routing map referenced at that sweep.</summary>
    [Id(1)] public int PhysicalShardCount { get; init; }

    /// <summary>The tree's configured base physical shard count.</summary>
    [Id(2)] public int BaseShardCount { get; init; }

    /// <summary>
    /// Physical shards above the base count - the healing work outstanding.
    /// Zero means the tree is healed.
    /// </summary>
    [Id(3)] public int Backlog { get; init; }

    /// <summary>The tree's load-skew ratio at that sweep.</summary>
    [Id(4)] public double SkewRatio { get; init; }

    /// <summary>The tree's median shard rate, in operations per second, at that sweep.</summary>
    [Id(5)] public double MedianShardOpsPerSecond { get; init; }

    /// <summary>Folds in flight against this tree at that sweep.</summary>
    [Id(6)] public int InFlightConsolidations { get; init; }

    /// <summary>UTC ticks at which the sweep observed the tree, or zero when none has.</summary>
    [Id(7)] public long ObservedAtTicks { get; init; }
}
