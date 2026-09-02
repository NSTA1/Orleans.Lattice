namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The resolved thresholds one over-split healing sweep decides against,
/// snapshotted from <see cref="LatticeOptions"/> so the decision core stays a
/// pure function of its inputs and can be driven directly under test.
/// </summary>
internal readonly record struct ShardHealingPolicy
{
    /// <summary>
    /// Whether automatic healing is switched on. Mirrors
    /// <see cref="LatticeOptions.ShardHealingEnabled"/>.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// The load-skew ratio at or below which the tree counts as uniformly
    /// loaded and consolidating cannot recreate a hot spot. Mirrors
    /// <see cref="LatticeOptions.HotShardConsolidationSkewRatio"/> - the same
    /// knob the splitter's admission policy carries, so the two loops share
    /// one number rather than agreeing by coincidence.
    /// </summary>
    public double ConsolidationSkewRatio { get; init; }

    /// <summary>
    /// How many folds may be in flight against this tree at once. Mirrors
    /// <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/>; zero
    /// admits nothing.
    /// </summary>
    public int MaxConcurrentConsolidations { get; init; }

    /// <summary>
    /// The tree's median shard rate at or above which healing yields to
    /// foreground traffic, or zero to heal regardless of load. Mirrors
    /// <see cref="LatticeOptions.ShardHealingBackpressureOpsPerSecond"/>.
    /// </summary>
    public double BackpressureOpsPerSecond { get; init; }

    /// <summary>
    /// Projects the healing thresholds out of a resolved
    /// <see cref="LatticeOptions"/>.
    /// </summary>
    /// <param name="options">The tree's resolved options.</param>
    public static ShardHealingPolicy FromOptions(LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new ShardHealingPolicy
        {
            Enabled = options.ShardHealingEnabled,
            ConsolidationSkewRatio = options.HotShardConsolidationSkewRatio,
            MaxConcurrentConsolidations = options.MaxConcurrentShardConsolidations,
            BackpressureOpsPerSecond = options.ShardHealingBackpressureOpsPerSecond,
        };
    }
}
