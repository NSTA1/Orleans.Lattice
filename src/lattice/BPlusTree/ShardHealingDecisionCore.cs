namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Pure, allocation-free decision core for automatic over-split healing: the
/// exact predicate <c>ShardHealingOrchestratorGrain</c> applies once per
/// sweep, extracted so it can be driven directly under unit test - including
/// the oscillation case - without a silo, in the same spirit as
/// <see cref="ShardSplitAdmissionCore"/> and <see cref="WalGcTrimCore"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The hysteresis contract.</b> Adaptive splitting and automatic healing
/// are one control loop and would oscillate if their trigger regions
/// overlapped: a tree would be split, immediately consolidated, split again,
/// and never settle. The contract that prevents it has three parts, and this
/// type implements the third:
/// </para>
/// <list type="number">
///   <item>
///     Both loops reason against <b>one statistic</b> - the skew ratio
///     <c>maxShardRate / medianShardRate</c>, computed with
///     <see cref="ShardSplitAdmissionCore.ComputeMedianRate"/> and
///     <see cref="ShardSplitAdmissionCore.ComputeSkewRatio"/>. There is no
///     second, private notion of "over-split" anywhere.
///   </item>
///   <item>
///     Their regions are separated by a <b>dead band</b>. Splitting fires at
///     or above <see cref="LatticeOptions.HotShardMinSkewRatio"/>; healing
///     fires at or below <see cref="LatticeOptions.HotShardConsolidationSkewRatio"/>;
///     <see cref="ShardSplitAdmissionCore.AreTriggerRegionsDisjoint"/> asserts
///     they never meet, and <c>LatticeOptionsValidator</c> rejects any
///     configuration in which they could, so the separation is structural
///     rather than a convention two loops politely observe.
///   </item>
///   <item>
///     Healing requires <b>both</b> halves of its trigger:
///     <see cref="ShardSplitAdmissionCore.IsConsolidationSkew"/> (the load is
///     uniform, so folding cannot recreate a hot spot) <em>and</em>
///     <see cref="ShardSplitAdmissionCore.IsOverSplit"/> (the tree really does
///     carry more physical shards than its base). Shard count alone says
///     nothing about whether consolidating recreates a hot spot, so a
///     count-only trigger reintroduces exactly the oscillation the dead band
///     exists to remove.
///   </item>
/// </list>
/// <para>
/// <b>Clause order.</b> Every refusal below is independently blocking, so the
/// order does not change which sweeps admit - it changes only which reason is
/// reported. It is ordered cheapest-and-most-structural first, so a healthy
/// tree reports <see cref="ShardHealingDecision.NotOverSplit"/> rather than
/// some incidental condition, and the published decision series stays
/// interpretable.
/// </para>
/// </remarks>
internal static class ShardHealingDecisionCore
{
    /// <summary>
    /// Returns how many physical shards a tree carries above its configured
    /// base - the healing work outstanding, and the figure published as
    /// <c>orleans.lattice.shard.healing.backlog</c>.
    /// </summary>
    /// <param name="physicalShardCount">The tree's current physical shard count.</param>
    /// <param name="baseShardCount">
    /// The tree's configured base physical shard count. Zero or less means
    /// unknown, which reports a backlog of zero rather than guessing.
    /// </param>
    /// <returns>The excess shard count, never negative.</returns>
    public static int ComputeBacklog(int physicalShardCount, int baseShardCount)
    {
        if (!ShardSplitAdmissionCore.IsOverSplit(physicalShardCount, baseShardCount)) return 0;
        return physicalShardCount - baseShardCount;
    }

    /// <summary>
    /// Whether foreground traffic is heavy enough that healing should yield.
    /// </summary>
    /// <param name="medianShardOpsPerSecond">
    /// The tree's median shard rate. The median rather than the sum: a sum
    /// scales with the shard count, so it would report the thousand-shard
    /// tree that most needs healing as the busiest tree on the box even when
    /// every one of its shards is idle.
    /// </param>
    /// <param name="backpressureOpsPerSecond">
    /// The configured threshold. Zero or less disables backpressure, so
    /// healing proceeds regardless of load.
    /// </param>
    /// <returns><see langword="true"/> when healing should yield this sweep.</returns>
    public static bool IsUnderBackpressure(double medianShardOpsPerSecond, double backpressureOpsPerSecond)
    {
        if (backpressureOpsPerSecond <= 0d) return false;
        return medianShardOpsPerSecond >= backpressureOpsPerSecond;
    }

    /// <summary>
    /// Returns the decision reachable from the tree's <em>structural</em>
    /// facts alone - the kill switch, the admission cap, and the shard count -
    /// or <see langword="null"/> when every structural gate is open and the
    /// remaining clauses need a load sample.
    /// <para>
    /// This split is what keeps steady-state observation cheap. The clauses
    /// answered here need only the routing map and the resolved options, both
    /// of which the orchestrator already holds, so a healthy tree - the
    /// overwhelmingly common case, and the one that must cost nothing - is
    /// decided without polling a single shard. Only a tree that really is
    /// over-split pays for a hotness sweep. <see cref="Decide"/> calls this
    /// first, so the two entry points cannot disagree.
    /// </para>
    /// </summary>
    /// <param name="physicalShardCount">The tree's current physical shard count.</param>
    /// <param name="baseShardCount">The tree's configured base physical shard count.</param>
    /// <param name="policy">The resolved healing thresholds.</param>
    public static ShardHealingDecision? DecideStructural(
        int physicalShardCount,
        int baseShardCount,
        in ShardHealingPolicy policy)
    {
        if (!policy.Enabled) return ShardHealingDecision.Disabled;

        if (policy.MaxConcurrentConsolidations <= 0) return ShardHealingDecision.AdmissionClosed;

        // Structural half of the trigger. A tree at or below its base shard
        // count is the healthy steady state and is by far the most common
        // sweep, so it is answered first and costs one comparison.
        if (!ShardSplitAdmissionCore.IsOverSplit(physicalShardCount, baseShardCount))
            return ShardHealingDecision.NotOverSplit;

        return null;
    }

    /// <summary>
    /// Decides whether the tree may start another consolidation this sweep,
    /// applying every clause in evaluation order.
    /// </summary>
    /// <param name="sample">The sweep's observation of the tree.</param>
    /// <param name="policy">The resolved healing thresholds.</param>
    /// <returns>
    /// <see cref="ShardHealingDecision.Admitted"/>, or the first clause that
    /// refused.
    /// </returns>
    public static ShardHealingDecision Decide(in ShardHealingSample sample, in ShardHealingPolicy policy)
    {
        if (DecideStructural(sample.PhysicalShardCount, sample.BaseShardCount, policy) is { } structural)
            return structural;

        // Skew half of the trigger, and the clause that makes the two control
        // loops safe together. Never reachable on the same sample that admits
        // a split, because the two regions are disjoint by construction.
        if (!ShardSplitAdmissionCore.IsConsolidationSkew(sample.SkewRatio, policy.ConsolidationSkewRatio))
            return ShardHealingDecision.SkewedLoad;

        if (sample.InTreeMaintenance) return ShardHealingDecision.TreeMaintenance;

        // Folds serialise behind splits. The consolidation coordinator refuses
        // a donor or survivor with a split in flight, so admitting here would
        // produce a fault rather than progress.
        if (sample.IsSplitting) return ShardHealingDecision.SplitInFlight;

        if (sample.InCooldown) return ShardHealingDecision.Cooldown;

        if (IsUnderBackpressure(sample.MedianShardOpsPerSecond, policy.BackpressureOpsPerSecond))
            return ShardHealingDecision.Backpressure;

        if (sample.InFlightConsolidations >= policy.MaxConcurrentConsolidations)
            return ShardHealingDecision.AtCapacity;

        return ShardHealingDecision.Admitted;
    }
}
