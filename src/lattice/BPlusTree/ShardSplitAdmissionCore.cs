namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Pure, allocation-free decision core for autonomic shard-split admission.
/// Extracted from <c>HotShardMonitorGrain</c> so the exact production rule can
/// be driven directly under unit test and systematic interleaving without a
/// silo, in the same spirit as <see cref="WalGcTrimCore"/>: a violation the
/// model finds is a violation of the real split trigger.
/// </summary>
/// <remarks>
/// <para>
/// Rate alone cannot tell a hot shard from a hot <em>tree</em>. A bulk ingest
/// streams writes uniformly across the whole key space, so every shard sits far
/// above <see cref="LatticeOptions.HotShardOpsPerSecondThreshold"/> at once.
/// Splitting does not help that workload at all - the load is uniform, so every
/// resulting shard is equally hot - and the only durable effect is a permanent
/// multiplication of grain activations. Admission therefore reasons about the
/// <em>shape</em> of the load as well as its rate, through four independent
/// clauses, each of which is <b>load-bearing</b>:
/// </para>
/// <list type="number">
///   <item>
///     <b>Rate clause</b> - the shard must be at or above the configured
///     operations-per-second threshold. This is the legacy behaviour and is
///     preserved unchanged.
///   </item>
///   <item>
///     <b>Skew clause</b> - the tree's load must be concentrated, measured as
///     <c>maxShardRate / medianShardRate</c> across its physical shards. A
///     uniformly loaded tree sits at approximately <c>1.0</c> and is refused; a
///     genuinely skewed workload sits well above
///     <see cref="LatticeOptions.HotShardMinSkewRatio"/> and is admitted exactly
///     as before. The median is used rather than the mean because it is robust:
///     one hot shard among many barely moves it, so the ratio measures true
///     concentration instead of being diluted by the shard count.
///   </item>
///   <item>
///     <b>Occupancy clause</b> - the shard must hold at least
///     <see cref="LatticeOptions.HotShardMinShardEntries"/> live entries.
///     Splitting a shard of a few dozen records cannot relieve anything and
///     permanently doubles its activation footprint.
///   </item>
///   <item>
///     <b>Ceiling and cooldown clauses</b> - the tree must be below
///     <see cref="LatticeOptions.MaxPhysicalShardsPerTree"/> physical shards and
///     the shard must be outside its post-split cooldown window, so a
///     pathological signal cannot run a tree away.
///   </item>
/// </list>
/// <para>
/// <b>Hysteresis seam.</b> The split trigger and the shard-consolidation
/// trigger form a control loop that would oscillate if their trigger regions
/// overlapped. Both reason against the same statistic - the skew ratio this
/// type computes - and their regions are separated by a dead band:
/// <see cref="IsSplitSkew"/> fires at or above
/// <see cref="ShardSplitAdmissionPolicy.MinSkewRatio"/>,
/// <see cref="IsConsolidationSkew"/> fires at or below
/// <see cref="ShardSplitAdmissionPolicy.ConsolidationSkewRatio"/>, and
/// <see cref="AreTriggerRegionsDisjoint"/> asserts the two never overlap. A
/// consolidation driver must gate on <see cref="IsConsolidationSkew"/> and
/// <see cref="IsOverSplit"/> together, never on shard count alone.
/// </para>
/// </remarks>
internal static class ShardSplitAdmissionCore
{
    /// <summary>
    /// Computes a shard's observed operations per second from its hotness
    /// counters.
    /// </summary>
    /// <param name="reads">Read operations observed over the window.</param>
    /// <param name="writes">Write operations observed over the window.</param>
    /// <param name="window">
    /// The wall-clock window the counters accumulated over. A non-positive
    /// window yields a rate of zero rather than a division by zero, so a shard
    /// that has not yet established a measurement window is never hot.
    /// </param>
    /// <returns>The observed operations per second, never negative.</returns>
    public static double ComputeRate(long reads, long writes, TimeSpan window)
    {
        if (window <= TimeSpan.Zero) return 0d;
        var operations = reads + writes;
        if (operations <= 0) return 0d;
        return operations / window.TotalSeconds;
    }

    /// <summary>
    /// Returns the tree's median shard rate, sorting <paramref name="rates"/> in
    /// place. The caller owns the buffer, so the sort allocates nothing; pass a
    /// scratch copy when the original order still matters.
    /// </summary>
    /// <param name="rates">
    /// Every physical shard's observed rate. Reordered in place by this call.
    /// </param>
    /// <returns>
    /// The lower median (the element at index <c>(length - 1) / 2</c> of the
    /// sorted span), or zero when the span is empty. The lower median is used so
    /// a two-shard tree with one idle shard reports a median of zero and is
    /// correctly treated as fully concentrated rather than as half-loaded.
    /// </returns>
    public static double ComputeMedianRate(Span<double> rates)
    {
        if (rates.IsEmpty) return 0d;
        rates.Sort();
        return rates[(rates.Length - 1) / 2];
    }

    /// <summary>
    /// Computes the tree's load-skew ratio: how many times the tree's median
    /// shard load the hottest shard carries.
    /// </summary>
    /// <param name="maxRate">The highest per-shard rate in the tree.</param>
    /// <param name="medianRate">
    /// The tree's median shard rate, from <see cref="ComputeMedianRate"/>.
    /// </param>
    /// <returns>
    /// Zero when the tree carries no load at all (nothing to concentrate),
    /// <see cref="double.PositiveInfinity"/> when the median is zero but some
    /// shard is loaded (load fully concentrated on a minority of shards), and
    /// <c>maxRate / medianRate</c> otherwise. A perfectly uniform tree returns
    /// exactly <c>1.0</c>.
    /// </returns>
    public static double ComputeSkewRatio(double maxRate, double medianRate)
    {
        if (maxRate <= 0d) return 0d;
        if (medianRate <= 0d) return double.PositiveInfinity;
        return maxRate / medianRate;
    }

    /// <summary>
    /// Whether the tree's load is concentrated enough for a split to relieve
    /// anything. This is the upper edge of the split / consolidation hysteresis
    /// band.
    /// </summary>
    /// <param name="skewRatio">The tree's skew ratio, from <see cref="ComputeSkewRatio"/>.</param>
    /// <param name="minSkewRatio">
    /// The configured admission ratio. A value at or below <c>1.0</c> disables
    /// the skew clause entirely (every distribution has a ratio of at least
    /// <c>1.0</c>), restoring pure rate-based admission.
    /// </param>
    /// <returns><see langword="true"/> when a split may be admitted on skew grounds.</returns>
    public static bool IsSplitSkew(double skewRatio, double minSkewRatio)
    {
        if (minSkewRatio <= 1d) return true;
        return skewRatio >= minSkewRatio;
    }

    /// <summary>
    /// Whether the tree's load is uniform enough that consolidating its shards
    /// would not create a hot spot. This is the lower edge of the split /
    /// consolidation hysteresis band, and is the skew half of a consolidation
    /// driver's trigger.
    /// </summary>
    /// <param name="skewRatio">The tree's skew ratio, from <see cref="ComputeSkewRatio"/>.</param>
    /// <param name="consolidationSkewRatio">
    /// The configured uniformity ratio, strictly below the split ratio.
    /// </param>
    /// <returns><see langword="true"/> when the tree's load is uniform.</returns>
    public static bool IsConsolidationSkew(double skewRatio, double consolidationSkewRatio)
        => skewRatio <= consolidationSkewRatio;

    /// <summary>
    /// Whether the split and consolidation trigger regions are disjoint, so the
    /// two control loops cannot oscillate against each other. Consolidation
    /// fires at or below <paramref name="consolidationSkewRatio"/>; splitting
    /// fires at or above <paramref name="minSkewRatio"/>; the interval between
    /// them is the dead band in which neither acts.
    /// </summary>
    /// <param name="consolidationSkewRatio">The consolidation (lower) edge.</param>
    /// <param name="minSkewRatio">The split (upper) edge.</param>
    /// <returns>
    /// <see langword="true"/> when the regions do not overlap. Always
    /// <see langword="true"/> when the skew clause is disabled
    /// (<paramref name="minSkewRatio"/> at or below <c>1.0</c>), because a
    /// disabled split-skew gate leaves no skew region to overlap with.
    /// </returns>
    public static bool AreTriggerRegionsDisjoint(double consolidationSkewRatio, double minSkewRatio)
    {
        if (minSkewRatio <= 1d) return true;
        return consolidationSkewRatio < minSkewRatio;
    }

    /// <summary>
    /// Whether a tree carries more physical shards than its configured base, and
    /// is therefore a candidate for consolidation on structural grounds. This is
    /// the shard-count half of a consolidation driver's trigger and must be
    /// combined with <see cref="IsConsolidationSkew"/>: shard count alone says
    /// nothing about whether consolidating would recreate a hot spot.
    /// </summary>
    /// <param name="physicalShardCount">The tree's current physical shard count.</param>
    /// <param name="baseShardCount">
    /// The tree's configured base physical shard count (its registry-pinned
    /// <c>ShardCount</c>). Zero or less means "unknown", which reports not
    /// over-split rather than guessing.
    /// </param>
    /// <returns><see langword="true"/> when the tree has grown past its base shard count.</returns>
    public static bool IsOverSplit(int physicalShardCount, int baseShardCount)
        => baseShardCount > 0 && physicalShardCount > baseShardCount;

    /// <summary>
    /// Whether the tree may still grow through autonomic splitting.
    /// </summary>
    /// <param name="physicalShardCount">The tree's current physical shard count.</param>
    /// <param name="maxPhysicalShards">
    /// The configured ceiling. Zero or less means no ceiling.
    /// </param>
    /// <returns><see langword="true"/> when at least one more split is permitted.</returns>
    public static bool HasSplitHeadroom(int physicalShardCount, int maxPhysicalShards)
        => maxPhysicalShards <= 0 || physicalShardCount < maxPhysicalShards;

    /// <summary>
    /// Decides whether a single sampled shard may be split, applying every
    /// admission clause in evaluation order. This is the exact predicate
    /// <c>HotShardMonitorGrain</c> applies to every shard it polls.
    /// </summary>
    /// <param name="sample">The shard's sampled counters.</param>
    /// <param name="policy">The resolved admission thresholds.</param>
    /// <param name="treeSkewRatio">
    /// The whole tree's skew ratio for this pass, from
    /// <see cref="ComputeSkewRatio"/>. It is a property of the tree, not of the
    /// shard, so it is passed alongside the per-shard sample.
    /// </param>
    /// <param name="physicalShardCount">
    /// The tree's current physical shard count, checked against the ceiling.
    /// </param>
    /// <returns>
    /// <see cref="ShardSplitAdmissionOutcome.Admitted"/>, or the first clause
    /// that refused the shard.
    /// </returns>
    public static ShardSplitAdmissionOutcome Evaluate(
        in ShardSplitSample sample,
        in ShardSplitAdmissionPolicy policy,
        double treeSkewRatio,
        int physicalShardCount)
    {
        // A shard that is already the source of an unfinished split is not a
        // candidate and is not a refusal worth reporting: it is progress.
        if (sample.IsSplitting) return ShardSplitAdmissionOutcome.AlreadySplitting;

        // Rate clause first, so a cold shard never produces a structural
        // refusal reason and the deferral counters stay interpretable.
        if (sample.Rate < policy.OpsPerSecondThreshold) return ShardSplitAdmissionOutcome.BelowRateThreshold;

        if (!HasSplitHeadroom(physicalShardCount, policy.MaxPhysicalShards))
            return ShardSplitAdmissionOutcome.ShardCeilingReached;

        // Shape clause: high but uniform load is a bulk ingest, not a hot spot.
        if (!IsSplitSkew(treeSkewRatio, policy.MinSkewRatio)) return ShardSplitAdmissionOutcome.UniformLoad;

        if (sample.InCooldown) return ShardSplitAdmissionOutcome.Cooldown;

        if (sample.OwnedSlots < 2) return ShardSplitAdmissionOutcome.InsufficientSlots;

        // Occupancy clause last: it is the only clause whose input costs an RPC,
        // so it is evaluated only for shards that cleared everything cheaper.
        // A sample carrying ShardSplitSample.EntriesNotSampled skips the clause,
        // which is how the caller runs the cheap phase before probing occupancy.
        if (policy.MinShardEntries > 0
            && sample.Entries >= 0
            && sample.Entries < policy.MinShardEntries)
        {
            return ShardSplitAdmissionOutcome.LowOccupancy;
        }

        return ShardSplitAdmissionOutcome.Admitted;
    }
}
