using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounds the <em>work</em> of a single leaf-chain walk turn, so a page-fill
/// loop is limited by the number of leaves it visits rather than only by the
/// number of results it happens to collect.
/// <para>
/// The distinction matters because a leaf can cost a full grain call (plus, on
/// a cold activation, a snapshot rehydration or WAL replay) and still
/// contribute nothing to the page: its entries may all be filtered as
/// moved-away by an adaptive split, tombstoned and inside
/// <see cref="LatticeOptions.TombstoneGracePeriod"/>, TTL-expired, or rejected
/// by a pushed-down predicate. A loop bounded only by output therefore has a
/// worst case of O(leaves in range) per call, and because
/// <c>ShardRootGrain</c> reads are deliberately non-reentrant that call
/// head-of-line-blocks every other request to the shard for its whole
/// duration (issue 1955).
/// </para>
/// <para>
/// <b>Forward progress is the load-bearing invariant.</b>
/// <see cref="ShouldYield"/> only ever returns <see langword="true"/> once at
/// least one result has been collected. A caller derives its next continuation
/// token from the last result in the page, so a page that is empty but claims
/// <c>HasMore = true</c> would leave it re-issuing an identical request
/// forever. Guaranteeing at least one result keeps every existing caller
/// correct with no wire-format change: a short page is already a representable
/// state, whereas an empty one carrying more is not. The residual cost is that
/// a genuinely sterile run of leaves (a wide range whose entries are all
/// tombstoned or moved away) still walks unbounded; closing that needs an
/// additive resume-key on the page records, which is deliberately out of scope
/// here.
/// </para>
/// </summary>
internal struct LeafWalkBudget
{
    private readonly int _maxLeaves;
    private readonly long _deadlineTimestamp;
    private int _leavesVisited;

    /// <summary>
    /// Creates a budget. A non-positive <paramref name="maxLeaves"/> disables
    /// the leaf bound, and a null or non-positive <paramref name="maxDuration"/>
    /// disables the deadline, so a misconfigured option degrades to today's
    /// unbounded behaviour rather than to a silently truncated walk.
    /// </summary>
    internal LeafWalkBudget(int maxLeaves, TimeSpan? maxDuration)
    {
        _maxLeaves = maxLeaves > 0 ? maxLeaves : int.MaxValue;
        _deadlineTimestamp = maxDuration is { } duration && duration > TimeSpan.Zero
            ? Stopwatch.GetTimestamp() + (long)(duration.TotalSeconds * Stopwatch.Frequency)
            : 0L;
        _leavesVisited = 0;
    }

    /// <summary>
    /// Builds the budget a shard range-scan page fill runs under, from the
    /// tree's resolved options.
    /// </summary>
    internal static LeafWalkBudget ForScanPage(LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new LeafWalkBudget(options.MaxLeavesPerScanPage, options.MaxScanPageDuration);
    }

    /// <summary>
    /// Builds the budget a background coordinator's drain pass runs under - the
    /// split drain, the cross-tree merge drain, and the online snapshot copy.
    /// <para>
    /// These walks are bounded for a different reason from the read paths: they
    /// hold their own non-reentrant coordinator rather than a shard root, so
    /// the cost of an unbounded pass is a coordinator that cannot report
    /// progress or honour a cancellation until the whole shard is swept. The
    /// mechanism is nonetheless identical, which is why they share this type
    /// (issue 1973).
    /// </para>
    /// </summary>
    internal static LeafWalkBudget ForBackgroundDrain(LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new LeafWalkBudget(options.BackgroundDrainLeavesPerPass, options.BackgroundDrainMaxDuration);
    }

    /// <summary>
    /// Builds the budget for a background pass whose leaf cap is a
    /// coordinator-specific option rather than the shared background default -
    /// the tombstone compactor's <see cref="LatticeOptions.CompactionLeafBatchSize"/>
    /// and the shard consolidator's
    /// <see cref="LatticeOptions.ConsolidationDrainLeavesPerPass"/>. Both keep
    /// their own long-standing knob and inherit the shared wall-clock net.
    /// </summary>
    internal static LeafWalkBudget ForBackgroundDrain(int maxLeaves, LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new LeafWalkBudget(maxLeaves, options.BackgroundDrainMaxDuration);
    }

    /// <summary>
    /// A budget that never yields, for a walk whose whole-walk atomicity is
    /// load-bearing and which therefore runs to the end of the chain in one
    /// turn. Expressing that as an explicit unbounded budget - rather than as a
    /// second, un-budgeted copy of the walk - keeps every leaf-chain walk on
    /// the one implementation, so the intent is visible at the call site and a
    /// site cannot drift into being unbounded by omission (issue 1973). Pair it
    /// with <see cref="AtomicLeafWalk"/> so the hold is attributable.
    /// </summary>
    internal static LeafWalkBudget Unbounded() => new(0, null);

    /// <summary>Leaves visited so far in this turn.</summary>
    internal readonly int LeavesVisited => _leavesVisited;

    /// <summary>
    /// Records that one leaf has been visited. Call once per leaf actually
    /// read, so the count reflects work done rather than results kept.
    /// </summary>
    internal void RecordLeafVisited() => _leavesVisited++;

    /// <summary>
    /// Whether the walk should stop here and return a partial page.
    /// <para>
    /// Returns <see langword="false"/> whenever
    /// <paramref name="resultsCollected"/> is zero, regardless of how much
    /// budget has been spent: yielding an empty page that claims more is
    /// available would strand a caller that derives its continuation from the
    /// last returned result. See the type remarks.
    /// </para>
    /// </summary>
    internal readonly bool ShouldYield(int resultsCollected)
    {
        if (resultsCollected <= 0)
        {
            return false;
        }

        if (_leavesVisited >= _maxLeaves)
        {
            return true;
        }

        return _deadlineTimestamp != 0L && Stopwatch.GetTimestamp() >= _deadlineTimestamp;
    }
}
