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
/// <b>This type answers "is the work budget spent?" and nothing else.</b>
/// Whether the walk may actually stop here is the call site's business, because
/// only the call site knows whether it can name a position to resume from.
/// <see cref="ShouldYield"/> deliberately takes no result count: an earlier
/// revision gated it behind <c>resultsCollected &gt; 0</c> so that a page could
/// never come back empty while claiming more, and that gate silently disarmed
/// both the leaf cap and the deadline for exactly the run of leaves they exist
/// to bound - a wide range whose rows are all tombstoned, TTL-expired,
/// moved away by an adaptive split, or rejected by a pushed-down predicate
/// (issue 1992).
/// </para>
/// <para>
/// <b>Forward progress remains load-bearing, one level up.</b> A bounded walk
/// may only stop where it can hand back a resume position strictly beyond the
/// leaf it stopped on - the visited leaf's exclusive high bound for a forward
/// walk, its inclusive low bound for a reverse one - which the page records
/// carry as <c>ResumeFromKey</c>. A site that cannot name such a position keeps
/// walking rather than emitting a page a caller could not advance past. See
/// <see cref="BoundedLeafWalk"/>, which implements that rule for the background
/// coordinators, and the <c>ShardRootGrain</c> page fills, which implement it
/// for the shard read paths.
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
    /// Whether this turn's work budget is spent - the leaf cap has been reached
    /// or the wall-clock deadline has passed.
    /// <para>
    /// It is a pure work question, independent of how many results the caller
    /// has collected, so it fires just as reliably on a sterile run of leaves
    /// that yields nothing as on a productive one (issue 1992). Acting on it is
    /// the caller's decision: stop only where a resume position can be named.
    /// See the type remarks.
    /// </para>
    /// </summary>
    internal readonly bool ShouldYield()
    {
        if (_leavesVisited >= _maxLeaves)
        {
            return true;
        }

        return _deadlineTimestamp != 0L && Stopwatch.GetTimestamp() >= _deadlineTimestamp;
    }
}
