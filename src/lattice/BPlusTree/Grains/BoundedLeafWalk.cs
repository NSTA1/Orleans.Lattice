namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// One work-bounded, resumable pass over a shard's leaf chain, for the
/// background coordinator grains (split drain, merge drain, snapshot copy,
/// tombstone compaction, shard consolidation).
/// <para>
/// It is the single implementation of the budget / cursor / stop-condition
/// logic those coordinators need, so the six background leaf walks share one
/// set of rules rather than three divergent hand-rolled copies (issue 1973).
/// The work bound itself is delegated to <see cref="LeafWalkBudget"/>, the same
/// helper the <c>ShardRootGrain</c> read paths use (issue 1957); what this type
/// adds on top is the <b>resume position</b>, which is what makes a bound safe
/// on a walk that must survive a turn boundary.
/// </para>
/// <para>
/// <b>The cursor is a key, never a leaf <see cref="GrainId"/>.</b> Orleans
/// grains are virtual, so a leaf id persisted across a turn boundary can
/// activate a fresh, empty grain whose sibling pointer is <see langword="null"/>:
/// the resumed walk would conclude it had reached the end of the chain and stop,
/// silently leaving the rest of the shard unvisited. A key can always be
/// re-descended onto whichever leaf now owns it, at the cost of one traversal,
/// so a key cursor turns that silent truncation into an ordinary lookup.
/// </para>
/// <para>
/// <b>Only stop where you can resume.</b> The walk yields at a leaf boundary
/// only when it can name the key the next pass must start from - the visited
/// leaf's exclusive high bound, which is exactly where the next leaf begins. A
/// leaf that declares no usable high bound is not a stopping point: the walk
/// keeps going rather than stop without a resume position, degrading to
/// today's unbounded behaviour instead of to a truncated sweep. That is the
/// same rule the bounded range-delete and page-fill walks follow.
/// </para>
/// <para>
/// <b>Forward progress.</b> A pass always visits at least one leaf before it
/// can yield, and the resume key it emits lies strictly beyond the leaf it
/// yielded on, so a resumed walk can never re-derive the position it started
/// from. Re-visiting a leaf is nonetheless harmless at every call site: each
/// forwards entries under their original HLC, so a re-drain is a fixed point
/// under LWW merge.
/// </para>
/// </summary>
internal sealed class BoundedLeafWalk
{
    private readonly IGrainFactory _grainFactory;
    private readonly string? _startKeyInclusive;
    private LeafWalkBudget _budget;
    private GrainId? _leafId;

    /// <summary>
    /// The resolved grain for <see cref="_leafId"/>, cached so a leaf is
    /// resolved once per visit rather than once for the caller's read and again
    /// for the advance. The walk itself is one allocation per <em>pass</em>,
    /// amortised over up to a budget's worth of leaf RPCs, so it is not on any
    /// per-leaf path.
    /// </summary>
    private IBPlusLeafGrain? _leaf;

    private BoundedLeafWalk(
        IGrainFactory grainFactory, GrainId? leafId, string? startKeyInclusive, LeafWalkBudget budget)
    {
        _grainFactory = grainFactory;
        _leafId = leafId;
        _startKeyInclusive = startKeyInclusive;
        _budget = budget;
        Completed = leafId is null;
    }

    /// <summary>
    /// Opens a bounded pass over <paramref name="shard"/>'s leaf chain,
    /// resuming from <paramref name="resumeFromInclusive"/> when a previous
    /// pass persisted one and starting at the leftmost leaf otherwise.
    /// <para>
    /// A pass with no resume position goes through
    /// <see cref="IShardRootGrain.GetLeftmostLeafIdAsync"/>, and only a genuine
    /// resume consults the key-descent seam. The two are equivalent - the
    /// shard's resolver returns the leftmost leaf for a null key - but keeping
    /// the fresh start on the long-standing entry point makes the key seam
    /// exactly what its name says: the resume path.
    /// </para>
    /// <para>
    /// An empty shard yields a walk that is already <see cref="Completed"/> with
    /// no leaf to visit, which callers treat as a finished sweep rather than as
    /// an error.
    /// </para>
    /// </summary>
    internal static async Task<BoundedLeafWalk> StartAsync(
        IGrainFactory grainFactory,
        IShardRootGrain shard,
        string? resumeFromInclusive,
        LeafWalkBudget budget)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(shard);

        var leafId = string.IsNullOrEmpty(resumeFromInclusive)
            ? await shard.GetLeftmostLeafIdAsync()
            : await shard.GetLeafIdForKeyAsync(resumeFromInclusive);
        return new BoundedLeafWalk(grainFactory, leafId, resumeFromInclusive, budget);
    }

    /// <summary>
    /// The leaf the caller should process now, or <see langword="null"/> when
    /// the pass has no further leaf to visit.
    /// </summary>
    internal GrainId? CurrentLeafId => _leafId;

    /// <summary>Whether there is a leaf to process.</summary>
    internal bool HasLeaf => _leafId is not null;

    /// <summary>
    /// Whether the walk reached the end of the leaf chain. A completed walk has
    /// no <see cref="ResumeFromInclusive"/>: there is nothing left to resume.
    /// </summary>
    internal bool Completed { get; private set; }

    /// <summary>
    /// The key the next pass must resume from, or <see langword="null"/> when
    /// the chain was swept to its end. Persist this verbatim; it is the whole
    /// resume position.
    /// </summary>
    internal string? ResumeFromInclusive { get; private set; }

    /// <summary>Leaves visited by this pass.</summary>
    internal int LeavesVisited => _budget.LeavesVisited;

    /// <summary>The grain for <see cref="CurrentLeafId"/>.</summary>
    internal IBPlusLeafGrain CurrentLeaf =>
        _leaf ??= _grainFactory.GetGrain<IBPlusLeafGrain>(
            _leafId ?? throw new InvalidOperationException(
                "BoundedLeafWalk has no current leaf; check HasLeaf before reading CurrentLeaf."));

    /// <summary>
    /// Records that the current leaf has been processed and advances to the
    /// next one, returning <see langword="false"/> when the pass should stop.
    /// <para>
    /// It stops for one of two reasons, which the caller distinguishes through
    /// <see cref="Completed"/>: the chain ended (nothing left to do), or the
    /// work budget was spent at a leaf whose high bound gives a resume position
    /// (<see cref="ResumeFromInclusive"/> is set). Spending the budget at a
    /// leaf with no usable high bound is not a stop - the walk continues,
    /// because stopping without a resume position would truncate the sweep.
    /// </para>
    /// </summary>
    internal async Task<bool> MoveNextAsync()
    {
        if (_leafId is null) return false;

        // Reuse the resolved reference when the caller already read CurrentLeaf,
        // which every call site does, so a leaf costs one resolution per visit.
        var leaf = CurrentLeaf;
        _budget.RecordLeafVisited();

        var next = await leaf.GetNextSiblingAsync();
        if (next is null)
        {
            _leafId = null;
            _leaf = null;
            Completed = true;
            ResumeFromInclusive = null;
            return false;
        }

        // resultsCollected is 1 because the unit of progress here is a leaf
        // processed and a cursor advanced, not a row returned. These walks emit
        // no page for a caller to derive a continuation from, so the
        // forward-progress rule the page fills need does not apply; the resume
        // key below is what guarantees progress instead.
        if (_budget.ShouldYield(resultsCollected: 1))
        {
            // The visited leaf's exclusive high bound is exactly where the next
            // leaf begins, so re-descending onto it lands on the leaf that owns
            // the rest of the chain - even if either leaf has split in the
            // meantime, because a split only narrows a leaf's range and the
            // descent follows whichever leaf now covers the key.
            //
            // Read only when the budget wants to yield, so the common case pays
            // no extra round trip per leaf.
            // Read only when the budget wants to yield, so the common case pays
            // no extra round trip per leaf.
            var bounds = await leaf.GetKeyRangeAsync();
            if (bounds.HighKeyExclusive is { } high && IsAheadOfStart(high))
            {
                _leafId = null;
                _leaf = null;
                Completed = false;
                ResumeFromInclusive = high;
                return false;
            }
        }

        _leafId = next;
        _leaf = null;
        return true;
    }

    /// <summary>
    /// Whether <paramref name="candidate"/> is a resume position strictly ahead
    /// of the one this pass started from.
    /// <para>
    /// Forward progress should follow from the B+ tree's own shape - a leaf owns
    /// <c>[Low, High)</c>, so its exclusive high bound is beyond every key it
    /// holds, including the one the pass resumed at. This check makes that
    /// structural rather than assumed: a leaf whose persisted bounds have
    /// drifted out of step with its parent's separators could otherwise hand
    /// back the very key the pass began with, and a coordinator that drives
    /// passes until the sweep completes would then re-issue an identical pass
    /// forever. Refusing the candidate falls through to "keep walking", the same
    /// safe degradation as a leaf that declares no high bound at all.
    /// </para>
    /// </summary>
    private bool IsAheadOfStart(string candidate)
        => _startKeyInclusive is null
            || string.CompareOrdinal(candidate, _startKeyInclusive) > 0;
}
