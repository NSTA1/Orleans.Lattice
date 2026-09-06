using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-side support for empty-leaf chain reclaim: the inverse direction of a
/// split.
/// <para>
/// A split allocates a leaf when a key range grows. Nothing gave the range's
/// leaf count a way back down when the range later shrank, so a chain kept
/// every leaf it had ever reached and a range scan went on paying for the
/// high-water mark rather than for the rows that are actually live. These two
/// seams are what let the shard root fold an emptied leaf out of the chain:
/// one read that answers "may this leaf go?", and one write that hands its
/// abandoned key range to the predecessor that will own it afterwards.
/// </para>
/// <para>
/// Neither seam moves data, because the only leaf either is ever applied to is
/// one that holds no live rows. That is the property that makes reclaim safe
/// to run online: there is no migration window in which a row exists in two
/// places or neither.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task<LeafReclaimProbe> GetReclaimProbeAsync()
    {
        // Reuse CountAsync rather than reading Cache.Count directly: it is the
        // method that already knows about expiry, tombstones, the in-progress
        // split boundary and prepared-but-uncommitted rows. A leaf that looks
        // empty by raw cache count but holds an unexpired tombstone or a
        // pending prepare is not empty for reclaim purposes, and duplicating
        // that judgement here is how the two would drift apart.
        var liveRows = await CountAsync(null, null);

        return new LeafReclaimProbe
        {
            LiveRowCount = liveRows,
            PrevSibling = state.State.PrevSibling,
            NextSibling = state.State.NextSibling,
            LowKeyInclusive = state.State.LowKeyInclusive,
            HighKeyExclusive = state.State.HighKeyExclusive,
            HasBlockingState = HasReclaimBlockingState(),
        };
    }

    /// <summary>
    /// Whether this leaf carries state that forbids reclaim however empty it
    /// looks. Each condition is a case where removing the leaf from the chain
    /// would lose information that is not held anywhere else.
    /// </summary>
    private bool HasReclaimBlockingState()
    {
        // A split that has persisted its intent but not completed owns rows
        // that are mid-flight between this leaf and a sibling that may not
        // exist yet. The row count above can legitimately read zero in that
        // window, so it is exactly the case the count cannot detect.
        if (state.State.SplitState == SplitState.SplitInProgress)
            return true;

        // The moved-away seal is deliberately sticky: it is what stops a
        // donor resurfacing an orphan snapshot for a slot that has migrated
        // to another shard. Deleting the leaf would delete the seal, and the
        // seal outliving the rows is the entire point of it.
        if (state.State.MovedAwaySlots is { Length: > 0 })
            return true;

        // A prepared saga bucket commits onto this leaf later. Unlinking now
        // would land those rows on a leaf no scan can reach.
        if (_pendingTx is { Count: > 0 }) return true;
        if (_pendingTxDeltas is { Count: > 0 }) return true;

        // A destination-side shadow marker means a cross-shard saga is
        // relying on this leaf's read gate; the gate has to outlive the
        // reclaim decision.
        if (_shadowedSagas is { Count: > 0 }) return true;

        return false;
    }

    /// <summary>
    /// Compare-and-swap on this leaf's successor pointer: unlinks
    /// <paramref name="expectedNext"/> and widens onto the range it gives up,
    /// in one persist, and only if this leaf still points at it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// DO NOT REPLACE THE COMPARISON BELOW WITH AN UNCONDITIONAL WRITE. It is
    /// not a redundant equality check, and the reason is not local to this
    /// method, which is exactly why it is written out here rather than left on
    /// the interface for a reader to go and find.
    /// </para>
    /// <para>
    /// Empty-leaf reclaim is a multi-grain sequence (shard root, parent
    /// internal node, predecessor leaf, successor leaf) while
    /// <c>_splitGate</c> is per-grain, so reclaim and split are NOT serialised
    /// with respect to each other. A split of THIS leaf can therefore land in
    /// between the shard root reading our sibling pointer and writing it. That
    /// split inserts a new leaf S between us and the leaf L the reclaim means
    /// to fold away, and moves live rows into S. Writing the pointer the
    /// reclaim planned would then set our successor past S entirely: S is
    /// unlinked from the chain while still holding rows that were live
    /// throughout, so they are invisible to every scan and lost on the next
    /// projection rebuild. That is silent data loss caused by the reclaim path
    /// in the growth direction - the direction that was already correct.
    /// </para>
    /// <para>
    /// Declining is safe where corrupting is not: reclaim is background work,
    /// and the next pass sees a settled topology. Covered by
    /// <c>LeafReclaimSplitRaceIntegrationTests</c>, which drives the
    /// interleaving with a real split and asserts the moved rows survive.
    /// </para>
    /// <para>
    /// The unlink and the widen share one persist for a second, independent
    /// reason: performed as two writes there is a window in which this leaf has
    /// taken over routing for the vacated range while still declaring the
    /// narrower span the WAL materialiser filters by, so a write landing in
    /// that window survives in cache and vanishes on the next rebuild.
    /// </para>
    /// </remarks>
    public async Task<bool> TryUnlinkSuccessorAsync(
        GrainId expectedNext,
        GrainId? newNext,
        string? absorbHighKeyExclusive)
    {
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            // The compare half of the compare-and-swap, and the whole reason
            // this method exists rather than a bare sibling setter. Reclaim is
            // a multi-grain sequence and the split gate is per-grain, so a
            // split of THIS leaf can land between the caller reading our
            // sibling pointer and writing it. That split inserts a new leaf
            // between us and the successor the caller means to remove, and a
            // blind write would point us past it - orphaning a leaf holding
            // the rows the split had just moved into it. Declining is the only
            // safe answer; reclaim is background work and the next pass sees
            // the settled topology.
            if (state.State.NextSibling != expectedNext) return false;

            var prevNext = state.State.NextSibling;
            var prevHigh = state.State.HighKeyExclusive;

            state.State.NextSibling = newNext;

            // Widen in the SAME persist as the unlink. Split into two writes,
            // there is a window in which this leaf has taken over routing for
            // the successor's range while still declaring the narrower span
            // that the WAL materialiser filters by, so a write landing in that
            // window survives in cache and vanishes on the next projection
            // rebuild. One write, no window.
            if (prevHigh is not null
                && (absorbHighKeyExclusive is null
                    || string.CompareOrdinal(absorbHighKeyExclusive, prevHigh) > 0))
            {
                state.State.HighKeyExclusive = absorbHighKeyExclusive;
            }

            try
            {
                await PersistAsync();
            }
            catch
            {
                // Class B revert: an activation that believes it has absorbed a
                // range storage says it has not would route and replay-filter
                // against a topology no peer shares.
                state.State.NextSibling = prevNext;
                state.State.HighKeyExclusive = prevHigh;
                throw;
            }

            return true;
        }
        finally
        {
            _splitGate.Release();
        }
    }

    /// <inheritdoc />
    public async Task AbsorbSuccessorRangeAsync(string? highKeyExclusive)
    {
        // See SetNextSiblingAsync for the gate rationale.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            var current = state.State.HighKeyExclusive;

            // A null high bound already means "unbounded to the right", so
            // this leaf covers whatever the successor covered and there is
            // nothing to widen.
            if (current is null) return;

            // Widen only, and never narrow. A null argument widens to
            // unbounded (the successor was the chain tail); otherwise the
            // successor's bound is taken only when it is genuinely further
            // right. Making the operation monotonic is what makes it
            // idempotent, so a reclaim re-driven after a crash converges
            // instead of walking the bound backwards onto a range this leaf
            // has since been given.
            if (highKeyExclusive is not null
                && string.CompareOrdinal(highKeyExclusive, current) <= 0)
            {
                return;
            }

            var prevHighKey = current;
            state.State.HighKeyExclusive = highKeyExclusive;
            try
            {
                await PersistAsync();
            }
            catch
            {
                // Class B revert: leaving the widened bound in memory while
                // storage still holds the narrow one would have this
                // activation claim ownership of a range no peer routes to it,
                // and the WAL materialiser filters by exactly this bound.
                state.State.HighKeyExclusive = prevHighKey;
                throw;
            }
        }
        finally
        {
            _splitGate.Release();
        }
    }
}
