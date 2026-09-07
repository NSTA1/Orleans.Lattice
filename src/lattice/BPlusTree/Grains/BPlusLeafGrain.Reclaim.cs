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
    /// <summary>
    /// Non-zero once this leaf has been retired from the tree. From that
    /// point a mutation is refused rather than applied, because the leaf is
    /// already out of the routing table and the range it used to own now
    /// belongs to its predecessor, so a write still arriving here is
    /// misdirected and must be re-routed rather than silently applied to a
    /// leaf that is about to be cleared.
    /// </summary>
    private int _reclaimRetired;

    /// <summary>
    /// Mutations admitted through <see cref="EnterMutationScope"/> that have
    /// not yet completed. A retire decision refuses to latch while this is
    /// non-zero: such a mutation was admitted while the leaf was still routed,
    /// so it is a legitimate write whose rows may not have reached the
    /// projection yet, and the emptiness check below would not see them.
    /// </summary>
    private int _mutationsInFlight;

    /// <summary>
    /// Admits a mutation, or refuses it when this leaf has already been
    /// retired from the tree.
    /// <para>
    /// This is the interlock that makes empty-leaf reclaim safe against a
    /// concurrent write. Reclaim decides on the evidence of a probe and then
    /// acts across several further grain calls; the leaf mutation surface is
    /// <c>[AlwaysInterleave]</c> and the commit path takes no gate, so without
    /// an interlock a write can be routed, WAL-appended, applied and
    /// acknowledged inside that window and then erased by the clear. That is a
    /// silent loss of an acknowledged write, and an unrecoverable one: after
    /// the fold the key routes to the predecessor, whose projection checkpoint
    /// is already past the offset the lost write occupies, so no replay ever
    /// re-materialises it.
    /// </para>
    /// <para>
    /// The counter is what closes the window, and the order of the two
    /// statements below is the whole argument. A mutation publishes itself
    /// first and reads the latch second; a retire writes the latch first and
    /// reads the counter second. Whichever of the two runs first, the other
    /// sees it - so a mutation is either refused by the latch or observed by
    /// the count, and cannot be both admitted and unseen.
    /// </para>
    /// </summary>
    private MutationScope EnterMutationScope()
    {
        _mutationsInFlight++;

        if (_reclaimRetired != 0)
        {
            _mutationsInFlight--;
            throw new LeafRetiredException(context.GrainId.ToString());
        }

        return new MutationScope(this);
    }

    /// <summary>
    /// Decrements the leaf's in-flight mutation count on
    /// <see cref="IDisposable.Dispose"/>, so the count falls on every exit
    /// path of the mutation body including an exceptional one.
    /// </summary>
    private readonly struct MutationScope(BPlusLeafGrain grain) : IDisposable
    {
        public void Dispose() => grain._mutationsInFlight--;
    }

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

    /// <inheritdoc />
    public async Task<bool> TryBeginRetirementAsync()
    {
        // Latch FIRST, before any await. Everything that makes this decision
        // sound depends on the leaf being frozen while it is taken, and the
        // latch is the only thing that freezes it.
        //
        // Probing before latching does not work, and the reason is worth
        // stating because it reads as though it would. CountAsync is awaited,
        // so a mutation can be admitted, apply its rows and complete entirely
        // within that await - after the count observed zero and before the
        // latch is set. It is then invisible to both checks: the count ran too
        // early to see its rows, and the in-flight counter runs too late to
        // see the mutation, which has already decremented it. The window is
        // narrow but it is exactly the acknowledged-then-erased loss this gate
        // exists to prevent, so the order has to close it by construction
        // rather than shorten it.
        _reclaimRetired = 1;

        var retire = false;
        try
        {
            // A mutation admitted BEFORE the latch is not refused by it, and
            // its rows may not have reached the projection yet, so the count
            // below could still miss them. Refuse rather than reason about it.
            // Reading the counter here, after the latch and before any await,
            // is the other half of the handshake in EnterMutationScope: that
            // publishes itself first and reads the latch second, this writes
            // the latch first and reads the counter second, so neither can
            // miss the other.
            if (_mutationsInFlight != 0) return false;

            // From here no mutation is in flight and none can be admitted, so
            // the leaf's contents cannot change and the judgement below is
            // taken against state that is frozen for the rest of the fold.
            // This re-runs what the probe ran, but against the state as it is
            // NOW rather than as it was several grain calls ago.
            if (HasReclaimBlockingState()) return false;
            if (await CountAsync(null, null) != 0) return false;

            retire = true;
            return true;
        }
        finally
        {
            // Unlatch on every path that did not commit to retiring,
            // including an exceptional one. A latched leaf that is still
            // routed refuses writes that will re-route straight back to it, so
            // failing to unlatch trades a data-loss risk for a livelock.
            if (!retire) _reclaimRetired = 0;
        }
    }

    /// <inheritdoc />
    public Task AbandonRetirementAsync()
    {
        _reclaimRetired = 0;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Drops a split boundary that a widen has just absorbed, so the leaf
    /// stops advertising that keys it now owns belong to somebody else.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>SplitKey</c> means "keys at or above this value moved to my
    /// successor", and every <c>LeafCacheGrain</c> acts on it by pruning
    /// exactly those keys from its mirror on every refresh - including an
    /// empty one, because the prune runs before the is-empty early return.
    /// That is sound while the successor owns them, and it is why the prune
    /// was written unconditional: a split leaf never regains keys above its
    /// split point, so re-applying the boundary costs nothing.
    /// </para>
    /// <para>
    /// Leaf reclaim is the feature that makes that premise false. Folding an
    /// empty successor away widens this leaf back over the boundary, so it
    /// owns those keys again and replay legitimately materialises them -
    /// while the boundary it still publishes says they belong elsewhere.
    /// Every cache then prunes rows this leaf holds, which reads as a null
    /// through the cache while a direct read returns the row, and no refresh
    /// of any kind heals it because each one re-applies the same prune.
    /// </para>
    /// <para>
    /// Only once the split has finished. <c>SplitState</c> is the guard on
    /// the two forwarding sites - the recovery branch in <c>SetCoreAsync</c>
    /// and the matching one on the merge path - and both dereference
    /// <c>SplitKey</c> with a null-forgiving operator, encoding
    /// "SplitInProgress implies SplitKey is not null". Nulling the key under
    /// an in-flight split would break that invariant, and since
    /// <c>CompareOrdinal(key, null) &gt;= 0</c> holds for every non-null key,
    /// every write would then be forwarded to the sibling instead of only the
    /// keys above the boundary. Gating on the split being finished preserves
    /// the invariant exactly, and is the honest reading besides: while a
    /// split really is in flight there is no stale boundary to clear, because
    /// the keys above it really do belong elsewhere.
    /// </para>
    /// <para>
    /// <c>SplitSiblingId</c> goes with the key it qualifies: after a fold it
    /// can name the leaf reclaim has just retired, and a reference to a
    /// retired grain left in persisted state invites a future reader to
    /// follow it. <c>SplitState</c> itself is a monotone lattice merged
    /// across replicas and is deliberately not reset - driving it backwards
    /// is not an operation the type supports.
    /// </para>
    /// <para>
    /// The caller must have already applied the widen, and must revert
    /// <c>SplitKey</c> and <c>SplitSiblingId</c> alongside the bound if the
    /// persist fails, so that no observer can ever see one without the other.
    /// </para>
    /// </remarks>
    /// <param name="newHighKeyExclusive">
    /// The bound the leaf is widening to; <see langword="null"/> is unbounded
    /// and therefore always past the boundary.
    /// </param>
    /// <returns>Whether a boundary was cleared.</returns>
    private bool TryClearAbsorbedSplitBoundary(string? newHighKeyExclusive)
    {
        if (state.State.SplitKey is not { } splitKey) return false;

        if (state.State.SplitState == SplitState.SplitInProgress) return false;

        // A widen that stops at or below the boundary has not absorbed it,
        // and the successor still owns the keys above it.
        if (newHighKeyExclusive is not null
            && string.CompareOrdinal(newHighKeyExclusive, splitKey) <= 0)
        {
            return false;
        }

        state.State.SplitKey = null;
        state.State.SplitSiblingId = null;
        return true;
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
        //
        // NOTE (see issue #2143): this guards the *victim* only. Nothing
        // checks the predecessor's seal before the fold widens it to cover
        // the victim's span, so a sealed predecessor can end up owning a
        // range it will answer for while its own seal still suppresses
        // slots inside it. Latent rather than live - deliberately left
        // unfixed here to keep this change reviewable - but it is the
        // asymmetry to close if the seal ever gains a second reader.
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
    /// There are TWO declinations here, not one, and they cover opposite
    /// orderings of the same race. The comparison covers a split that landed
    /// after the reclaim built its plan. The <c>SplitInProgress</c> check
    /// covers a reclaim that built its plan after the split intent landed -
    /// in which case the plan names the split's own new sibling, the
    /// comparison agrees, and only this leaf's split state shows that the
    /// successor is about to receive rows. See issue #2160.
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

            // The SAME hazard arriving in the OPPOSITE order, which the
            // comparison above cannot see. See issue #2160.
            //
            // The check above catches a split that landed AFTER the reclaim
            // built its plan: the plan names the pre-split successor, our
            // pointer names the new sibling, they differ, we decline. But
            // SplitAsync persists SplitState, SplitKey, SplitSiblingId and
            // NextSibling in ONE atomic block, so the new sibling S becomes
            // chain-reachable the instant the split intent lands. The reclaim
            // walk follows NextSibling, so a pass starting after that instant
            // builds its plan naming S itself - and then expectedNext == S ==
            // our NextSibling, the comparison agrees, and the fold proceeds.
            //
            // S passes the reclaim probe for a reason that is not a bug in the
            // probe: CompleteSplitAsync seeds S's key range BEFORE awaiting
            // MergeEntriesAsync, so in that window S is a fresh grain with a
            // declared range, zero rows, SplitState.Unsplit and no
            // MovedAwaySlots. HasReclaimBlockingState() on S is legitimately
            // false. The evidence that S must not be touched is not on S at
            // all - it is here, on the leaf that is splitting into it.
            //
            // Unlinking S then lets CompleteSplitAsync merge the split's rows
            // into a leaf that is retired and out of the chain. The retirement
            // latch does not save it: _reclaimRetired is a bare instance field
            // with no persisted counterpart, so if S deactivates in between,
            // the merge call reactivates it with the latch cleared and the
            // rows land silently on an unreachable leaf. Nothing throws.
            //
            // Declining costs nothing that matters: reclaim is background work
            // and the next pass sees a settled topology, exactly as for the
            // comparison above. The shard root releases the retirement latch it
            // took on S on this path (ShardRootGrain.LeafReclaim.cs, the
            // !unlinked arm calls AbandonRetirementAsync), so a declination
            // does not leave S refusing writes.
            if (state.State.SplitState == SplitState.SplitInProgress
                && state.State.SplitSiblingId == expectedNext)
            {
                return false;
            }

            var prevNext = state.State.NextSibling;
            var prevHigh = state.State.HighKeyExclusive;
            var prevSplitKey = state.State.SplitKey;
            var prevSplitSiblingId = state.State.SplitSiblingId;

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

                // And drop a split boundary this widen has just absorbed, in
                // that same write, for the same reason the widen is in it.
                // See TryClearAbsorbedSplitBoundary.
                TryClearAbsorbedSplitBoundary(absorbHighKeyExclusive);
            }

            try
            {
                await PersistAsync();
            }
            catch
            {
                // Class B revert: an activation that believes it has absorbed a
                // range storage says it has not would route and replay-filter
                // against a topology no peer shares. The absorbed split
                // boundary is part of the same decision and reverts with it.
                state.State.NextSibling = prevNext;
                state.State.HighKeyExclusive = prevHigh;
                state.State.SplitKey = prevSplitKey;
                state.State.SplitSiblingId = prevSplitSiblingId;
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
            var prevSplitKey = state.State.SplitKey;
            var prevSplitSiblingId = state.State.SplitSiblingId;

            state.State.HighKeyExclusive = highKeyExclusive;

            // A widen can absorb a split boundary here too, on the resume
            // path, and it has to be cleared in the same persist for the same
            // reason. See TryClearAbsorbedSplitBoundary.
            TryClearAbsorbedSplitBoundary(highKeyExclusive);

            try
            {
                await PersistAsync();
            }
            catch
            {
                // Class B revert: leaving the widened bound in memory while
                // storage still holds the narrow one would have this
                // activation claim ownership of a range no peer routes to it,
                // and the WAL materialiser filters by exactly this bound. The
                // absorbed split boundary is part of the same decision and
                // reverts with it, so no observer can see one without the
                // other.
                state.State.HighKeyExclusive = prevHighKey;
                state.State.SplitKey = prevSplitKey;
                state.State.SplitSiblingId = prevSplitSiblingId;
                throw;
            }
        }
        finally
        {
            _splitGate.Release();
        }
    }
}
