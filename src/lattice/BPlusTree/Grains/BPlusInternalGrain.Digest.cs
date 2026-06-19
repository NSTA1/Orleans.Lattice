using System.Buffers.Binary;
using System.IO.Hashing;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal-node digest aggregation. Maintains a running XOR-fold over every
/// descendant leaf's <c>ProjectionHash</c> in
/// <c>state.State.SubtreeProjectionHash</c>, updated incrementally on every
/// <see cref="OnChildDigestPublishedAsync"/> call. The fold is bitwise-identical
/// across silos at the same applied-prefix because the XOR operation is
/// commutative and self-inverse; descendants need not arrive in any particular
/// order. The aggregated entry count and max-reduced checkpoint offset are
/// chained into the final published <see cref="LeafProjectionDigest"/> shape
/// via XxHash128, preserving bit-identical output relative to the legacy
/// per-leaf walk implementation.
/// </summary>
internal sealed partial class BPlusInternalGrain
{
    private const int SubtreeHashSize = 16;

    /// <summary>
    /// Lazily-seeded, per-activation strictly-increasing stamp applied to
    /// every <see cref="ChildDigestSnapshot"/> this internal node forwards
    /// to its own parent (push) or returns on a pull, mirroring the leaf's
    /// <c>NextDigestPublishSequence</c>. Lets the grandparent's fold drop a
    /// stale out-of-order publish for a still-owned child the same way the
    /// leaf-to-parent path does. Seeded from <see cref="DateTime.UtcNow"/>
    /// ticks (never zero) so it stays monotonic across reactivations
    /// without persisting a counter.
    /// </summary>
    private long _digestPublishSeq;

    /// <summary>
    /// Returns the next strictly-increasing publish sequence for this
    /// activation, seeding the counter from the wall clock on first use.
    /// </summary>
    private long NextDigestPublishSequence()
    {
        if (_digestPublishSeq == 0)
        {
            _digestPublishSeq = DateTime.UtcNow.Ticks;
        }
        return ++_digestPublishSeq;
    }

    /// <summary>
    /// Reusable deadline source for <see cref="PublishUpwardAsync"/>. Upward
    /// publishes only run while the activation holds its non-reentrant
    /// <c>_splitGate</c>, so at most one publish is in flight per activation at
    /// a time and this single source is never armed concurrently. Recycling it
    /// (arm with <see cref="CancellationTokenSource.CancelAfter(System.TimeSpan)"/>,
    /// disarm with <see cref="CancellationTokenSource.TryReset"/> after a
    /// non-fired publish) reuses the underlying timer object across the hot path
    /// instead of allocating a fresh <c>CancellationTokenSource(timeout)</c> -
    /// and the one-shot timer it arms - on every publish, every internal level,
    /// every mutation. At rest the source is always disarmed (no scheduled
    /// timer), so it needs no deactivation disposal beyond GC, matching the
    /// activation-lifetime convention of <c>_splitGate</c>.
    /// </summary>
    private CancellationTokenSource? _publishDeadline;

    /// <inheritdoc />
    public async Task SetParentAsync(GrainId? parentId)
    {
        // U9p c2-vi-followup: serialise against AcceptSplitCoreAsync's
        // state writes through the same per-activation _splitGate. The
        // hazard is that c2-vi's [AlwaysInterleave] on AcceptSplitAsync
        // lets it run on the activation while other public methods are
        // also live; this method writes state.State.ParentId and races
        // a concurrent AcceptSplitCoreAsync's writes on Children /
        // SplitState. The gate is non-reentrant; callers of
        // SetParentAsync are cross-grain RPCs that target a child grain
        // (different activation, different SemaphoreSlim), so this is
        // not a self-recursive acquisition.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            if (state.State.ParentId == parentId) return;

            var prev = state.State.ParentId;
            state.State.ParentId = parentId;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.ParentId = prev;
                throw;
            }

            // No reentrant callback into the new parent. The internal-node
            // seeding path that owns this re-parent operation already
            // dispatches a pull of GetChildDigestSnapshotAsync against this
            // node (or the next OnChildDigestPublishedAsync drives the
            // refresh), so a republish here would deadlock against the
            // parent's still-running mutation frame.
        }
        finally
        {
            _splitGate.Release();
        }
    }

    /// <inheritdoc />
    public async Task OnChildDigestPublishedAsync(GrainId childId, ChildDigestSnapshot newSnapshot)
    {
        // U9p c2-vi-followup: serialise against every other state-write
        // path on this activation. ApplyChildSnapshotAsync mutates
        // state.State.ChildDigests / SubtreeProjectionHash /
        // SubtreeEntryCount / SubtreeHighestCheckpointOffset and then
        // awaits state.WriteStateAsync(); without the gate, an
        // interleaved AcceptSplitCoreAsync (marked [AlwaysInterleave])
        // can have a pending WriteStateAsync against the same row in
        // flight, and whichever returns second gets a stale-etag error.
        // The same internal helper ApplyChildSnapshotAsync is also
        // invoked from SeedChildParentAsync inside AcceptSplitCoreAsync
        // (which already holds the gate), so we do NOT push the gate
        // down into the helper - we acquire it here at the public entry
        // point only.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            await ApplyChildSnapshotAsync(childId, newSnapshot);
        }
        finally
        {
            _splitGate.Release();
        }
    }

    /// <summary>
    /// Folds <paramref name="newSnapshot"/> into the persisted subtree
    /// aggregates by XOR-ing out any prior contribution from
    /// <paramref name="childId"/> and XOR-ing the new contribution in.
    /// Recomputes the max-reduced checkpoint offset, persists state, and
    /// republishes upward if a parent is registered. Used both as the
    /// cross-grain hook target (<see cref="OnChildDigestPublishedAsync"/>)
    /// and from the local seeding path that pulls child snapshots after
    /// a topology rotation, so the seeding path can fold without a
    /// reentrant self-call into the non-reentrant internal grain.
    /// </summary>
    internal async Task ApplyChildSnapshotAsync(GrainId childId, ChildDigestSnapshot newSnapshot)
    {
        EnsureSubtreeHashInitialized();

        // Ownership guard. Only fold a snapshot for a child this node
        // currently owns in its Children list. A child that has been
        // re-parented away - for example a child handed to a new sibling
        // during an internal-node split - can still have an in-flight (or
        // coalesced) publish targeting this, its former parent. Folding
        // that stale snapshot would re-add the moved child's entry count
        // and hash contribution here while the new sibling also counts it,
        // permanently double-counting the moved subtree across the chained
        // fold (the proactive PruneMovedChildDigests on split is otherwise
        // undone by the next stale publish). When the publish is for a
        // child we no longer own, drop any lingering row, recompute, and
        // republish the corrected aggregate upward rather than folding.
        // The guard is skipped while Children is empty: a node that has
        // not yet recorded any children cannot make an ownership decision
        // (the seeding race and the isolated-fold unit tests both rely on
        // this), and in production a child is always present in Children
        // before its snapshot is folded here.
        if (state.State.Children.Count > 0 && !OwnsChild(childId))
        {
            if (state.State.ChildDigests.Remove(childId))
            {
                RecomputeSubtreeAggregatesFromChildDigests();
                await state.WriteStateAsync();
                if (state.State.ParentId is { } staleParentId)
                {
                    await PublishUpwardAsync(staleParentId);
                }
            }
            return;
        }

        // Monotonic-sequence staleness guard. Under the [AlwaysInterleave]
        // leaf mutation surface a coalesced per-write publish can read a
        // child's PRE-split (pre-trim) entry count and race the split's
        // post-trim inline publish; the two cross-grain publishes can then
        // apply here out of order, leaving a stale higher count folded for a
        // child this node STILL owns (the ownership guard above only rejects
        // children that have been re-parented away). Each publisher stamps a
        // per-activation-monotonic PublishSequence; drop any snapshot
        // strictly older than the one already folded for this child so a
        // late stale publish cannot overwrite a fresher one. A default
        // sequence of 0 (direct unit-test pushes, range/partial digest
        // computations) is treated as unsequenced and always accepted, so
        // last-write-wins semantics are preserved for callers that do not
        // stamp a sequence.
        if (state.State.ChildDigests.TryGetValue(childId, out var existing)
            && newSnapshot.PublishSequence < existing.PublishSequence)
        {
            return;
        }

        // Update the per-child snapshot table first - it is the single
        // source of truth for every aggregate derived below.
        state.State.ChildDigests[childId] = newSnapshot;
        // Re-derive every aggregate (hash, entry count, max checkpoint)
        // from the persisted dictionary on every apply. The prior
        // incremental shape (`hash ^= prior; hash ^= new;
        // SubtreeEntryCount -= prior; SubtreeEntryCount += new`)
        // gated BOTH the hash XOR-out AND the EntryCount subtract on
        // the prior snapshot having a well-formed length-16 Hash; a
        // prior with a null-or-wrong-length Hash silently skipped
        // the entry-count subtract too, so the new count was added
        // to a stale count that should have been subtracted. Each
        // re-publish for that child compounded the drift, and the
        // CI-only flake on the digest-coalescing integration test
        // (post-split chained fold over-counting by exactly N for N
        // splits) was the visible symptom. Per-child fanout is
        // bounded by <c>MaxInternalChildren</c> (default 128) so
        // recomputing from the table is cheap, and the
        // single-source-of-truth shape leaves no incremental-
        // arithmetic invariant left to violate under topology
        // rewrites, legacy persisted state, or any interleave that
        // could otherwise eat a prior contribution. The shape
        // mirrors how <see cref="SubtreeHighestCheckpointOffset"/>
        // was already recomputed before this fix.
        var hash = state.State.SubtreeProjectionHash!;
        Array.Clear(hash, 0, SubtreeHashSize);
        long entryCount = 0;
        long maxCheckpoint = 0;
        foreach (var kvp in state.State.ChildDigests)
        {
            entryCount += kvp.Value.EntryCount;
            if (kvp.Value.CheckpointOffset > maxCheckpoint)
                maxCheckpoint = kvp.Value.CheckpointOffset;
            if (kvp.Value.Hash is { Length: SubtreeHashSize } childHash)
            {
                for (var i = 0; i < SubtreeHashSize; i++) hash[i] ^= childHash[i];
            }
        }
        state.State.SubtreeEntryCount = entryCount;
        state.State.SubtreeHighestCheckpointOffset = maxCheckpoint;

        await state.WriteStateAsync();

        // Propagate upward when we have a parent. The published snapshot
        // is our own current subtree state, irrespective of whether the
        // delta from this child happened to cancel out at the XOR level -
        // a future-proof shape that gracefully handles tree rewrites
        // (the parent's stored snapshot for us simply gets refreshed).
        if (state.State.ParentId is { } parentId)
        {
            await PublishUpwardAsync(parentId);
        }
    }

    /// <summary>
    /// Re-derives <c>SubtreeProjectionHash</c>, <c>SubtreeEntryCount</c>,
    /// and <c>SubtreeHighestCheckpointOffset</c> from the current
    /// <c>ChildDigests</c> table. The XOR fold is the single source of
    /// truth, so this is safe to call after any structural mutation that
    /// adds or removes rows from the table (e.g. pruning the children
    /// moved to a new sibling during an internal-node split). Does not
    /// persist or publish - the caller owns those side effects so the
    /// recompute can be batched into a single write.
    /// </summary>
    private void RecomputeSubtreeAggregatesFromChildDigests()
    {
        EnsureSubtreeHashInitialized();
        var hash = state.State.SubtreeProjectionHash!;
        Array.Clear(hash, 0, SubtreeHashSize);
        long entryCount = 0;
        long maxCheckpoint = 0;
        foreach (var kvp in state.State.ChildDigests)
        {
            entryCount += kvp.Value.EntryCount;
            if (kvp.Value.CheckpointOffset > maxCheckpoint)
                maxCheckpoint = kvp.Value.CheckpointOffset;
            if (kvp.Value.Hash is { Length: SubtreeHashSize } childHash)
            {
                for (var i = 0; i < SubtreeHashSize; i++) hash[i] ^= childHash[i];
            }
        }
        state.State.SubtreeEntryCount = entryCount;
        state.State.SubtreeHighestCheckpointOffset = maxCheckpoint;
    }

    /// <summary>
    /// Drops the per-child digest snapshot rows for
    /// <paramref name="movedChildIds"/> from this node's
    /// <c>ChildDigests</c> table and re-derives the subtree aggregates
    /// from what remains. Called from the internal-node split path so a
    /// donor that hands half its children to a new sibling stops summing
    /// the moved children's entry counts and hash contributions into its
    /// own subtree digest. Without this prune the donor permanently
    /// double-counts those children (the new sibling counts them too),
    /// inflating the chained-fold aggregate by the moved subtree's entry
    /// total. Returns <see langword="true"/> if any row was removed so
    /// the caller can decide whether a persist is warranted.
    /// </summary>
    internal bool PruneMovedChildDigests(IEnumerable<GrainId> movedChildIds)
    {
        ArgumentNullException.ThrowIfNull(movedChildIds);
        var removedAny = false;
        foreach (var childId in movedChildIds)
        {
            if (state.State.ChildDigests.Remove(childId))
            {
                removedAny = true;
            }
        }
        if (removedAny)
        {
            RecomputeSubtreeAggregatesFromChildDigests();
        }
        return removedAny;
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="childId"/> is
    /// one of this node's currently-owned children. Used by
    /// <see cref="ApplyChildSnapshotAsync"/> as an ownership guard so a
    /// stale digest publish from a child that has since been re-parented
    /// away (e.g. moved to a new sibling during an internal-node split)
    /// is rejected rather than folded back into this node's aggregate.
    /// </summary>
    private bool OwnsChild(GrainId childId)
    {
        var children = state.State.Children;
        for (var i = 0; i < children.Count; i++)
        {
            if (children[i].ChildId == childId)
            {
                return true;
            }
        }
        return false;
    }

    /// <inheritdoc />
    public async Task<LeafProjectionDigest> GetSubtreeProjectionDigestAsync()
    {
        var options = await GetOptionsAsync();
        if (!options.MaintainProjectionDigest)
        {
            throw new InvalidOperationException(
                $"Projection-digest maintenance is disabled for this tree " +
                $"({nameof(LatticeOptions)}.{nameof(LatticeOptions.MaintainProjectionDigest)} = false), " +
                "so the persisted subtree aggregate is not the source of truth and the " +
                "digest API is unavailable. Set the option to true to resume maintenance.");
        }
        EnsureSubtreeHashInitialized();
        return ComputePublishedDigest();
    }

    /// <inheritdoc />
    public Task<ChildDigestSnapshot> GetChildDigestSnapshotAsync()
    {
        EnsureSubtreeHashInitialized();
        return Task.FromResult(new ChildDigestSnapshot
        {
            Hash = (byte[])state.State.SubtreeProjectionHash!.Clone(),
            EntryCount = state.State.SubtreeEntryCount,
            CheckpointOffset = state.State.SubtreeHighestCheckpointOffset,
            PublishSequence = NextDigestPublishSequence(),
        });
    }

    /// <summary>
    /// Snapshots this node's current subtree aggregates and forwards them
    /// to <paramref name="parentId"/>'s <see cref="OnChildDigestPublishedAsync"/>
    /// hook. The published <see cref="ChildDigestSnapshot.Hash"/> is a
    /// fresh copy of <c>SubtreeProjectionHash</c> so subsequent XOR
    /// updates on this activation do not retroactively mutate the bytes
    /// the parent's table has captured.
    /// <para>
    /// The upward publish is a cross-grain RPC that is awaited while this
    /// activation holds its non-reentrant <c>_splitGate</c>, and it
    /// recurses up the internal-node chain toward the shard root. A parent
    /// that is itself mid-mutation can leave the await neither completing
    /// nor faulting, pinning the gate with no ceiling and wedging every
    /// subsequent mutating turn on this activation. The await is therefore
    /// bounded by <see cref="LatticeOptions.DigestPublishTimeout"/>: on a
    /// park the publish is abandoned (its eventual completion is harmlessly
    /// unobserved) and the turn faults with a <see cref="TimeoutException"/>
    /// so the gate releases via the caller's <c>finally</c>. The digest is
    /// staleness-tolerant - the next mutation's dirty-flag publish
    /// re-drives convergence - and the abandoned publish never partially
    /// applied at the parent, so the exact-count invariant is preserved.
    /// When the timeout is <see cref="Timeout.InfiniteTimeSpan"/> the call
    /// is awaited unbounded, restoring the historical behaviour.
    /// </para>
    /// </summary>
    private async Task PublishUpwardAsync(GrainId parentId)
    {
        EnsureSubtreeHashInitialized();
        var parent = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
        var snapshot = new ChildDigestSnapshot
        {
            Hash = (byte[])state.State.SubtreeProjectionHash!.Clone(),
            EntryCount = state.State.SubtreeEntryCount,
            CheckpointOffset = state.State.SubtreeHighestCheckpointOffset,
            PublishSequence = NextDigestPublishSequence(),
        };

        var timeout = (await GetOptionsAsync()).DigestPublishTimeout;
        if (timeout == Timeout.InfiniteTimeSpan)
        {
            await parent.OnChildDigestPublishedAsync(context.GrainId, snapshot);
            return;
        }

        // Recycle a single per-activation deadline source rather than allocating
        // a CancellationTokenSource(timeout) - and arming a fresh one-shot timer -
        // on every publish. Safe because the gate serialises publishes to one
        // in-flight per activation. On the non-fired path TryReset() unschedules
        // the timer and returns the source to the pool; on the fired (timeout)
        // path the source can no longer be reset, so it is dropped and a fresh
        // one is created next publish.
        var deadline = _publishDeadline ??= new CancellationTokenSource();
        deadline.CancelAfter(timeout);
        try
        {
            await parent.OnChildDigestPublishedAsync(context.GrainId, snapshot)
                .WaitAsync(deadline.Token);
        }
        catch (OperationCanceledException oce) when (deadline.IsCancellationRequested)
        {
            deadline.Dispose();
            _publishDeadline = null;
            LatticeMetrics.DigestPublishTimeouts.Add(
                1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
            throw new TimeoutException(
                $"Internal-node digest publish from '{context.GrainId}' of tree "
                + $"'{state.State.TreeId ?? "<unknown>"}' to parent '{parentId}' exceeded the "
                + $"{timeout} publish deadline ({nameof(LatticeOptions.DigestPublishTimeout)}); the "
                + "parent is likely mid-mutation. The publish is abandoned and the split gate "
                + "released; the next mutation's digest publish re-drives convergence.", oce);
        }

        // Non-fired publish: disarm the timer and recycle the source. If the
        // timer fired in the race window after the await resumed successfully,
        // TryReset() fails and we drop the source (the publish still landed, so
        // no false timeout is reported).
        if (!deadline.TryReset())
        {
            deadline.Dispose();
            _publishDeadline = null;
        }
    }

    /// <summary>
    /// Computes the public-shape digest from the persisted subtree
    /// aggregates. The shape mirrors <see cref="BPlusLeafGrain.GetProjectionDigestAsync"/>:
    /// XxHash128 of <c>(SubtreeProjectionHash || EntryCount || CheckpointOffset)</c>.
    /// Two silos at the same applied-prefix observe byte-identical output.
    /// </summary>
    private LeafProjectionDigest ComputePublishedDigest()
    {
        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[8];

        hasher.Append(state.State.SubtreeProjectionHash!);
        BinaryPrimitives.WriteInt64LittleEndian(scratch, state.State.SubtreeEntryCount);
        hasher.Append(scratch[..8]);
        BinaryPrimitives.WriteInt64LittleEndian(scratch, state.State.SubtreeHighestCheckpointOffset);
        hasher.Append(scratch[..8]);

        return new LeafProjectionDigest
        {
            Hash = hasher.GetHashAndReset(),
            EntryCount = state.State.SubtreeEntryCount,
            CheckpointOffset = state.State.SubtreeHighestCheckpointOffset,
            Version = LeafProjectionDigest.CurrentVersion,
        };
    }

    /// <summary>
    /// Lazily initialises <c>SubtreeProjectionHash</c> to a 16-byte zero
    /// buffer on first use (or backfills if persisted state pre-dates
    /// this slot). Treats a missing buffer as zeros so the XOR algebra
    /// applies uniformly across legacy and fresh state shapes.
    /// </summary>
    private void EnsureSubtreeHashInitialized()
    {
        if (state.State.SubtreeProjectionHash is null
            || state.State.SubtreeProjectionHash.Length != SubtreeHashSize)
        {
            state.State.SubtreeProjectionHash = new byte[SubtreeHashSize];
        }
    }
}
