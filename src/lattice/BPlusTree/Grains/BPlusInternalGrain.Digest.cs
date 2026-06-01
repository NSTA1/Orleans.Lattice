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
        });
    }

    /// <summary>
    /// Snapshots this node's current subtree aggregates and forwards them
    /// to <paramref name="parentId"/>'s <see cref="OnChildDigestPublishedAsync"/>
    /// hook. The published <see cref="ChildDigestSnapshot.Hash"/> is a
    /// fresh copy of <c>SubtreeProjectionHash</c> so subsequent XOR
    /// updates on this activation do not retroactively mutate the bytes
    /// the parent's table has captured.
    /// </summary>
    private Task PublishUpwardAsync(GrainId parentId)
    {
        EnsureSubtreeHashInitialized();
        var parent = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
        var snapshot = new ChildDigestSnapshot
        {
            Hash = (byte[])state.State.SubtreeProjectionHash!.Clone(),
            EntryCount = state.State.SubtreeEntryCount,
            CheckpointOffset = state.State.SubtreeHighestCheckpointOffset,
        };
        return parent.OnChildDigestPublishedAsync(context.GrainId, snapshot);
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
