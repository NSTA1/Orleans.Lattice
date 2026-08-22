using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-node split logic: two-phase crash-safe split with deterministic sibling identity.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation gated entry point for <see cref="SplitAsync"/>.
    /// Tries to acquire <c>_splitGate</c> <em>without blocking</em>; the
    /// single turn that wins the gate owns the split and runs it, while
    /// every concurrent overflowing turn returns immediately rather than
    /// convoying on the gate. Inside the gate the overflow predicate is
    /// re-checked (a just-completed in-flight split may have already
    /// pushed <c>Cache.Count</c> back under the threshold), and the
    /// split runs only if still required. Returns <see langword="null"/>
    /// when the caller did not own the split (either the gate was held
    /// by an in-flight split, or the re-check found nothing to do),
    /// mirroring the no-split branch of the foreground commit paths.
    /// <para>
    /// Why non-blocking: the mutation surface
    /// (<see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetAsync(string, byte[])"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.DeleteAsync"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.MergeManyAsync"/>) is marked
    /// <c>[AlwaysInterleave]</c>, so under a write burst many interleaved
    /// turns observe overflow on the same hot leaf at once. Each turn's
    /// data is already durable (WAL append + projection apply both run
    /// <em>before</em> this predicate is evaluated), and an in-flight
    /// split's cross-grain migration runs a long chain of Azure-Table
    /// round-trips while holding the gate. A blocking
    /// <c>WaitAsync()</c> here parks every concurrent producer slot on
    /// the gate for the full migration duration only for each to
    /// discover, via the re-check, that the in-flight split already
    /// absorbed its overflow - a dead convoy that stalls ingest under
    /// table saturation. Skipping when the gate is contended keeps those
    /// slots flowing; the leaf is transiently over-full but correct (the
    /// owning split, or the next write that wins the gate, migrates the
    /// excess), and reads/writes against an over-full leaf are
    /// unaffected.
    /// </para>
    /// </summary>
    private async Task<SplitResult?> SplitIfNeededUnderGateAsync(int maxLeafKeys)
    {
        // Non-blocking acquire: the loser of the race does NOT wait for
        // the in-flight split's cross-grain migration to drain. Its
        // write is already durable and the owning split (or a later
        // write) will carry the leaf back under threshold, so returning
        // null here is the same observable outcome the blocking re-check
        // would have produced - minus the convoy wait.
        if (!_splitGate.Wait(0))
            return null;
        try
        {
            // Re-check inside the gate. A concurrent turn may have
            // already split this leaf and removed our overflow's
            // entries to the sibling, leaving Cache.Count back under
            // the threshold; in that case we have nothing to do.
            if (Cache.Count <= maxLeafKeys)
                return null;
            return await SplitAsync();
        }
        finally
        {
            _splitGate.Release();
        }
    }

    /// <summary>
    /// Per-activation gated entry point for the recovery-path
    /// <see cref="CompleteSplitAsync"/> calls in
    /// <see cref="SetCoreAsync"/> and
    /// <see cref="MergeManyAsync"/>.
    /// Acquires <c>_splitGate</c>, re-checks
    /// <see cref="Primitives.SplitState.SplitInProgress"/> inside the
    /// gate (a concurrent turn may have already completed the split),
    /// and runs <see cref="CompleteSplitAsync"/> + <see cref="PersistAsync"/>
    /// only if the in-progress state is still observed. Returns
    /// <see langword="null"/> when a concurrent turn already finished
    /// the recovery; the caller still has stable
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.SplitKey"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.SplitSiblingId"/> fields to
    /// route its own write across the donor / sibling boundary, so
    /// the post-gate routing in the caller is correct either way.
    /// <para>
    /// This recovery acquire stays <em>blocking</em> (unlike the
    /// non-blocking acquire in <see cref="SplitIfNeededUnderGateAsync"/>)
    /// because it guards the migration-serialisation invariant: while a
    /// split is mid-flight, <see cref="CompleteSplitAsync"/> snapshots
    /// the donor's <c>&gt;= splitKey</c> entries into the sibling and
    /// then removes them from the donor. A contended turn that skipped
    /// the gate here would route its write to a sibling that is not yet
    /// initialised (tree id / key range / entries unset) or race the
    /// snapshot-then-remove window and lose the write. The thundering
    /// herd that motivated the non-blocking split-predicate acquire is
    /// the simultaneous-overflow case on a not-yet-splitting leaf, which
    /// arrives through <see cref="SplitIfNeededUnderGateAsync"/>, not
    /// here; mid-migration arrivals through this path are comparatively
    /// few and must serialise for correctness.
    /// </para>
    /// </summary>
    private async Task<SplitResult?> CompleteRecoverySplitUnderGateAsync()
    {
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            if (state.State.SplitState != Primitives.SplitState.SplitInProgress)
                return null;
            var recovered = await CompleteSplitAsync();
            await PersistAsync();
            return recovered;
        }
        finally
        {
            _splitGate.Release();
        }
    }

    private async Task<SplitResult> SplitAsync()
    {
        // Only the median key is needed to pivot the split, so avoid
        // materialising every key into a throwaway List<string>. The cache's
        // Keys view is the backing SortedDictionary's ordered key collection:
        // its Count is O(1) and enumerating to the midpoint touches half the
        // keys without copying the whole set into a new array.
        var keys = Cache.Keys;
        int mid = keys.Count() / 2;
        var splitKey = keys.ElementAt(mid);

        // Snapshot the WAL head per partition before the split's
        // intent is persisted. Under multi-partition replay every
        // partition has its own offset space, so the sibling's per-
        // partition replay-from-zero must be bounded by the matching
        // partition's head; a single scalar would conflate them.
        var treeId = state.State.TreeId;
        long[]? walHeadsAtSplit = null;
        if (!string.IsNullOrEmpty(treeId))
        {
            walHeadsAtSplit = await CaptureWalHeadsByPartitionAsync(treeId);
        }

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
        state.State.OldNextSibling = state.State.NextSibling;
        state.State.NextSibling = state.State.SplitSiblingId;
        await PersistAsync();

        LatticeMetrics.LeafSplits.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
        return await CompleteSplitAsync(walHeadsAtSplit);
    }

    /// <summary>
    /// Captures the current WAL head offset across every partition for
    /// the leaf's tree. Returns a non-null array whose length is the
    /// configured <see cref="LatticeOptions.WalPartitions"/>; entry
    /// <c>i</c> is the head of partition <c>i</c> at capture time.
    /// </summary>
    private async Task<long[]> CaptureWalHeadsByPartitionAsync(string treeId)
    {
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        if (partitionCount == 1)
        {
            // Common single-partition shape: skip the WhenAll plumbing.
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{treeId}/0");
            return [await coordinator.GetHeadOffsetAsync(CancellationToken.None)];
        }

        // Each partition's head lives on an independent coordinator grain,
        // so the reads have no ordering dependency - fan them out in
        // parallel instead of awaiting each one serially. On the split
        // fast-path this turns an O(WalPartitions) round-trip chain into a
        // single round-trip's worth of wall-clock latency.
        var tasks = new Task<long>[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{treeId}/{p}");
            tasks[p] = coordinator.GetHeadOffsetAsync(CancellationToken.None);
        }
        return await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times - MergeEntriesAsync is idempotent (LWW merge).
    /// On the recovery path (caller does not hold captured WAL heads) the
    /// optional <paramref name="walHeadsAtSplit"/> is omitted; the recovery
    /// branch reads the current WAL heads fresh, which is still safe - a
    /// later head only causes the sibling to skip more replay, never less.
    /// </summary>
    private async Task<SplitResult> CompleteSplitAsync(long[]? walHeadsAtSplit = null)
    {
        var splitKey = state.State.SplitKey!;
        var siblingId = state.State.SplitSiblingId!.Value;
        var donorPreSplitHigh = state.State.HighKeyExclusive;
        var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(siblingId);

        // Fresh per-partition WAL-head reads on the recovery path.
        long[]? resolvedHeads = walHeadsAtSplit;
        if (resolvedHeads is null)
        {
            var treeId = state.State.TreeId;
            if (!string.IsNullOrEmpty(treeId))
            {
                resolvedHeads = await CaptureWalHeadsByPartitionAsync(treeId);
            }
        }

        var oldNextId = state.State.OldNextSibling;

        // The old-next leaf's back-pointer fixup targets a different grain
        // than the sibling-seeding chain below, so it has no ordering
        // dependency on it - kick it off now and await it alongside the
        // sibling work to overlap the two cross-grain round-trips.
        Task oldNextFixup = Task.CompletedTask;
        if (oldNextId is not null)
        {
            var oldNext = grainFactory.GetGrain<IBPlusLeafGrain>(oldNextId.Value);
            oldNextFixup = oldNext.SetPrevSiblingAsync(siblingId);
        }

        // Seed every birth-time metadata slot on the sibling in one
        // round-trip: tree id, shard index, ownership key range, and the
        // next/prev sibling pointers. This replaces five separate gated
        // setter RPCs (each its own gate acquire + WriteStateAsync) with a
        // single gate acquire and a single persist on the sibling.
        await newLeaf.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = state.State.TreeId!,
            ShardIndex = state.State.ShardIndex,
            LowKeyInclusive = splitKey,
            HighKeyExclusive = donorPreSplitHigh,
            NextSibling = oldNextId,
            PrevSibling = context.GrainId,
        });

        var rightEntries = new Dictionary<string, LwwValue<byte[]>>();
        foreach (var (key, lww) in Cache.EnumerateRows())
        {
            if (string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
            {
                rightEntries[key] = lww;
            }
        }

        if (rightEntries.Count > 0)
        {
            await newLeaf.MergeEntriesAsync(rightEntries);
        }

        // Per-partition projection-checkpoint hints on the sibling, applied
        // in a single round-trip. Each partition's hint is scoped to that
        // partition inside the callee so the sibling's clamp targets the
        // right offset space - replacing the per-partition RPC fan-out.
        if (resolvedHeads is not null)
        {
            await newLeaf.SetCheckpointOffsetHintsAsync(resolvedHeads);
        }

        // Join the back-pointer fixup before mutating the donor's own
        // state so a thrown fixup surfaces here (and not on a later
        // unobserved-task path).
        await oldNextFixup;

        foreach (var key in rightEntries.Keys)
        {
            RemoveEntry(key);
        }

        state.State.HighKeyExclusive = splitKey;
        state.State.OldNextSibling = null;
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        // Advance the donor's per-partition projection checkpoints to
        // the WAL heads captured at split time. Each partition's
        // SetCheckpointOffsetAsync call is scoped to that partition so
        // the per-partition clamp is applied correctly.
        //
        // A split is not instantaneous: while it is in flight the donor
        // keeps applying WAL entries on these partitions, advancing the
        // projection checkpoint past the head captured at split start.
        // The advance here is only meant to push the donor forward to
        // the split frontier, so when the donor's current checkpoint for
        // a partition already meets or exceeds the captured head, skip
        // the advance entirely. Calling SetCheckpointOffsetAsync with a
        // stale head would otherwise ask it to move the checkpoint
        // backward and trip the monotonic-non-decreasing guard. See
        // issue 905.
        if (resolvedHeads is not null)
        {
            var projection = (ILeafProjection)this;
            for (var p = 0; p < resolvedHeads.Length; p++)
            {
                var donorHead = resolvedHeads[p];
                if (donorHead > 0 && GetCurrentCheckpointForPartition(p) < donorHead)
                {
                    using (LatticeApplyOffsetContext.BeginScope(p, donorHead))
                    {
                        await projection.SetCheckpointOffsetAsync(donorHead, CancellationToken.None);
                    }
                }
            }
        }

        await PublishDigestUpwardInlineAsync();

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = siblingId,
            ChildIsLeaf = true,
        };
    }
}
