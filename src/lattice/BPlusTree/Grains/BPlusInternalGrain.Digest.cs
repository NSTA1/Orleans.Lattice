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

    /// <inheritdoc />
    public async Task OnChildDigestPublishedAsync(GrainId childId, ChildDigestSnapshot newSnapshot)
    {
        await ApplyChildSnapshotAsync(childId, newSnapshot);
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
        var hash = state.State.SubtreeProjectionHash!;

        // XOR the prior contribution out (if any) and the new contribution in.
        var hadPrior = state.State.ChildDigests.TryGetValue(childId, out var prior);
        if (hadPrior && prior.Hash is { Length: SubtreeHashSize } priorHash)
        {
            for (var i = 0; i < SubtreeHashSize; i++) hash[i] ^= priorHash[i];
            state.State.SubtreeEntryCount -= prior.EntryCount;
        }
        if (newSnapshot.Hash is { Length: SubtreeHashSize } incoming)
        {
            for (var i = 0; i < SubtreeHashSize; i++) hash[i] ^= incoming[i];
        }
        state.State.SubtreeEntryCount += newSnapshot.EntryCount;
        state.State.ChildDigests[childId] = newSnapshot;

        // Max-reduce upward: a child's checkpoint can only advance, but a
        // child can be REMOVED from the table (split-merge in future code).
        // Recompute the max from the dictionary to remain correct under that
        // shape; the per-child count is bounded by the internal fanout
        // (<= MaxInternalChildren) so the scan is cheap.
        long maxCheckpoint = 0;
        foreach (var kvp in state.State.ChildDigests)
        {
            if (kvp.Value.CheckpointOffset > maxCheckpoint)
                maxCheckpoint = kvp.Value.CheckpointOffset;
        }
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
