using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafSnapshotStorageGrain"/> implementation.
/// Holds a single persisted <see cref="LeafSnapshotBlob"/> per leaf
/// via the lattice storage provider configured by
/// <see cref="LatticeOptions.StorageProviderName"/>.
/// <para>
/// The implementation is intentionally minimal: one read, one write,
/// one clear. No projection-side logic lives here; capture and
/// rehydrate logic is owned by <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/> and the
/// maintenance grain that schedules captures.
/// </para>
/// </summary>
internal sealed class LeafSnapshotStorageGrain(
    IGrainContext context,
    [PersistentState("leaf-snapshot", LatticeOptions.StorageProviderName)]
    IPersistentState<LeafSnapshotBlob> state) : ILeafSnapshotStorageGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// True when <paramref name="blob"/> carries a durably-captured prefix that
    /// a leaf can rehydrate from. The scalar <see cref="LeafSnapshotBlob.SnapshotOffset"/>
    /// only describes partition 0; under the default <c>WalPartitions = 8</c> a
    /// leaf whose live keys hash entirely to a non-zero partition captures a
    /// blob whose scalar offset is the <c>-1</c> "partition 0 idle" sentinel yet
    /// whose <see cref="LeafSnapshotBlob.SnapshotOffsetsByPartition"/> covers the
    /// busy partition. Keying the load/clear guards on the scalar alone would
    /// discard that blob on cold restart - and because the coverage-gated WAL GC
    /// has already trimmed the busy partition's covered prefix, discarding the
    /// sole durable copy silently loses it. Treat a blob as captured when the
    /// scalar is non-negative OR any per-partition slot is. Legacy blobs
    /// (persisted before the per-partition slot existed) decode
    /// <see cref="LeafSnapshotBlob.SnapshotOffsetsByPartition"/> as <c>null</c>
    /// and so fall back to the scalar-only check, exactly as before.
    /// </summary>
    private static bool HasCapturedPrefix(LeafSnapshotBlob blob)
    {
        if (blob.SnapshotOffset >= 0)
        {
            return true;
        }

        var perPartition = blob.SnapshotOffsetsByPartition;
        if (perPartition is not null)
        {
            foreach (var offset in perPartition)
            {
                if (offset >= 0)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <inheritdoc />
    public async Task SaveAsync(LeafSnapshotBlob blob, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(blob);
        cancellationToken.ThrowIfCancellationRequested();

        state.State = blob;
        await state.WriteStateAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task<LeafSnapshotBlob?> LoadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // SnapshotOffset == -1 is the "nothing captured" sentinel baked into
        // LeafSnapshotBlob's default, but the scalar only describes partition 0.
        // A blob captured for a leaf whose live data is in a non-zero partition
        // (partition 0 idle) carries a -1 scalar yet a >= 0 per-partition slot;
        // it is loadable and MUST NOT be discarded (see HasCapturedPrefix).
        if (!HasCapturedPrefix(state.State))
        {
            return Task.FromResult<LeafSnapshotBlob?>(null);
        }

        return Task.FromResult<LeafSnapshotBlob?>(state.State);
    }

    /// <inheritdoc />
    public Task<long> GetSnapshotByteSizeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!HasCapturedPrefix(state.State))
        {
            return Task.FromResult(0L);
        }

        // O(1) field read for blobs persisted with the precomputed byte
        // total. Legacy blobs (persisted before the SnapshotBytes slot
        // existed) decode the slot as 0; recompute once from Rows and
        // cache the answer on the in-memory state (no WriteStateAsync) so
        // a subsequent reactivation reading the legacy blob picks the
        // same value back up on first read and the next foreground
        // capture-overwrite stamps the slot durably.
        if (state.State.SnapshotBytes > 0 || state.State.Rows.Count == 0)
        {
            return Task.FromResult(state.State.SnapshotBytes);
        }

        long bytes = 0;
        foreach (var row in state.State.Rows)
        {
            bytes += System.Text.Encoding.UTF8.GetByteCount(row.Key)
                + (row.Value.IsTombstone ? 0 : (row.Value.Value?.Length ?? 0));
        }
        state.State.SnapshotBytes = bytes;
        return Task.FromResult(bytes);
    }

    /// <inheritdoc />
    public async Task ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!HasCapturedPrefix(state.State))
        {
            // Nothing to clear; ClearStateAsync still touches the
            // provider, so short-circuit to keep idempotent calls
            // I/O-free.
            return;
        }

        await state.ClearStateAsync().ConfigureAwait(true);

        // After ClearStateAsync the in-memory state is reset by the
        // provider; defensively re-seed the sentinel so LoadAsync's
        // null contract holds without relying on the provider's
        // post-clear state shape.
        state.State = new LeafSnapshotBlob();
    }
}
