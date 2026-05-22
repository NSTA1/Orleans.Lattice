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
/// rehydrate logic is owned by <see cref="BPlusLeafGrain"/> and the
/// maintenance grain that schedules captures.
/// </para>
/// </summary>
internal sealed class LeafSnapshotStorageGrain(
    IGrainContext context,
    [PersistentState("leaf-snapshot", LatticeOptions.StorageProviderName)]
    IPersistentState<LeafSnapshotBlob> state) : ILeafSnapshotStorageGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

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

        // SnapshotOffset == -1 is the "nothing captured" sentinel
        // baked into LeafSnapshotBlob's default. Distinguishing it
        // from a default-allocated state instance is the only way to
        // tell "no snapshot has ever been written" apart from "a
        // freshly-defaulted state row was returned by the provider".
        if (state.State.SnapshotOffset < 0)
        {
            return Task.FromResult<LeafSnapshotBlob?>(null);
        }

        return Task.FromResult<LeafSnapshotBlob?>(state.State);
    }

    /// <inheritdoc />
    public async Task ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (state.State.SnapshotOffset < 0)
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
