using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ISnapshotBaselineStorageGrain"/> implementation. Holds a
/// single persisted <see cref="SnapshotShardBaseline"/> per (snapshot cursor,
/// physical shard) via the lattice storage provider configured by
/// <see cref="LatticeOptions.StorageProviderName"/>.
/// <para>
/// Intentionally minimal: one read, one write, one clear. The capture fold
/// (leaf-chain walk plus per-leaf tail replay) is owned by
/// <see cref="ShardRootGrain"/>; this grain only persists the materialised
/// result.
/// </para>
/// </summary>
internal sealed class SnapshotBaselineStorageGrain(
    IGrainContext context,
    [PersistentState("snapshot-baseline", LatticeOptions.StorageProviderName)]
    IPersistentState<SnapshotShardBaseline> state) : ISnapshotBaselineStorageGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task SaveAsync(SnapshotShardBaseline baseline, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(baseline);
        cancellationToken.ThrowIfCancellationRequested();

        baseline.Captured = true;
        state.State = baseline;
        await state.WriteStateAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task<SnapshotShardBaseline?> LoadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // The default-allocated state row carries Captured == false; a real
        // baseline is stamped Captured == true on save. This is the only way
        // to tell "no baseline was ever written" apart from "a freshly
        // defaulted state row was returned by the provider".
        if (!state.State.Captured)
        {
            return Task.FromResult<SnapshotShardBaseline?>(null);
        }

        return Task.FromResult<SnapshotShardBaseline?>(state.State);
    }

    /// <inheritdoc />
    public async Task ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!state.State.Captured)
        {
            // Nothing to clear; ClearStateAsync still touches the provider, so
            // short-circuit to keep idempotent calls I/O-free.
            return;
        }

        await state.ClearStateAsync().ConfigureAwait(true);

        // After ClearStateAsync the provider resets the in-memory state;
        // defensively re-seed the sentinel so LoadAsync's null contract holds
        // without relying on the provider's post-clear state shape.
        state.State = new SnapshotShardBaseline();
    }
}
