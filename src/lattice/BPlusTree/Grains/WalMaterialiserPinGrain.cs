using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="IWalMaterialiserPinGrain"/>. A single activation per tree
/// (keyed by the tree id) persists the leaf-materialiser checkpoint frontiers
/// to durable grain state so the WAL GC's trim floor survives a full silo or
/// cluster restart. See <see cref="IWalMaterialiserPinGrain"/> for the
/// contract.
/// </summary>
internal sealed class WalMaterialiserPinGrain(
    IGrainContext context,
    [PersistentState("wal-materialiser-pins", LatticeOptions.StorageProviderName)]
    IPersistentState<WalMaterialiserPinState> state) : IGrainBase, IWalMaterialiserPinGrain
{
    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task ReportAsync(string consumerId, HybridLogicalClock frontier)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);

        if (state.State.Pins.TryGetValue(consumerId, out var existing))
        {
            // Monotonic-max merge: a report at or below the stored frontier
            // is coalesced so the pin never rolls backwards (and a Zero
            // re-seed after a real frontier has landed is ignored).
            if (frontier <= existing)
            {
                return;
            }
        }

        state.State.Pins[consumerId] = frontier;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
        Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock>(state.State.Pins, StringComparer.Ordinal));

    /// <inheritdoc />
    public async Task RemoveAsync(string consumerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);

        if (state.State.Pins.Remove(consumerId))
        {
            await state.WriteStateAsync();
        }
    }

    /// <inheritdoc />
    public async Task ClearAsync()
    {
        if (state.State.Pins.Count == 0)
        {
            return;
        }

        state.State.Pins.Clear();
        await state.WriteStateAsync();
    }
}
