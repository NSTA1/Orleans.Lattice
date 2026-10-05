using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafRowRecordGrain"/> implementation: one persisted
/// <see cref="LeafRowRecordState"/> per leaf, via the lattice storage provider.
/// </summary>
internal sealed class LeafRowRecordGrain(
    [PersistentState("leaf-row-record", LatticeOptions.StorageProviderName)]
    IPersistentState<LeafRowRecordState> state) : Grain, ILeafRowRecordGrain
{
    /// <inheritdoc />
    public Task<LeafRowRecordState?> GetAsync() =>
        Task.FromResult(state.RecordExists ? new LeafRowRecordState { TreeId = state.State.TreeId } : null);

    /// <inheritdoc />
    public async Task RecordAsync(string? treeId)
    {
        if (state.RecordExists && (treeId is null || string.Equals(state.State.TreeId, treeId, StringComparison.Ordinal)))
            return;

        var previous = state.State.TreeId;
        state.State.TreeId = treeId ?? previous;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.TreeId = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task ClearAsync()
    {
        if (state.RecordExists)
        {
            await state.ClearStateAsync();
        }

        state.State = new LeafRowRecordState();
    }
}
