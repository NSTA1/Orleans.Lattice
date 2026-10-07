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
        Task.FromResult(state.RecordExists
            ? new LeafRowRecordState { TreeId = state.State.TreeId, PurgeCleared = state.State.PurgeCleared }
            : null);

    /// <inheritdoc />
    public async Task RecordAsync(string? treeId)
    {
        if (state.RecordExists
            && !state.State.PurgeCleared
            && (treeId is null || string.Equals(state.State.TreeId, treeId, StringComparison.Ordinal)))
            return;

        var previous = (state.State.TreeId, state.State.PurgeCleared);
        state.State.TreeId = treeId ?? previous.TreeId;
        state.State.PurgeCleared = false;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            (state.State.TreeId, state.State.PurgeCleared) = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task MarkPurgeClearedAsync()
    {
        if (state.RecordExists && state.State.PurgeCleared)
            return;

        state.State.PurgeCleared = true;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.PurgeCleared = false;
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
