using System.Collections.Immutable;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>Default <see cref="ICrossTreeBarrierIndexGrain"/>. Calls no other grain.</summary>
internal sealed class CrossTreeBarrierIndexGrain(
    [PersistentState("cross-tree-barrier-index", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreeBarrierIndexState> state) : Grain, ICrossTreeBarrierIndexGrain
{
    /// <inheritdoc />
    public Task<ImmutableArray<string>> GetAsync() => Task.FromResult(state.State.Barriers.ToImmutableArray());

    /// <inheritdoc />
    public async Task AddAsync(string barrierKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(barrierKey);
        if (!state.State.Barriers.Add(barrierKey))
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Barriers.Remove(barrierKey);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task RemoveAsync(string barrierKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(barrierKey);
        if (!state.State.Barriers.Remove(barrierKey))
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Barriers.Add(barrierKey);
            throw;
        }
    }
}
