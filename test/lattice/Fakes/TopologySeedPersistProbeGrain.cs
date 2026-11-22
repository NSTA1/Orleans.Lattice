using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.Fakes;

/// <inheritdoc cref="ITopologySeedPersistProbeGrain" />
internal sealed class TopologySeedPersistProbeGrain(
    [PersistentState(TopologySeedPersist.InternalStateName, LatticeOptions.StorageProviderName)]
    IPersistentState<InternalNodeState> state) : Grain, ITopologySeedPersistProbeGrain
{
    /// <inheritdoc />
    public async Task<string?> WriteAsync(long value)
    {
        state.State.SubtreeEntryCount = value;
        await TopologySeedPersist.WriteRecoveringLandedConflictAsync(state, null, GrainContext);
        return state.Etag;
    }

    /// <inheritdoc />
    public async Task<bool> TryWriteSwallowingConflictAsync(long value)
    {
        state.State.SubtreeEntryCount = value;
        try
        {
            await TopologySeedPersist.WriteRecoveringLandedConflictAsync(state, null, GrainContext);
            return true;
        }
        catch (Orleans.Storage.InconsistentStateException)
        {
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<long> ReadStoredAsync()
    {
        await state.ReadStateAsync();
        return state.State.SubtreeEntryCount;
    }
}
