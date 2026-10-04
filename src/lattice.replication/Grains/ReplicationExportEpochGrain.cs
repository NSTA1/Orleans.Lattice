using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="IReplicationExportEpochGrain"/>.</summary>
internal sealed class ReplicationExportEpochGrain(
    [PersistentState("replication-export-epoch", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationExportEpochState> state) : Grain, IReplicationExportEpochGrain
{
    /// <inheritdoc />
    public Task<long> GetAsync() => Task.FromResult(state.State.Epoch);

    /// <inheritdoc />
    public async Task<long> AdvanceAsync()
    {
        state.State.Epoch++;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Epoch--;
            throw;
        }

        return state.State.Epoch;
    }
}
