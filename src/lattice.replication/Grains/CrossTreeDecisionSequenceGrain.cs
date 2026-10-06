using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="ICrossTreeDecisionSequenceGrain"/>.</summary>
internal sealed class CrossTreeDecisionSequenceGrain(
    [PersistentState("replication-cross-tree-sequence", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreeDecisionSequenceState> state) : Grain, ICrossTreeDecisionSequenceGrain
{
    /// <inheritdoc />
    public async Task<long> IssueAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        if (state.State.Pending.TryGetValue(operationId, out var pending))
        {
            return pending;
        }

        var sequence = state.State.Counter + 1;
        state.State.Counter = sequence;
        state.State.Pending[operationId] = sequence;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Counter = sequence - 1;
            state.State.Pending.Remove(operationId);
            throw;
        }

        return sequence;
    }

    /// <inheritdoc />
    public async Task ConfirmAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        if (!state.State.Pending.Remove(operationId, out var sequence))
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Pending[operationId] = sequence;
            throw;
        }
    }

    /// <inheritdoc />
    public Task<CrossTreeDecisionSequenceSnapshot> GetAsync() =>
        Task.FromResult(new CrossTreeDecisionSequenceSnapshot(
            state.State.Counter,
            state.State.Pending.Count == 0 ? null : state.State.Pending.Values.Min()));
}
