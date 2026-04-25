using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-origin high-water-mark grain. See
/// <see cref="IReplicationHighWaterMarkGrain"/> for the contract.
/// </summary>
internal sealed class ReplicationHighWaterMarkGrain(
    [PersistentState("replication-hwm", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationHighWaterMarkState> state)
    : IReplicationHighWaterMarkGrain
{
    /// <inheritdoc />
    public Task<HybridLogicalClock> GetAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(state.State.HighWaterMark);
    }

    /// <inheritdoc />
    public async Task<bool> TryAdvanceAsync(HybridLogicalClock candidate, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (candidate <= state.State.HighWaterMark)
        {
            return false;
        }

        var previous = state.State.HighWaterMark;
        state.State.HighWaterMark = candidate;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Roll the in-memory advance back so a transient storage
            // failure does not leave a phantom HWM that subsequent
            // dedupe checks would surface as if it were persisted.
            state.State.HighWaterMark = previous;
            throw;
        }

        return true;
    }

    /// <inheritdoc />
    public async Task PinSnapshotAsync(HybridLogicalClock asOfHlc, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (state.State.HighWaterMark == asOfHlc)
        {
            return;
        }

        var previous = state.State.HighWaterMark;
        state.State.HighWaterMark = asOfHlc;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.HighWaterMark = previous;
            throw;
        }
    }
}
