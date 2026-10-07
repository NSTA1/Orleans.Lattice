using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable per-tree inbound-apply gate. See <see cref="ITreeReceiveFenceGrain"/>
/// for the contract. State is the owning saga id and the pause epoch, so the
/// pause is crash-durable and idempotent.
/// </summary>
internal sealed class TreeReceiveFenceGrain(
    [PersistentState("tree-receive-fence", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeReceiveFenceState> state,
    ILogger<TreeReceiveFenceGrain> logger)
    : ITreeReceiveFenceGrain
{
    /// <inheritdoc />
    public async Task<long> PauseAsync(string sagaId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        if (string.Equals(state.State.PauseSagaId, sagaId, StringComparison.Ordinal))
        {
            return state.State.Epoch;
        }

        await PersistAsync(sagaId, state.State.Epoch + 1);
        logger.LogInformation(
            "Inbound apply paused for a tree by saga '{SagaId}' (epoch {Epoch}).", sagaId, state.State.Epoch);
        return state.State.Epoch;
    }

    /// <inheritdoc />
    public async Task ResumeAsync(string sagaId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        if (!string.Equals(state.State.PauseSagaId, sagaId, StringComparison.Ordinal))
        {
            // Not the owning saga (or already resumed): a superseded saga must
            // not unpause a tree a newer saga now owns.
            return;
        }

        await PersistAsync(null, state.State.Epoch);
        logger.LogInformation(
            "Inbound apply resumed for a tree by saga '{SagaId}'.", sagaId);
    }

    /// <inheritdoc />
    public Task<bool> IsPausedAsync()
        => Task.FromResult(state.State.PauseSagaId is not null);

    /// <inheritdoc />
    public Task<ReceiveFenceObservation> ObserveAsync()
        => Task.FromResult(new ReceiveFenceObservation
        {
            Paused = state.State.PauseSagaId is not null,
            Epoch = state.State.Epoch,
        });

    /// <summary>
    /// Assigns and persists the owning saga and epoch, restoring the previous
    /// values when the write fails. Both callers short-circuit on the in-memory
    /// owner, so an owner left assigned by a failed write would turn the saga's
    /// retry into a no-op and the pause or resume would never reach storage.
    /// </summary>
    private async Task PersistAsync(string? sagaId, long epoch)
    {
        var previousSaga = state.State.PauseSagaId;
        var previousEpoch = state.State.Epoch;
        state.State.PauseSagaId = sagaId;
        state.State.Epoch = epoch;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.PauseSagaId = previousSaga;
            state.State.Epoch = previousEpoch;
            throw;
        }
    }
}