using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable per-tree inbound-apply gate. See <see cref="ITreeReceiveFenceGrain"/>
/// for the contract. State is a single owning saga id, so the pause is
/// crash-durable and idempotent.
/// </summary>
internal sealed class TreeReceiveFenceGrain(
    [PersistentState("tree-receive-fence", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeReceiveFenceState> state,
    ILogger<TreeReceiveFenceGrain> logger)
    : ITreeReceiveFenceGrain
{
    /// <inheritdoc />
    public async Task PauseAsync(string sagaId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        if (string.Equals(state.State.PauseSagaId, sagaId, StringComparison.Ordinal))
        {
            return;
        }

        await PersistOwnerAsync(sagaId);
        logger.LogInformation(
            "Inbound apply paused for a tree by saga '{SagaId}'.", sagaId);
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

        await PersistOwnerAsync(null);
        logger.LogInformation(
            "Inbound apply resumed for a tree by saga '{SagaId}'.", sagaId);
    }

    /// <inheritdoc />
    public Task<bool> IsPausedAsync()
        => Task.FromResult(state.State.PauseSagaId is not null);

    /// <summary>
    /// Assigns and persists the owning saga, restoring the previous owner when the
    /// write fails. Both callers short-circuit on the in-memory owner, so an owner
    /// left assigned by a failed write would turn the saga's retry into a no-op and
    /// the pause or resume would never reach storage.
    /// </summary>
    private async Task PersistOwnerAsync(string? sagaId)
    {
        var previous = state.State.PauseSagaId;
        state.State.PauseSagaId = sagaId;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.PauseSagaId = previous;
            throw;
        }
    }
}
