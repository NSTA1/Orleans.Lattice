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

        state.State.PauseSagaId = sagaId;
        await state.WriteStateAsync();
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

        state.State.PauseSagaId = null;
        await state.WriteStateAsync();
        logger.LogInformation(
            "Inbound apply resumed for a tree by saga '{SagaId}'.", sagaId);
    }

    /// <inheritdoc />
    public Task<bool> IsPausedAsync()
        => Task.FromResult(state.State.PauseSagaId is not null);
}
