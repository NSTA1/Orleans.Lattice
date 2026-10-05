using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable receive fence on one physical tree copy. See
/// <see cref="ICopyReceiveFenceGrain"/> for the contract. Key: the physical tree
/// id. A copy no restore ever closed holds no state, so the fence costs nothing
/// for it.
/// </summary>
internal sealed class CopyReceiveFenceGrain(
    IGrainContext context,
    [PersistentState("copy-receive-fence", LatticeOptions.StorageProviderName)]
    IPersistentState<CopyReceiveFenceState> state,
    ILogger<CopyReceiveFenceGrain> logger) : ICopyReceiveFenceGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    private string PhysicalTreeId => context.GrainId.Key.ToString()!;

    /// <inheritdoc />
    public Task OnActivateAsync(CancellationToken cancellationToken)
    {
        if (state.State.ClosedBySagaId is not null)
        {
            CopyReceiveFenceCensus.Enrol(PhysicalTreeId, state.State.ClosedAtTicks);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (state.State.ClosedBySagaId is not null)
        {
            CopyReceiveFenceCensus.Withdraw(PhysicalTreeId, state.State.ClosedAtTicks);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task CloseAsync(string sagaId, long minAdmissionEpoch)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);
        ArgumentOutOfRangeException.ThrowIfNegative(minAdmissionEpoch);

        var floor = Math.Max(state.State.MinAdmissionEpoch, minAdmissionEpoch);
        if (string.Equals(state.State.ClosedBySagaId, sagaId, StringComparison.Ordinal)
            && floor == state.State.MinAdmissionEpoch)
        {
            return;
        }

        var previousSaga = state.State.ClosedBySagaId;
        var previousTicks = state.State.ClosedAtTicks;
        var previousFloor = state.State.MinAdmissionEpoch;
        var closedAt = previousSaga is null ? DateTime.UtcNow.Ticks : previousTicks;
        state.State.ClosedBySagaId = sagaId;
        state.State.ClosedAtTicks = closedAt;
        state.State.MinAdmissionEpoch = floor;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.ClosedBySagaId = previousSaga;
            state.State.ClosedAtTicks = previousTicks;
            state.State.MinAdmissionEpoch = previousFloor;
            throw;
        }

        CopyReceiveFenceCensus.Enrol(PhysicalTreeId, closedAt);
        logger.LogInformation(
            "Receive fence closed on restored copy '{PhysicalTreeId}' by saga '{SagaId}' (minimum admission epoch {Epoch}).",
            PhysicalTreeId, sagaId, floor);
    }

    /// <inheritdoc />
    public async Task OpenAsync(string sagaId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        if (!string.Equals(state.State.ClosedBySagaId, sagaId, StringComparison.Ordinal))
        {
            // Open already, or a newer saga owns the close: a superseded saga must
            // not open a copy another restore still holds closed.
            return;
        }

        var closedAt = state.State.ClosedAtTicks;
        state.State.ClosedBySagaId = null;
        state.State.ClosedAtTicks = 0;
        try
        {
            // The minimum admission epoch is kept: it is what refuses an apply
            // admitted before the restore's pause after the copy has opened.
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.ClosedBySagaId = sagaId;
            state.State.ClosedAtTicks = closedAt;
            throw;
        }

        CopyReceiveFenceCensus.Withdraw(PhysicalTreeId, closedAt);
        logger.LogInformation(
            "Receive fence opened on restored copy '{PhysicalTreeId}' by saga '{SagaId}'.",
            PhysicalTreeId, sagaId);
    }

    /// <inheritdoc />
    public Task<CopyReceiveFenceStatus> GetStatusAsync() => Task.FromResult(new CopyReceiveFenceStatus
    {
        Closed = state.State.ClosedBySagaId is not null,
        MinAdmissionEpoch = state.State.MinAdmissionEpoch,
    });
}