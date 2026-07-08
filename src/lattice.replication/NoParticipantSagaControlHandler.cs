namespace Orleans.Lattice.Replication;

/// <summary>
/// Safe default <see cref="ILatticeSagaControlHandler"/> registered by
/// the gRPC binding until a durable coordinator/participant model is
/// wired. It holds no participant state: it reports
/// <see cref="SagaPhase.None"/> for every saga and votes
/// <see cref="SagaVote.Abort"/> on <c>Prepare</c>, which is the safe
/// default (a participant that cannot durably prepare must not let the
/// coordinator commit). Registered with <c>TryAddSingleton</c> so a
/// real participant handler supplied by the host replaces it.
/// </summary>
public sealed class NoParticipantSagaControlHandler : ILatticeSagaControlHandler
{
    private const string NoParticipantDetail =
        "No saga participant is wired on this cluster; the LatticeSaga control channel is transport-only.";

    /// <inheritdoc />
    public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new SagaControlResponse
        {
            SagaId = request.SagaId,
            Phase = SagaPhase.None,
            Vote = SagaVote.Abort,
            Detail = NoParticipantDetail,
        });
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new SagaControlResponse
        {
            SagaId = request.SagaId,
            Phase = SagaPhase.None,
            Vote = SagaVote.None,
            Detail = NoParticipantDetail,
        });
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new SagaControlResponse
        {
            SagaId = request.SagaId,
            Phase = SagaPhase.None,
            Vote = SagaVote.None,
            Detail = NoParticipantDetail,
        });
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new SagaControlResponse
        {
            SagaId = request.SagaId,
            Phase = SagaPhase.None,
            Vote = SagaVote.None,
            Detail = NoParticipantDetail,
        });
    }
}
