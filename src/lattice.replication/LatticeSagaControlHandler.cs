using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Durable <see cref="ILatticeSagaControlHandler"/> that replaces the
/// transport-only <see cref="NoParticipantSagaControlHandler"/> default. It is
/// the inbound side of the cross-cluster saga control channel on a participant
/// cluster: it routes each RPC to the per-saga
/// <see cref="ICrossClusterSagaParticipantGrain"/> (keyed by
/// <see cref="SagaControlRequest.SagaId"/>), whose single cluster-wide
/// activation provides the durable prepared record, the cutover fence, and the
/// idempotency guarantees. The gRPC saga service validates and authorizes the
/// request, then delegates to this handler - exactly as the snapshot service
/// delegates to <see cref="LatticeRemoteSnapshotService"/>.
/// <para>
/// Registered by <c>AddLatticeReplication</c> before the gRPC binding's
/// <c>TryAddSingleton</c> default runs, so this durable handler is the
/// effective <see cref="ILatticeSagaControlHandler"/>.
/// </para>
/// </summary>
internal sealed class LatticeSagaControlHandler(IGrainFactory grainFactory) : ILatticeSagaControlHandler
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Participant(request).PrepareAsync(request);
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Participant(request).CommitAsync(request);
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Participant(request).AbortAsync(request);
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Participant(request).GetStatusAsync(request);
    }

    private ICrossClusterSagaParticipantGrain Participant(SagaControlRequest request) =>
        _grainFactory.GetGrain<ICrossClusterSagaParticipantGrain>(request.SagaId);
}
