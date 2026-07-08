using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// In-process <see cref="ISagaControlChannel"/> that routes each RPC directly
/// to a per-cluster <see cref="ICrossClusterSagaParticipantGrain"/> instance,
/// bypassing gRPC. Lets a coordinator drive the durable participant model
/// end-to-end without standing up a real transport (the gRPC round trip is
/// covered separately). An unknown cluster id throws, mirroring an unroutable
/// peer.
/// </summary>
internal sealed class InProcessSagaControlChannel : ISagaControlChannel
{
    private readonly Dictionary<string, ICrossClusterSagaParticipantGrain> _participants =
        new(StringComparer.Ordinal);

    /// <summary>Registers the participant grain that hosts <paramref name="clusterId"/>.</summary>
    public void Register(string clusterId, ICrossClusterSagaParticipantGrain participant) =>
        _participants[clusterId] = participant;

    /// <inheritdoc />
    public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Resolve(clusterId).PrepareAsync(request);

    /// <inheritdoc />
    public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Resolve(clusterId).CommitAsync(request);

    /// <inheritdoc />
    public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Resolve(clusterId).AbortAsync(request);

    /// <inheritdoc />
    public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Resolve(clusterId).GetStatusAsync(request);

    private ICrossClusterSagaParticipantGrain Resolve(string clusterId) =>
        _participants.TryGetValue(clusterId, out var participant)
            ? participant
            : throw new InvalidOperationException($"No participant registered for cluster '{clusterId}'.");
}
