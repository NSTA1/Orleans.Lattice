using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// In-process <see cref="ISagaControlChannel"/> that routes each RPC directly
/// to a per-cluster <see cref="ICrossClusterSagaParticipantGrain"/> instance,
/// bypassing gRPC. Lets a coordinator drive the durable participant model
/// end-to-end without standing up a real transport (the gRPC round trip is
/// covered separately). An unknown cluster id throws, mirroring an unroutable
/// peer.
/// <para>
/// A participant's decision query (issue #4637) is routed to the coordinator
/// grain registered for the coordinator cluster through a per-cluster view
/// (<see cref="ViewFrom"/>), which stamps the asking cluster as the requester
/// the way the gRPC service stamps the transport origin. A coordinator that is
/// not registered, or one marked unreachable, throws like an unroutable peer.
/// </para>
/// </summary>
internal sealed class InProcessSagaControlChannel : ISagaControlChannel
{
    private readonly Dictionary<string, ICrossClusterSagaParticipantGrain> _participants =
        new(StringComparer.Ordinal);

    private readonly Dictionary<string, ICrossClusterSagaCoordinatorGrain> _coordinators =
        new(StringComparer.Ordinal);

    private readonly Dictionary<string, TaskCompletionSource> _heldCommits = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TaskCompletionSource> _commitsReached = new(StringComparer.Ordinal);

    /// <summary>Whether the registered coordinators cannot be reached.</summary>
    public bool CoordinatorUnreachable { get; set; }

    /// <summary>Registers the participant grain that hosts <paramref name="clusterId"/>.</summary>
    public void Register(string clusterId, ICrossClusterSagaParticipantGrain participant) =>
        _participants[clusterId] = participant;

    /// <summary>Registers the coordinator grain that runs on <paramref name="clusterId"/>.</summary>
    public void RegisterCoordinator(string clusterId, ICrossClusterSagaCoordinatorGrain coordinator) =>
        _coordinators[clusterId] = coordinator;

    /// <summary>
    /// Holds the next commit delivered to <paramref name="clusterId"/> until
    /// <see cref="ReleaseCommit"/>. Returns a task that completes when the held
    /// commit reaches the channel.
    /// </summary>
    public Task HoldCommit(string clusterId)
    {
        _heldCommits[clusterId] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _commitsReached[clusterId] = reached;
        return reached.Task;
    }

    /// <summary>Releases a commit held for <paramref name="clusterId"/>.</summary>
    public void ReleaseCommit(string clusterId)
    {
        if (_heldCommits.TryGetValue(clusterId, out var hold))
            hold.TrySetResult();
    }

    /// <summary>The channel as seen from <paramref name="clusterId"/>: it stamps that cluster as requester.</summary>
    public ISagaControlChannel ViewFrom(string clusterId) => new RequesterView(this, clusterId);

    /// <inheritdoc />
    public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Resolve(clusterId).PrepareAsync(request);

    /// <inheritdoc />
    public async Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        if (_heldCommits.TryGetValue(clusterId, out var hold))
        {
            _commitsReached[clusterId].TrySetResult();
            await hold.Task;
        }

        return await Resolve(clusterId).CommitAsync(request);
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Resolve(clusterId).AbortAsync(request);

    /// <inheritdoc />
    public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Resolve(clusterId).GetStatusAsync(request);

    /// <inheritdoc />
    public async Task<SagaControlResponse> GetDecisionAsync(string coordinatorClusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        if (CoordinatorUnreachable || !_coordinators.TryGetValue(coordinatorClusterId, out var coordinator))
            throw new InvalidOperationException($"Coordinator cluster '{coordinatorClusterId}' cannot be reached.");

        var decision = await coordinator.ResolveDecisionForParticipantAsync(request.RequesterClusterId!);
        return new SagaControlResponse
        {
            SagaId = request.SagaId,
            Phase = decision switch
            {
                CrossClusterSagaDecision.Committed => SagaPhase.Committed,
                CrossClusterSagaDecision.Aborted => SagaPhase.Aborted,
                _ => SagaPhase.Prepared,
            },
        };
    }

    private ICrossClusterSagaParticipantGrain Resolve(string clusterId) =>
        _participants.TryGetValue(clusterId, out var participant)
            ? participant
            : throw new InvalidOperationException($"No participant registered for cluster '{clusterId}'.");

    private sealed class RequesterView(InProcessSagaControlChannel inner, string self) : ISagaControlChannel
    {
        public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.PrepareAsync(clusterId, request, cancellationToken);

        public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.CommitAsync(clusterId, request, cancellationToken);

        public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.AbortAsync(clusterId, request, cancellationToken);

        public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.GetStatusAsync(clusterId, request, cancellationToken);

        public Task<SagaControlResponse> GetDecisionAsync(string coordinatorClusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            inner.GetDecisionAsync(coordinatorClusterId, request with { RequesterClusterId = self }, cancellationToken);
    }
}
