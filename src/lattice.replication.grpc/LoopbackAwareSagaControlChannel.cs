using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// The <see cref="ISagaControlChannel"/> a coordinator resolves: it routes a saga
/// control call for the <b>local</b> cluster to the in-process
/// <see cref="ILatticeSagaControlHandler"/> and every <b>remote</b> cluster over
/// gRPC through <see cref="GrpcSagaControlChannel"/>.
/// <para>
/// The cross-cluster saga coordinator dispatches to every participant uniformly,
/// including its own (coordinator) cluster. The coordinator and the local
/// participant live in the same Orleans cluster, so the local leg needs no network
/// hop and no configured gRPC endpoint - which also means a deployment does not
/// have to list a loopback endpoint for itself in
/// <see cref="LatticeReplicationGrpcOptions.Peers"/>. This mirrors the dispatcher's
/// peer-reachability pre-flight, which likewise treats the local cluster as always
/// reachable and probes only remote peers. Without this loopback the coordinator
/// would try to open a gRPC channel to its own cluster id and fail because no
/// self-endpoint is configured.
/// </para>
/// </summary>
internal sealed class LoopbackAwareSagaControlChannel : ISagaControlChannel
{
    private readonly GrpcSagaControlChannel _remote;
    private readonly ILatticeSagaControlHandler _local;
    private readonly IOptionsMonitor<GrpcSagaControlChannelOptions> _options;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions;

    /// <summary>Initialises the loopback-aware channel with its remote and local legs.</summary>
    /// <param name="remote">The gRPC channel used for remote participant clusters. Must not be <c>null</c>.</param>
    /// <param name="local">The in-process handler used for the local participant cluster. Must not be <c>null</c>.</param>
    /// <param name="options">The saga channel options carrying the optional local cluster id. Must not be <c>null</c>.</param>
    /// <param name="replicationOptions">The replication options carrying the fallback cluster id. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    public LoopbackAwareSagaControlChannel(
        GrpcSagaControlChannel remote,
        ILatticeSagaControlHandler local,
        IOptionsMonitor<GrpcSagaControlChannelOptions> options,
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions)
    {
        ArgumentNullException.ThrowIfNull(remote);
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(replicationOptions);

        _remote = remote;
        _local = local;
        _options = options;
        _replicationOptions = replicationOptions;
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => IsLocal(clusterId)
            ? _local.PrepareAsync(request, cancellationToken)
            : _remote.PrepareAsync(clusterId, request, cancellationToken);

    /// <inheritdoc />
    public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => IsLocal(clusterId)
            ? _local.CommitAsync(request, cancellationToken)
            : _remote.CommitAsync(clusterId, request, cancellationToken);

    /// <inheritdoc />
    public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => IsLocal(clusterId)
            ? _local.AbortAsync(request, cancellationToken)
            : _remote.AbortAsync(clusterId, request, cancellationToken);

    /// <inheritdoc />
    public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => IsLocal(clusterId)
            ? _local.GetStatusAsync(request, cancellationToken)
            : _remote.GetStatusAsync(clusterId, request, cancellationToken);

    private bool IsLocal(string clusterId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        var configured = _options.CurrentValue.LocalClusterId;
        var local = !string.IsNullOrWhiteSpace(configured)
            ? configured!
            : _replicationOptions.CurrentValue.ClusterId;

        return !string.IsNullOrWhiteSpace(local)
            && string.Equals(clusterId, local, StringComparison.Ordinal);
    }
}
