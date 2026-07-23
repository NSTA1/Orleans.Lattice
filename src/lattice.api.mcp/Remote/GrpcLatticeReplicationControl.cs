using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the replication control facade
/// (<see cref="ILatticeReplicationControl"/>) by delegating to the
/// replication-API gRPC client (<see cref="LatticeReplicationApiGrpcClient"/>), so
/// the topology-agnostic replication tool module works unchanged against a cluster
/// reached over gRPC. Cancellation flows through every call.
/// </summary>
/// <remarks>
/// Every facade member is wire-backed - the replication control surface has a full
/// gRPC binding - so, unlike the backup adapter, no member throws
/// <see cref="NotSupportedException"/>. The gRPC client already projects the wire
/// messages back onto the abstractions DTOs, so this adapter is a pure pass-through
/// that adds no authorization of its own: the caller credential is stamped onto the
/// outbound request by the credential-forwarding interceptor and the remote cluster
/// re-runs the facade's own fail-closed replication access gate.
/// </remarks>
internal sealed class GrpcLatticeReplicationControl : ILatticeReplicationControl
{
    private readonly LatticeReplicationApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied replication-API gRPC client.</summary>
    public GrpcLatticeReplicationControl(LatticeReplicationApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<ReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default)
        => _client.EnableReplicationAsync(treeId, mode, bootstrapSourceClusterId, cancellationToken);

    /// <inheritdoc />
    public Task<ReplicationDisableResult> DisableReplicationAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.DisableReplicationAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<ReplicationConfigReport> GetReplicationConfigAsync(
        CancellationToken cancellationToken = default)
        => _client.GetReplicationConfigAsync(cancellationToken);
}
