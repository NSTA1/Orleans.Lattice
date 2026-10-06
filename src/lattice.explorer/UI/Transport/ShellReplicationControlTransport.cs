using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeReplicationControl"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeReplicationApiGrpcClient"/>. Faults map through
/// <see cref="ShellTransportFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellReplicationControlTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeReplicationApiGrpcClient>(channel, LatticeReplicationApiGrpcClient.Create), ILatticeReplicationControl
{
    /// <inheritdoc />
    public Task<ReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Mode: mode, Source: bootstrapSourceClusterId),
            static (client, state, ct) => client.EnableReplicationAsync(state.TreeId, state.Mode, state.Source, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.DisableReplicationAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.GetReplicationConfigAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<ReplicationDecommissionPeerResult> DecommissionPeerAsync(string peerClusterId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        return CallAsync(
            peerClusterId,
            static (client, state, ct) => client.DecommissionPeerAsync(state, ct),
            null,
            cancellationToken);
    }
}
