using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeReplicationStatus"/> over gRPC. The
/// <see cref="LatticeReplicationStatusGrpcClient"/> already implements the facade;
/// this per-circuit adapter delegates to it only so its transport faults map
/// through <see cref="ShellTransportFaults"/> like every other facade the Shell
/// consumes.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellReplicationStatusTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeReplicationStatusGrpcClient>(channel, LatticeReplicationStatusGrpcClient.Create), ILatticeReplicationStatus
{
    /// <inheritdoc />
    public Task<ReplicationPeerStatusPage> GetPeerStatusAsync(
        ReplicationPeerStatusQuery query,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return CallAsync(query, static (client, state, ct) => client.GetPeerStatusAsync(state, ct), null, cancellationToken);
    }
}
