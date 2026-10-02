using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeWalReclamation"/> over gRPC (#4195): a per-circuit
/// adapter over <see cref="LatticeTreeAdminApiGrpcClient"/>. Faults map through
/// <see cref="ShellTransportFaults"/>; a cluster that serves no WAL reclamation read
/// answers <c>Unimplemented</c>, which arrives as a <see cref="NotSupportedException"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellWalReclamationTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTreeAdminApiGrpcClient>(channel, LatticeTreeAdminApiGrpcClient.Create), ILatticeWalReclamation
{
    /// <inheritdoc />
    public Task<TreeWalReclamationReport> GetWalReclamationAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetWalReclamationAsync(state, ct),
            null,
            cancellationToken);
    }
}
