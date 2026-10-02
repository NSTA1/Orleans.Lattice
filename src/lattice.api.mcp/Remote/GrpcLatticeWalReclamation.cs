using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-mode <see cref="ILatticeWalReclamation"/> (#4237): forwards the WAL
/// reclamation read to a Lattice tree-administration control-API endpoint over gRPC,
/// so <c>lattice_treeadmin_wal_reclamation</c> runs unchanged when the MCP server is
/// hosted outside the silo. The server re-authorizes every call, so this adapter
/// holds no authorization of its own; a cluster that serves no WAL reclamation read
/// answers <c>Unimplemented</c>.
/// </summary>
internal sealed class GrpcLatticeWalReclamation : ILatticeWalReclamation
{
    private readonly LatticeTreeAdminApiGrpcClient _client;

    /// <summary>Initializes the adapter over a tree-administration control-API gRPC client.</summary>
    /// <param name="client">The client. Must not be <c>null</c>.</param>
    public GrpcLatticeWalReclamation(LatticeTreeAdminApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<TreeWalReclamationReport> GetWalReclamationAsync(string treeId, CancellationToken cancellationToken = default)
        => _client.GetWalReclamationAsync(treeId, cancellationToken);
}
