using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the tree-administration control facade
/// (<see cref="ILatticeTreeAdmin"/>) by delegating to the tree-administration-API
/// gRPC client (<see cref="LatticeTreeAdminApiGrpcClient"/>), so the
/// topology-agnostic tree-administration tool module works unchanged against a
/// cluster reached over gRPC. Cancellation flows through every call.
/// </summary>
/// <remarks>
/// The gRPC client projects the wire messages back onto the abstractions DTOs, so
/// this adapter is a pure pass-through that adds no authorization of its own: the
/// caller credential is stamped onto the outbound request by the
/// credential-forwarding interceptor and the remote cluster re-runs the facade's
/// own fail-closed access gate. As the tree-administration facade grows operations
/// beyond capability probing, each is added here as a one-line delegation, and the
/// underlying routing invoker can adopt region-targeting without changing this
/// adapter.
/// </remarks>
internal sealed class GrpcLatticeTreeAdmin : ILatticeTreeAdmin
{
    private readonly LatticeTreeAdminApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied tree-administration-API gRPC client.</summary>
    public GrpcLatticeTreeAdmin(LatticeTreeAdminApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.ProbeCapabilitiesAsync(treeId, cancellationToken);
}
