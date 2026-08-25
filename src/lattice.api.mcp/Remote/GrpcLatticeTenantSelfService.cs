using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the read-only tenant self-awareness facade
/// (<see cref="ILatticeTenantSelfService"/>) by delegating to the tenant
/// self-service gRPC client (<see cref="LatticeTenantSelfServiceApiGrpcClient"/>),
/// so the topology-agnostic tenant self-awareness tool group works unchanged
/// against a cluster reached over gRPC. Cancellation flows through every call.
/// </summary>
/// <remarks>
/// The gRPC client projects the wire messages back onto the abstractions DTOs, so
/// this adapter is a pure pass-through that adds no authorization of its own: the
/// caller credential is stamped onto the outbound request by the
/// credential-forwarding interceptor and the remote cluster resolves the caller's
/// subject and scopes enumeration and inspection fail-closed at the facade - the
/// single narrowest seam. The self-service RPCs are exempt from the remote
/// endpoint's default-deny tenant-admin authorizer precisely so this read-only
/// surface lights up for any read-capable caller, exactly as it does on a co-hosted
/// head.
/// </remarks>
internal sealed class GrpcLatticeTenantSelfService : ILatticeTenantSelfService
{
    private readonly LatticeTenantSelfServiceApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied tenant self-service gRPC client.</summary>
    public GrpcLatticeTenantSelfService(LatticeTenantSelfServiceApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)
        => _client.GetCurrentTenantAsync(cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)
        => _client.ListAccessibleTenantsAsync(cancellationToken);

    /// <inheritdoc />
    public Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)
        => _client.GetTenantAsync(tenantId, cancellationToken);
}
