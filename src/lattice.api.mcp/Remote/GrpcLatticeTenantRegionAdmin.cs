using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the per-tenant region-residency control
/// facade (<see cref="ILatticeTenantRegionAdmin"/>) by delegating to the
/// tenant-administration-API gRPC client
/// (<see cref="LatticeTenantAdminApiGrpcClient"/>), so the topology-agnostic
/// region-residency tools work unchanged against a cluster reached over gRPC.
/// Cancellation flows through every call.
/// </summary>
/// <remarks>
/// A pure pass-through that adds no authorization of its own: the caller credential
/// is stamped onto the outbound request by the credential-forwarding interceptor
/// and the remote cluster re-runs the facade's own two-tier, fail-closed gate -
/// operator-only for the allowed set, operator-or-tenant-admin for residency and
/// status. Neither tier is widened by being reached over the wire.
/// </remarks>
internal sealed class GrpcLatticeTenantRegionAdmin : ILatticeTenantRegionAdmin
{
    private readonly LatticeTenantAdminApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied tenant-administration-API gRPC client.</summary>
    public GrpcLatticeTenantRegionAdmin(LatticeTenantAdminApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)
        => _client.AuthorizeAllowedRegionsAsync(tenantId, allowedRegions, cancellationToken);

    /// <inheritdoc />
    public Task<TenantResidencyChangeResult> SetResidencyAsync(
        string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)
        => _client.SetTenantResidencyAsync(tenantId, residencyRegions, cancellationToken);

    /// <inheritdoc />
    public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId, CancellationToken cancellationToken = default)
        => _client.GetTenantRegionStatusAsync(tenantId, cancellationToken);
}
