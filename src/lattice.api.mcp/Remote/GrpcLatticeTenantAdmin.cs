using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the tenant-administration control facade
/// (<see cref="ILatticeTenantAdmin"/>) by delegating to the tenant-administration-API
/// gRPC client (<see cref="LatticeTenantAdminApiGrpcClient"/>), so the
/// topology-agnostic tenant-administration tool group works unchanged against a
/// cluster reached over gRPC. Cancellation flows through every call.
/// </summary>
/// <remarks>
/// The gRPC client projects the wire messages back onto the abstractions DTOs, so
/// this adapter is a pure pass-through that adds no authorization of its own: the
/// caller credential is stamped onto the outbound request by the
/// credential-forwarding interceptor and the remote cluster re-runs the facade's
/// own default-deny access gate. The mutating tenant-admin tool group is
/// additionally gated behind its explicit control-tools opt-in, so a split head
/// enables the destructive surface only when its operator asks for it - matching a
/// co-hosted head byte for byte.
/// </remarks>
internal sealed class GrpcLatticeTenantAdmin : ILatticeTenantAdmin
{
    private readonly LatticeTenantAdminApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied tenant-administration-API gRPC client.</summary>
    public GrpcLatticeTenantAdmin(LatticeTenantAdminApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<TenantCreationResult> CreateTenantAsync(
        string tenantId, CancellationToken cancellationToken = default)
        => _client.CreateTenantAsync(tenantId, cancellationToken);

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> SuspendTenantAsync(
        string tenantId, CancellationToken cancellationToken = default)
        => _client.SuspendTenantAsync(tenantId, cancellationToken);

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> ResumeTenantAsync(
        string tenantId, CancellationToken cancellationToken = default)
        => _client.ResumeTenantAsync(tenantId, cancellationToken);

    /// <inheritdoc />
    public Task<TenantDeletionResult> DeleteTenantAsync(
        string tenantId, CancellationToken cancellationToken = default)
        => _client.DeleteTenantAsync(tenantId, cancellationToken);
}
