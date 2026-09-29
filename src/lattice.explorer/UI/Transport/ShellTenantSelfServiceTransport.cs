using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantSelfService"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeTenantSelfServiceApiGrpcClient"/>, ported from
/// the Tenancy plugin's <c>GrpcTenantAdminClient</c>. The read-only tenant
/// self-service surface My Tenant reads. Faults map through
/// <see cref="ShellTenantFaults"/>; the facade unifies an unauthorized tenant with
/// an absent one, so a refusal to see a tenant arrives as
/// <see cref="TenantNotFoundException"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantSelfServiceTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantSelfServiceApiGrpcClient>(channel, LatticeTenantSelfServiceApiGrpcClient.Create), ILatticeTenantSelfService
{
    /// <inheritdoc />
    public Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.GetCurrentTenantAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.ListAccessibleTenantsAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.GetTenantAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantFaults.Map(exception, subject, cancellationToken);
}
