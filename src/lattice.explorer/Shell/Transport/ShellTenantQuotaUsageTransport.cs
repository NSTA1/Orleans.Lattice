using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.Shell.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantQuotaUsage"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeTenantAdminApiGrpcClient"/>'s usage RPC. Part of
/// the tenant self-service surface My Tenant reads. Faults map through
/// <see cref="ShellTenantFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantQuotaUsageTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantAdminApiGrpcClient>(channel, LatticeTenantAdminApiGrpcClient.Create), ILatticeTenantQuotaUsage
{
    /// <inheritdoc />
    public Task<TenantQuotaUsageReport> GetQuotaUsageAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.GetTenantQuotaUsageAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantFaults.Map(exception, subject, cancellationToken);
}
