using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantAdmin"/> over gRPC: a per-circuit adapter
/// over <see cref="LatticeTenantAdminApiGrpcClient"/>, ported from the Tenancy
/// plugin's <c>GrpcTenantAdminClient</c>. Faults map through
/// <see cref="ShellTenantFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantAdminApiGrpcClient>(channel, LatticeTenantAdminApiGrpcClient.Create), ILatticeTenantAdmin
{
    /// <inheritdoc />
    public Task<TenantCreationResult> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(
            (TenantId: tenantId, AdminSubjects: adminSubjects),
            static (client, state, ct) => client.CreateTenantAsync(state.TenantId, state.AdminSubjects, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.SuspendTenantAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.ResumeTenantAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.DeleteTenantAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId,
        TenantQuotasDescriptor quotas,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(
            (TenantId: tenantId, Quotas: quotas),
            static (client, state, ct) => client.SetTenantQuotasAsync(state.TenantId, state.Quotas, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantFaults.Map(exception, subject, cancellationToken);
}
