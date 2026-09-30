using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantAccessAdmin"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeTenantAdminApiGrpcClient"/>'s admin-subject RPCs,
/// ported from the Tenancy plugin's <c>GrpcTenantAdminClient</c>. Faults map
/// through <see cref="ShellTenantFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantAccessAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantAdminApiGrpcClient>(channel, LatticeTenantAdminApiGrpcClient.Create), ILatticeTenantAccessAdmin
{
    /// <inheritdoc />
    public Task<TenantAdminSubjectReport> ListAdminSubjectsAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.ListTenantAdminSubjectsAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return CallAsync(
            (TenantId: tenantId, SubjectId: subjectId),
            static (client, state, ct) => client.AddTenantAdminSubjectAsync(state.TenantId, state.SubjectId, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return CallAsync(
            (TenantId: tenantId, SubjectId: subjectId),
            static (client, state, ct) => client.RemoveTenantAdminSubjectAsync(state.TenantId, state.SubjectId, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantFaults.Map(exception, subject, cancellationToken);
}
