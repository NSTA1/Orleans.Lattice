using Grpc.Core;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// The tenant admin audit trail half of the client: a direct implementation of
/// <see cref="ILatticeTenantAuditLog"/> over the binding's <c>ListTenantAudit</c> RPC.
/// </summary>
/// <remarks>
/// The client validates only what it can check locally. Tenant-id syntax,
/// authorization and the tenant scope are decided by the server's facade, whose
/// failures reach the caller as an <see cref="RpcException"/>:
/// <see cref="StatusCode.InvalidArgument"/> for a malformed tenant id or query,
/// <see cref="StatusCode.PermissionDenied"/> for a caller that is neither an
/// operator nor an admin of the tenant, and <see cref="StatusCode.Unimplemented"/>
/// when the server's host does not register the facade.
/// </remarks>
public sealed partial class LatticeTenantAdminApiGrpcClient : ILatticeTenantAuditLog
{
    /// <inheritdoc />
    public Task<AdminAuditPage> ListAuditAsync(
        string tenantId, AdminAuditQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(query);
        return UnaryAsync(
            _methods.ListTenantAudit,
            new TenantAdminAuditListRequest { TenantId = tenantId, Query = query },
            cancellationToken);
    }
}
