using Grpc.Core;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// The read-only tenant admin audit trail RPC: a thin adapter over the optional
/// <see cref="ILatticeTenantAuditLog"/> facade, which owns the authorization decision
/// and confines the read to the validated tenant.
/// </summary>
internal sealed partial class LatticeTenantAdminGrpcService
{
    private const string AuditUnimplemented =
        "This cluster does not serve the tenant admin audit trail. Register the tenant "
        + "audit facade (ILatticeTenantAuditLog) to enable it.";

    /// <inheritdoc />
    public override Task<AdminAuditPage> ListTenantAudit(TenantAdminAuditListRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_auditLog, AuditUnimplemented, request, context,
            static (log, req, ct) => log.ListAuditAsync(req.TenantId, req.Query, ct));
}
