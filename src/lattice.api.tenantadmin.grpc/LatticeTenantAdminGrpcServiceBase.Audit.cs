using Grpc.Core;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// The read-only tenant admin audit trail RPC of the tenant-administration control-API
/// service, over <see cref="ILatticeTenantAuditLog"/>. It is an
/// <b>operator-or-tenant-admin</b> read enforced fail-closed by its facade, and stays
/// behind the transport authorization interceptor.
/// </summary>
internal abstract partial class LatticeTenantAdminGrpcServiceBase
{
    /// <summary>Reads one page of a tenant's admin audit trail. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<AdminAuditPage> ListTenantAudit(TenantAdminAuditListRequest request, ServerCallContext context);

    /// <summary>
    /// Records the metadata of the audit RPC on the metadata-only binding pass
    /// <see cref="BindService"/> makes with no service instance.
    /// </summary>
    private static void BindAuditMetadata(ServiceBinderBase binder, LatticeTenantAdminGrpcMethods methods)
        => binder.AddMethod(methods.ListTenantAudit, (UnaryServerMethod<TenantAdminAuditListRequest, AdminAuditPage>?)null);

    /// <summary>Binds the audit RPC to <paramref name="serviceImpl"/>.</summary>
    private static void BindAudit(
        ServiceBinderBase binder, LatticeTenantAdminGrpcMethods methods, LatticeTenantAdminGrpcServiceBase serviceImpl)
        => binder.AddMethod(methods.ListTenantAudit, new UnaryServerMethod<TenantAdminAuditListRequest, AdminAuditPage>(serviceImpl.ListTenantAudit));
}
