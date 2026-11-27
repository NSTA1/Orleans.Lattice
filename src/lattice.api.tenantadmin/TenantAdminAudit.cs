using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Builds the admin audit requests of the tenant administration facades (#3638):
/// every record is filed under the target tenant's partition (<c>t/{tenant}</c>), so a
/// tenant's administrators can read their own trail and no other tenant's.
/// </summary>
internal static class TenantAdminAudit
{
    /// <summary>A mutation of <paramref name="tenant"/>'s tenant record.</summary>
    /// <param name="action">The action.</param>
    /// <param name="tenant">The target tenant.</param>
    /// <param name="kind">What <paramref name="targetId"/> names.</param>
    /// <param name="targetId">The target object's id.</param>
    /// <param name="detail">Allow-listed scalar detail, or <see langword="null"/>.</param>
    internal static LatticeAdminAuditRequest OnTenantRecord(
        LatticeAdminAuditAction action,
        TenantId tenant,
        LatticeAdminAuditTargetKind kind,
        string? targetId,
        IReadOnlyDictionary<string, string>? detail = null) =>
        new(
            action,
            LatticeAdminAuditRequest.TenantScope(tenant.Value!),
            kind,
            targetId,
            TenantAuditAnchors.Tenant(tenant.Value!),
            detail);

    /// <summary>A mutation of <paramref name="tenant"/> anchored at <paramref name="anchor"/>.</summary>
    /// <param name="action">The action.</param>
    /// <param name="tenant">The target tenant.</param>
    /// <param name="kind">What <paramref name="targetId"/> names.</param>
    /// <param name="targetId">The target object's id.</param>
    /// <param name="anchor">The anchor record the mutation writes last.</param>
    /// <param name="detail">Allow-listed scalar detail, or <see langword="null"/>.</param>
    internal static LatticeAdminAuditRequest Anchored(
        LatticeAdminAuditAction action,
        TenantId tenant,
        LatticeAdminAuditTargetKind kind,
        string? targetId,
        LatticeAdminAuditAnchor? anchor,
        IReadOnlyDictionary<string, string>? detail = null) =>
        new(action, LatticeAdminAuditRequest.TenantScope(tenant.Value!), kind, targetId, anchor, detail);

    /// <summary>
    /// The composed id of <paramref name="tenant"/>'s group <paramref name="name"/>, or
    /// <see langword="null"/> for the reserved default tenant, which has no tenant groups
    /// (the call is refused after authorization, so its record carries no anchor).
    /// </summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The validated local group name.</param>
    internal static string? TenantGroupId(TenantId tenant, string name) =>
        tenant.IsDefault ? null : LatticeTenantGroupId.Compose(tenant, name).Value;

    /// <summary>A read or introspection call on <paramref name="tenant"/>; audited only when refused.</summary>
    /// <param name="tenant">The target tenant.</param>
    internal static LatticeAdminAuditRequest Read(TenantId tenant) =>
        new(LatticeAdminAuditAction.Read, LatticeAdminAuditRequest.TenantScope(tenant.Value!), LatticeAdminAuditTargetKind.Tenant, tenant.Value);
}
