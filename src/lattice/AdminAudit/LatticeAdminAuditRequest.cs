namespace Orleans.Lattice;

/// <summary>
/// What an administration facade tells the admin audit trail about one call: the
/// action, its target, the anchor record it writes, and any allow-listed scalar
/// detail. The actor is never part of the request; the trail resolves it from the
/// authenticated caller context.
/// </summary>
/// <param name="Action">The administrative action.</param>
/// <param name="TargetScope">The partition the record is filed under (<see cref="ClusterScope"/> or <see cref="TenantScope(string)"/>).</param>
/// <param name="TargetKind">What kind of object <paramref name="TargetId"/> names.</param>
/// <param name="TargetId">The target object's identifier, or <see langword="null"/>.</param>
/// <param name="Anchor">The anchor record the action writes, or <see langword="null"/> for a call that writes nothing.</param>
/// <param name="Detail">Allow-listed, per-action scalar parameters, or <see langword="null"/>.</param>
/// <param name="ActorTenant">The validated active tenant the call acts as, or <see langword="null"/>.</param>
internal sealed record LatticeAdminAuditRequest(
    LatticeAdminAuditAction Action,
    string TargetScope,
    LatticeAdminAuditTargetKind TargetKind,
    string? TargetId,
    LatticeAdminAuditAnchor? Anchor = null,
    IReadOnlyDictionary<string, string>? Detail = null,
    string? ActorTenant = null)
{
    /// <summary>The partition for cluster-tier administration.</summary>
    public const string ClusterScope = "cluster";

    /// <summary>The partition prefix for a tenant's own administration.</summary>
    public const string TenantScopePrefix = "t/";

    /// <summary>Returns the partition for <paramref name="tenant"/>'s own administration.</summary>
    /// <param name="tenant">The tenant id text.</param>
    public static string TenantScope(string tenant) => string.Concat(TenantScopePrefix, tenant);

    /// <summary>Returns <see cref="TenantScope(string)"/> for a tenant, or <see cref="ClusterScope"/> when there is none.</summary>
    /// <param name="tenant">The tenant id text, or <see langword="null"/>.</param>
    public static string ScopeFor(string? tenant) => tenant is null ? ClusterScope : TenantScope(tenant);
}
