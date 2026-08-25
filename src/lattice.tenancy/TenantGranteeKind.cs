namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The kind of principal a <see cref="CrossTenantGrant"/> is issued to: an
/// individual caller subject, or an entire tenant (every subject scoped to it).
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantGranteeKind)]
public enum TenantGranteeKind
{
    /// <summary>The grant is issued to a single caller subject id.</summary>
    Subject = 0,

    /// <summary>The grant is issued to a whole tenant (all of its subjects).</summary>
    Tenant = 1,
}
