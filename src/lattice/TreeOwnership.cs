namespace Orleans.Lattice;

/// <summary>
/// The derived ownership of a Lattice tree, computed from its tree id alone by
/// <see cref="LatticeTenantTrees.GetOwner"/>. Ownership is never stored - the
/// tree registry keeps no tenant column - so it is always re-derived from the
/// id's structural prefix.
/// </summary>
/// <remarks>
/// <para>
/// A tree is either <em>tenant-owned</em> or <em>platform-owned</em>:
/// </para>
/// <list type="bullet">
/// <item>
/// A tenant-scoped id (<c>t/{tenantId}/{name}</c>) is owned by that
/// <see cref="TenantId"/>; a bare, unsegmented legacy id is adopted by the
/// reserved <see cref="TenantId.Default"/> tenant so an existing cluster that
/// opts in to tenancy preserves all of its pre-tenancy trees. Both are
/// <see cref="IsTenantOwned"/>.
/// </item>
/// <item>
/// A system id (the <c>_lattice_</c> system-internal namespace or the
/// <c>sys-</c> system-data namespace) is <see cref="IsPlatformOwned"/>: it sits
/// outside every tenant namespace and belongs to the platform, distinct from
/// the <see cref="TenantId.Default"/> tenant. Its <see cref="Tenant"/> is the
/// uninitialised <c>default(TenantId)</c> ("no tenant").
/// </item>
/// </list>
/// </remarks>
public readonly record struct TreeOwnership
{
    private TreeOwnership(bool isTenantOwned, TenantId tenant)
    {
        IsTenantOwned = isTenantOwned;
        Tenant = tenant;
    }

    /// <summary>
    /// <c>true</c> when the tree is owned by a tenant (a tenant-scoped
    /// <c>t/{tenantId}/{name}</c> id, or a bare legacy id adopted by the
    /// reserved <see cref="TenantId.Default"/> tenant); <c>false</c> for a
    /// platform-owned system tree.
    /// </summary>
    public bool IsTenantOwned { get; }

    /// <summary>
    /// <c>true</c> when the tree is platform-owned (a <c>_lattice_</c> or
    /// <c>sys-</c> system tree) and therefore outside every tenant namespace;
    /// the inverse of <see cref="IsTenantOwned"/>.
    /// </summary>
    public bool IsPlatformOwned => !IsTenantOwned;

    /// <summary>
    /// The owning tenant when <see cref="IsTenantOwned"/> is <c>true</c>;
    /// otherwise the uninitialised <c>default(TenantId)</c> ("no tenant") for a
    /// platform-owned tree.
    /// </summary>
    public TenantId Tenant { get; }

    /// <summary>
    /// The platform-owned ownership result for a system tree that belongs to no
    /// tenant.
    /// </summary>
    public static TreeOwnership Platform { get; } = new(isTenantOwned: false, tenant: default);

    /// <summary>
    /// Creates a tenant-owned ownership result for the given <paramref name="tenant"/>.
    /// </summary>
    /// <param name="tenant">The owning tenant. Must be an initialised (parsed) tenant id.</param>
    /// <returns>A tenant-owned <see cref="TreeOwnership"/>.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>
    /// ("no tenant").
    /// </exception>
    public static TreeOwnership ForTenant(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "Cannot build a tenant-owned result from the uninitialised 'no tenant' value.",
                nameof(tenant));
        }

        return new TreeOwnership(isTenantOwned: true, tenant: tenant);
    }
}
