namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The extent of the keyspace a tenant-tier rule governs. The first three kinds
/// mirror the cluster rule scopes over one of the tenant's own trees, with the
/// same numeric values as <see cref="Orleans.Lattice.Auth.LatticeScopeKind"/>;
/// <see cref="TenantWide"/> is the tenant-bounded analogue of the cluster
/// all-trees scope and covers every tree the tenant owns.
/// </summary>
/// <remarks>
/// <para>
/// A tenant-wide rule never reaches the tenant's app-owned trees, a reserved or
/// system tree, or another tenant's trees, and is authorable only in the
/// <see cref="TenantRuleLayer.Tenant"/> layer. It names no tree, so its
/// <see cref="TenantRuleDraft.TreeName"/> is <see langword="null"/>.
/// </para>
/// <para>
/// The zero value is <see cref="Tree"/>, the most common rule shape.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRuleScopeKind)]
public enum TenantRuleScopeKind
{
    /// <summary>The whole of one of the tenant's own trees.</summary>
    Tree = 0,

    /// <summary>A single key within one of the tenant's own trees.</summary>
    Key = 1,

    /// <summary>Every key beginning with a prefix within one of the tenant's own trees.</summary>
    Prefix = 2,

    /// <summary>Every tree the tenant owns, excluding its app-owned trees.</summary>
    TenantWide = 3,
}
