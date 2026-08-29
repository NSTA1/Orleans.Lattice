namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// One tenant's cross-tenant grants in both directions, in every lifecycle
/// state: the grants it issued over its own data, and the grants offered to it.
/// <para>
/// The two directions carry different affordances, which is why they stay
/// separate rather than merging into one list: a tenant admin offers and revokes
/// on the <see cref="Issued"/> side, and approves or rejects on the
/// <see cref="Received"/> side. Only the grantee may approve, so an admin of the
/// granting tenant can never approve its own offer.
/// </para>
/// </summary>
public sealed record ExplorerTenantGrants
{
    /// <summary>The empty report, used when no tenant is in scope.</summary>
    public static ExplorerTenantGrants Empty { get; } = new()
    {
        TenantId = string.Empty,
        Issued = Array.Empty<ExplorerTenantGrant>(),
        Received = Array.Empty<ExplorerTenantGrant>(),
    };

    /// <summary>The tenant the report is for.</summary>
    public required string TenantId { get; init; }

    /// <summary>
    /// Grants this tenant offered over its own data, in every state. Never
    /// <see langword="null"/>.
    /// </summary>
    public required IReadOnlyList<ExplorerTenantGrant> Issued { get; init; }

    /// <summary>
    /// Grants other tenants offered to this one, in every state. The pending
    /// entries here are the tenant's approval inbox. Never
    /// <see langword="null"/>.
    /// </summary>
    public required IReadOnlyList<ExplorerTenantGrant> Received { get; init; }
}
