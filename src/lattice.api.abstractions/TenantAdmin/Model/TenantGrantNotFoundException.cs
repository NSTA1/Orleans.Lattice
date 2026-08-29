namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a cross-tenant grant lifecycle operation names a grant that does
/// not exist: no grant has been offered by that granting tenant to that grantee
/// tenant on that scope, or the granting tenant is not registered at all. The two
/// are deliberately indistinguishable, so the surface can never be used to probe
/// whether a tenant exists. A transport binding surfaces it as a not-found
/// outcome.
/// </summary>
public sealed class TenantGrantNotFoundException : Exception
{
    /// <summary>Initialises the exception for the named grant.</summary>
    /// <param name="granterTenantId">The tenant the grant was expected to have been offered by.</param>
    /// <param name="granteeTenantId">The tenant the grant was expected to have been offered to.</param>
    /// <param name="scope">The scope the grant was expected to cover.</param>
    public TenantGrantNotFoundException(string granterTenantId, string granteeTenantId, string scope)
        : base($"Tenant '{granterTenantId}' has no cross-tenant grant to tenant '{granteeTenantId}' "
            + $"covering scope '{scope}'.")
    {
        GranterTenantId = granterTenantId;
        GranteeTenantId = granteeTenantId;
        Scope = scope;
    }

    /// <summary>The tenant the grant was expected to have been offered by.</summary>
    public string GranterTenantId { get; }

    /// <summary>The tenant the grant was expected to have been offered to.</summary>
    public string GranteeTenantId { get; }

    /// <summary>The scope the grant was expected to cover.</summary>
    public string Scope { get; }
}
