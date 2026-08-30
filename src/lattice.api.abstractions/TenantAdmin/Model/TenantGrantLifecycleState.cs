namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The transport-agnostic lifecycle state of a cross-tenant grant as reported by
/// the tenant-administration control facade. It mirrors the tenancy engine's own
/// grant-state enum without taking a dependency on the tenancy add-on, so the
/// shared contract package stays free of the engine's internals: the facade maps
/// between this enum and the engine state at the single implementation seam.
/// </summary>
/// <remarks>
/// A grant is a two-step agreement between two tenants. The granting tenant
/// offers it (<see cref="Pending"/>, which authorizes nothing) and the grantee
/// tenant either approves it (<see cref="Active"/>, the only state that
/// authorizes anything) or declines it (<see cref="Rejected"/>). Either party may
/// later withdraw an active grant (<see cref="Revoked"/>).
/// <see cref="Rejected"/> and <see cref="Revoked"/> are terminal; a further
/// agreement on the same scope requires a fresh offer.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGrantLifecycleState)]
public enum TenantGrantLifecycleState
{
    /// <summary>The grant is in force and authorizes its operations on its scope.</summary>
    Active = 0,

    /// <summary>The grant has been offered and the grantee has not yet answered. Authorizes nothing.</summary>
    Pending = 1,

    /// <summary>Terminal: the grantee declined the offer. Authorizes nothing.</summary>
    Rejected = 2,

    /// <summary>Terminal: one of the two parties withdrew a previously active grant. Authorizes nothing.</summary>
    Revoked = 3,
}
