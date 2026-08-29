using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// Which side of a cross-tenant grant the tenant in view is on. The two
/// directions carry different affordances, so a row states its direction rather
/// than leaving an operator to infer it from the tenant ids.
/// </summary>
public enum TenantGrantDirection
{
    /// <summary>
    /// The tenant in view offered this grant over its own data. It may withdraw
    /// an active one, but it may never approve its own offer.
    /// </summary>
    Issued = 0,

    /// <summary>
    /// Another tenant offered this grant to the tenant in view. A pending entry
    /// here is its approval inbox.
    /// </summary>
    Received = 1,
}
