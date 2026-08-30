namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The lifecycle state of a cross-tenant grant. The state is load-bearing, not
/// decoration: only <see cref="Active"/> authorizes anything, so a surface must
/// carry it explicitly and can never present a <see cref="Pending"/> offer as
/// live access.
/// </summary>
public enum ExplorerTenantGrantState
{
    /// <summary>
    /// The grantee approved the offer and the grant authorizes the operations
    /// it names. This is the only state that grants access.
    /// </summary>
    Active = 0,

    /// <summary>
    /// The granting tenant offered the grant and the grantee has not answered.
    /// It authorizes nothing.
    /// </summary>
    Pending = 1,

    /// <summary>The grantee declined the offer. Terminal, and authorizes nothing.</summary>
    Rejected = 2,

    /// <summary>Either party withdrew the grant. Terminal, and authorizes nothing.</summary>
    Revoked = 3,
}
