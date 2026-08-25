namespace Orleans.Lattice;

/// <summary>
/// The seam that filters a tree-id enumeration down to the trees a tenant may
/// observe (for example the cluster-state tree catalog). A registered filter
/// prunes the enumeration to the tenant's own trees; the core library ships only
/// the <see cref="NullTenantEnumerationFilter"/> (which returns the enumeration
/// unchanged and reports itself inactive, so an enumeration choke point pays
/// nothing when tenancy is off), and the real, tenant-pruning implementation is
/// contributed by the tenancy package.
/// </summary>
public interface ITenantEnumerationFilter
{
    /// <summary>
    /// Returns <c>true</c> when this filter wants to prune enumerations. Gates
    /// the seam so a cluster with no tenancy add-on never calls
    /// <see cref="Filter"/>. The result is expected to be cheap and stable so a
    /// choke point can cache it. The null default is always <c>false</c>.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Returns the subset of <paramref name="treeIds"/> that
    /// <paramref name="tenant"/> may observe. Invoked only when
    /// <see cref="IsActive"/> is <c>true</c>. Implementations must not mutate the
    /// supplied list.
    /// </summary>
    /// <param name="tenant">The tenant whose visible trees are being resolved.</param>
    /// <param name="treeIds">The full set of candidate tree ids. Must not be <c>null</c>.</param>
    /// <returns>The tenant-visible subset of <paramref name="treeIds"/>.</returns>
    IReadOnlyList<string> Filter(TenantId tenant, IReadOnlyList<string> treeIds);
}
