namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ITenantEnumerationFilter"/>: returns every tree-id
/// enumeration unchanged and reports itself inactive. Registered by
/// <c>AddLattice</c> as the safe default so a consumer of the seam always
/// resolves an instance even when the tenancy add-on is not registered. Because
/// <see cref="IsActive"/> is always <c>false</c>, an enumeration choke point
/// caches the inactive flag and never calls <see cref="Filter"/>, so an
/// unregistered filter adds no cost and the enumeration is byte-for-byte
/// unchanged. The tenancy package replaces it with a real, tenant-pruning
/// filter.
/// </summary>
internal sealed class NullTenantEnumerationFilter : ITenantEnumerationFilter
{
    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public IReadOnlyList<string> Filter(TenantId tenant, IReadOnlyList<string> treeIds) => treeIds;
}
