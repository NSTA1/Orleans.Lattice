namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ITenantRegionVisibilityResolver"/>: reports itself
/// inactive and resolves nothing. Registered by <c>AddLattice</c> as the safe
/// default so a consumer of the seam always resolves an instance even when the
/// tenancy add-on is not registered. Because <see cref="IsActive"/> is always
/// <c>false</c>, a region-discovery choke point never calls
/// <see cref="ResolveAsync"/>, so an unregistered tenancy engine adds no cost and
/// the advertised region list is byte-for-byte unchanged. The tenancy package
/// replaces it with a real, registry-backed resolver.
/// </summary>
internal sealed class NullTenantRegionVisibilityResolver : ITenantRegionVisibilityResolver
{
    private static readonly ValueTask<TenantRegionVisibilityMap> UnresolvedResult =
        new(TenantRegionVisibilityMap.Unresolved);

    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public ValueTask<TenantRegionVisibilityMap> ResolveAsync(
        TenantId tenant, CancellationToken cancellationToken = default) => UnresolvedResult;
}
