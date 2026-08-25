namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default no-op <see cref="ITenantResidencyResolver"/>: the null seam the
/// tenancy add-on ships until a residency feature supplies a real resolver.
/// <see cref="IsActive"/> is <c>false</c> and <see cref="IsOnlineInServingRegion"/>
/// always returns <c>true</c>, so an active tenant is always treated as online
/// and enforcement never denies on residency grounds. Registered via
/// <c>TryAddSingleton</c> so a residency feature can displace it with
/// <c>Replace</c>.
/// </summary>
internal sealed class NullTenantResidencyResolver : ITenantResidencyResolver
{
    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public bool IsOnlineInServingRegion(TenantId tenant) => true;
}
