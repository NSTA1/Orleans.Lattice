namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The residency / online resolver seam the tenant gate enforcer consults to
/// decide whether an active tenant is currently <b>online in this serving
/// region</b>. It is a nested null-default seam: the tenancy add-on ships only
/// the allow-everything <c>NullTenantResidencyResolver</c>, so residency is a
/// no-op until a residency feature supplies a real resolver and replaces the
/// default. When absent, an active tenant is always treated as online, so
/// enforcement never denies on residency grounds.
/// </summary>
/// <remarks>
/// The gate enforcer reads <see cref="IsActive"/> first and skips
/// <see cref="IsOnlineInServingRegion"/> entirely when it is <c>false</c>, so
/// the residency-absent path is a single bool read that adds no allocation to
/// the enforcement hot path.
/// </remarks>
public interface ITenantResidencyResolver
{
    /// <summary>
    /// <c>true</c> when a residency feature is wired in (a real resolver replaced
    /// the null default); <c>false</c> for the null default. The enforcer reads
    /// this first and consults <see cref="IsOnlineInServingRegion"/> only when it
    /// is <c>true</c>.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="tenant"/> is online in the region
    /// serving this request, so the request may proceed; <c>false</c> when the
    /// tenant is offline here and the request must be denied. The null default
    /// always returns <c>true</c>.
    /// </summary>
    /// <param name="tenant">The active tenant being validated.</param>
    /// <returns><c>true</c> when the tenant is online in this serving region; otherwise <c>false</c>.</returns>
    bool IsOnlineInServingRegion(TenantId tenant);
}
