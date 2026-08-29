namespace Orleans.Lattice.Explorer.MyTenant;

/// <summary>
/// Why a residency edit is refused before it is ever sent, so the surface can
/// say what is wrong at the moment the caller reaches for the control rather
/// than after a round trip.
/// <para>
/// These mirror the cluster's own refusals, which remain the enforcement point:
/// the client copy exists to make the two-set model legible, not to replace the
/// server's decision.
/// </para>
/// </summary>
public enum TenantResidencyRefusal
{
    /// <summary>The edit is permitted as far as the client can tell.</summary>
    None = 0,

    /// <summary>
    /// The region is not in the tenant's operator-authorized allowed set.
    /// Residency must stay a subset of that set, and only a platform operator
    /// can widen it, so a tenant admin cannot resolve this themselves.
    /// </summary>
    NotAllowed = 1,

    /// <summary>
    /// The edit would remove the tenant's last planned resident region. A tenant
    /// must remain resident somewhere, so another region has to be added first.
    /// </summary>
    LastRegion = 2,

    /// <summary>
    /// The region is not in the allowed set and cannot be planned for, but the
    /// tenant is nonetheless still resident in it - the allowed set was narrowed
    /// underneath a live residency. Only removing it is possible from here.
    /// </summary>
    ResidentButNoLongerAllowed = 3,
}
