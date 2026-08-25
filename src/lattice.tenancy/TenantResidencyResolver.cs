namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITenantResidencyResolver"/> supplied by the T20
/// per-tenant region-residency feature: the single narrowest seam the T7 tenant
/// gate enforcer and the T16 replicated-apply isolation gate consult to refuse an
/// operation for a tenant that is not online in this serving region. It reads the
/// decision from the in-memory <see cref="TenantResidencySnapshotMaintainer"/>
/// snapshot, so the answer is a pure synchronous O(1)
/// <see cref="System.Collections.Frozen.FrozenDictionary{TKey,TValue}"/> lookup
/// with no grain hop and no allocation on the hot path.
/// </summary>
/// <remarks>
/// <see cref="IsActive"/> is always <c>true</c> (this resolver is only registered
/// when the residency feature is wired in, displacing the null default), so the
/// gate always consults <see cref="IsOnlineInServingRegion"/>. An unconfigured
/// tenant resolves to online (admit-all), preserving pre-residency behaviour; a
/// configured tenant is online only when its local-region status is exactly
/// <see cref="TenantRegionStatus.Online"/>.
/// </remarks>
internal sealed class TenantResidencyResolver : ITenantResidencyResolver
{
    private readonly TenantResidencySnapshotMaintainer _maintainer;

    /// <summary>Initializes a new <see cref="TenantResidencyResolver"/>.</summary>
    /// <param name="maintainer">The snapshot maintainer whose current snapshot is read on the hot path.</param>
    /// <exception cref="ArgumentNullException"><paramref name="maintainer"/> is <c>null</c>.</exception>
    public TenantResidencyResolver(TenantResidencySnapshotMaintainer maintainer)
    {
        ArgumentNullException.ThrowIfNull(maintainer);
        _maintainer = maintainer;
    }

    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public bool IsOnlineInServingRegion(TenantId tenant) =>
        _maintainer.Current.IsOnlineLocally(tenant);
}
