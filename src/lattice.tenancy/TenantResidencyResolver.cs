namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITenantResidencyResolver"/> supplied by the T20
/// per-tenant region-residency feature: the single narrowest seam the T7 tenant
/// gate enforcer and the T16 replicated-apply isolation gate consult to refuse an
/// operation for a tenant that is not online in this serving region. It reads the
/// decision from the in-memory <see cref="TenantResidencySnapshotMaintainer"/>
/// snapshot, so the steady-state answer is a pure synchronous O(1)
/// <see cref="System.Collections.Frozen.FrozenDictionary{TKey,TValue}"/> lookup
/// with no grain hop and no allocation.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IsActive"/> is always <c>true</c> (this resolver is only registered
/// when the residency feature is wired in, displacing the null default), so the
/// gate always consults it. An unconfigured tenant resolves to online (admit-all),
/// preserving pre-residency behaviour; a configured tenant is online only when its
/// local-region status is exactly <see cref="TenantRegionStatus.Online"/>.
/// </para>
/// <para>
/// <b>Fail closed on a stale snapshot (issue #4051).</b> The snapshot is trusted
/// only while it is authoritative
/// (<see cref="TenantResidencySnapshotMaintainer.IsSnapshotAuthoritative"/>) - a
/// silo that has not yet learned of a drain committed through another silo would
/// otherwise keep reporting the tenant online. While it is not, the gates confirm
/// against the tenant's registry record through
/// <see cref="ITenantResidencyConfirmation"/>, and the synchronous
/// <see cref="IsOnlineInServingRegion"/>, which cannot read the registry, reports
/// the tenant as not online.
/// </para>
/// </remarks>
internal sealed class TenantResidencyResolver : ITenantResidencyResolver, ITenantResidencyConfirmation
{
    private readonly TenantResidencySnapshotMaintainer _maintainer;
    private readonly ITenantRegistry _registry;

    /// <summary>Initializes a new <see cref="TenantResidencyResolver"/>.</summary>
    /// <param name="maintainer">The snapshot maintainer whose current snapshot is read on the hot path.</param>
    /// <param name="registry">The tenant registry a non-authoritative answer is confirmed against.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantResidencyResolver(TenantResidencySnapshotMaintainer maintainer, ITenantRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(maintainer);
        ArgumentNullException.ThrowIfNull(registry);
        _maintainer = maintainer;
        _registry = registry;
    }

    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    /// <remarks>
    /// Reports <c>false</c> (not online) while the residency snapshot is not
    /// authoritative, because this synchronous form cannot confirm against the
    /// registry. The gates use <see cref="ITenantResidencyConfirmation"/> instead.
    /// </remarks>
    public bool IsOnlineInServingRegion(TenantId tenant) =>
        TryResolveOnline(tenant, out var online) && online;

    /// <inheritdoc />
    public bool TryResolveOnline(TenantId tenant, out bool online)
    {
        if (!_maintainer.IsSnapshotAuthoritative)
        {
            online = false;
            return false;
        }

        online = _maintainer.Current.IsOnlineLocally(tenant);
        return true;
    }

    /// <inheritdoc />
    public bool TryResolveReplicationAdmissible(TenantId tenant, out bool admissible)
    {
        if (!_maintainer.IsSnapshotAuthoritative)
        {
            admissible = false;
            return false;
        }

        admissible = _maintainer.Current.IsReplicationAdmissibleLocally(tenant);
        return true;
    }

    /// <inheritdoc />
    public bool TryResolveSourceResidency(TenantId tenant, string? regionId, out bool configured, out bool allowed)
    {
        if (!_maintainer.IsSnapshotAuthoritative)
        {
            configured = false;
            allowed = false;
            return false;
        }

        var snapshot = _maintainer.Current;
        configured = snapshot.TryGetStatus(tenant, out _);
        allowed = snapshot.IsReplicationSourceInRegion(tenant, regionId);
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ConfirmOnlineAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        return record is not null && IsOnline(record);
    }

    /// <inheritdoc />
    public bool IsOnline(TenantRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return !record.HasResidencyConfiguration
            || record.GetRegionStatus(_maintainer.LocalRegionId) == TenantRegionStatus.Online;
    }

    /// <inheritdoc />
    public bool IsReplicationAdmissible(TenantRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (!record.HasResidencyConfiguration)
        {
            return true;
        }

        return record.GetRegionStatus(_maintainer.LocalRegionId)
            is TenantRegionStatus.Backfilling or TenantRegionStatus.Online;
    }

    /// <inheritdoc />
    public bool IsReplicationSource(TenantRecord record, string regionId)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        return !record.HasResidencyConfiguration
            || TenantRegionLifecycle.IsReplicationSource(record.GetRegionStatus(regionId));
    }
}
