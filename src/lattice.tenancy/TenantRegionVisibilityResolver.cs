namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITenantRegionVisibilityResolver"/>: reports a tenant's
/// operator-authorized <b>allowed</b> region set and its per-region residency
/// status, read from the durable <see cref="ITenantRegistry"/>. Replaces the core
/// <c>NullTenantRegionVisibilityResolver</c> when the tenancy add-on is
/// registered, so a region-discovery surface can prune the routing topology it
/// advertises to a tenant caller down to the actionable set and annotate what
/// remains.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed.</b> Any outcome that leaves the tenant's standing unknown - an
/// uninitialised "no tenant" id, an unregistered tenant, or a registry read that
/// faults - resolves to <see cref="TenantRegionVisibilityMap.Unresolved"/>, never
/// to an empty resolved map and never to a permissive one, so the caller degrades
/// to the tenant-scoped minimal answer rather than disclosing the full topology.
/// A cancellation is deliberately <b>not</b> swallowed: it is the caller's own
/// signal, not a resolution failure.
/// </para>
/// <para>
/// <b>Read path.</b> This is a discovery-time resolver, not a data-plane hot path:
/// it is consulted once per <c>lattice_list_regions</c> style enumeration, so it
/// reads the registry directly rather than maintaining a snapshot. The
/// enforcement-time question ("is this tenant online in the serving region?") is
/// answered by the separate, snapshot-backed
/// <see cref="ITenantResidencyResolver"/>, which stays an O(1) in-memory lookup.
/// </para>
/// </remarks>
internal sealed class TenantRegionVisibilityResolver : ITenantRegionVisibilityResolver
{
    private readonly ITenantRegistry _registry;

    /// <summary>Initialises the resolver over the tenancy engine's registry.</summary>
    /// <param name="registry">The durable tenant registry. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="registry"/> is <c>null</c>.</exception>
    public TenantRegionVisibilityResolver(ITenantRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        _registry = registry;
    }

    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public async ValueTask<TenantRegionVisibilityMap> ResolveAsync(
        TenantId tenant, CancellationToken cancellationToken = default)
    {
        // The uninitialised "no tenant" value names nothing, so its standing can
        // never be established: fail closed rather than reading the registry.
        if (tenant.Value is null)
        {
            return TenantRegionVisibilityMap.Unresolved;
        }

        TenantRecord? record;
        try
        {
            record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The tenant's standing could not be established. Fail closed.
            return TenantRegionVisibilityMap.Unresolved;
        }

        if (record is null)
        {
            return TenantRegionVisibilityMap.Unresolved;
        }

        return Project(record);
    }

    /// <summary>
    /// Projects a tenant record onto the core seam's per-region map: one entry per
    /// region the tenant is allowed into or carries a status for, matching the
    /// union the control facade's per-region status report builds.
    /// </summary>
    /// <param name="record">The tenant record to project. Must not be <c>null</c>.</param>
    /// <returns>The resolved per-region standing of the tenant.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is <c>null</c>.</exception>
    internal static TenantRegionVisibilityMap Project(TenantRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        var allowed = record.AllowedRegionIds;
        var statuses = record.RegionStatusEntries;

        var regions = new Dictionary<string, TenantRegionVisibility>(
            allowed.Count + statuses.Count, StringComparer.Ordinal);

        for (var i = 0; i < allowed.Count; i++)
        {
            var regionId = allowed[i];
            regions[regionId] = new TenantRegionVisibility(
                isAllowed: true, Map(record.GetRegionStatus(regionId)));
        }

        for (var i = 0; i < statuses.Count; i++)
        {
            var entry = statuses[i];
            regions[entry.Key] = new TenantRegionVisibility(
                record.IsRegionAllowed(entry.Key), Map(entry.Value));
        }

        return TenantRegionVisibilityMap.Create(regions);
    }

    private static TenantRegionResidencyStatus Map(TenantRegionStatus status) => status switch
    {
        TenantRegionStatus.Provisioning => TenantRegionResidencyStatus.Provisioning,
        TenantRegionStatus.Backfilling => TenantRegionResidencyStatus.Backfilling,
        TenantRegionStatus.Online => TenantRegionResidencyStatus.Online,
        TenantRegionStatus.Draining => TenantRegionResidencyStatus.Draining,
        TenantRegionStatus.Offline => TenantRegionResidencyStatus.Offline,
        TenantRegionStatus.Removed => TenantRegionResidencyStatus.Removed,
        _ => TenantRegionResidencyStatus.None,
    };
}
