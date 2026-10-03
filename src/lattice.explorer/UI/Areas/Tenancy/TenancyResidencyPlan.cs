using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// A tenant's residency being edited: the committed set as the cluster reported
/// it, the set the caller is composing, and the two invariants the cluster
/// enforces, applied at the control so they are legible before anything is
/// sent. Residency stays inside the allowed set, and a tenant resident in a
/// region now stays resident in at least one. A tenant resident nowhere (no
/// residency set, or every region gone) may plan none, as the cluster accepts an
/// empty set for it. The cluster still enforces both.
/// </summary>
internal sealed class TenancyResidencyPlan
{
    /// <summary>Why the last planned region cannot be removed.</summary>
    public const string LastRegionRefusal = "A tenant stays resident in at least one region.";

    /// <summary>Why a region outside the allowed set cannot be added.</summary>
    public const string NotAllowedRefusal = "Not allowed for this tenant. A platform operator must allow it first.";

    private readonly List<TenantRegionStatusDescriptor> _regions = [];
    private readonly HashSet<string> _planned = new(StringComparer.Ordinal);

    /// <summary>The rows, in the order the cluster reported the regions.</summary>
    public IReadOnlyList<TenancyRegionRow> Rows { get; private set; } = [];

    /// <summary>Whether the plan differs from the committed residency.</summary>
    public bool IsChanged { get; private set; }

    /// <summary>The regions the plan keeps or adds, in the cluster's order.</summary>
    public IReadOnlyList<string> Planned => [.. _regions.Where(region => _planned.Contains(region.RegionId)).Select(region => region.RegionId)];

    /// <summary>The regions the plan adds to the committed residency, in the cluster's order.</summary>
    public IReadOnlyList<string> Added => [.. Rows.Where(row => row.IsPlanned && !row.IsResident).Select(row => row.RegionId)];

    /// <summary>The regions the plan removes from the committed residency, in the cluster's order.</summary>
    public IReadOnlyList<string> Removed => [.. Rows.Where(row => !row.IsPlanned && row.IsResident).Select(row => row.RegionId)];

    /// <summary>Whether the tenant has residency set; with none set the tenant is served in every region.</summary>
    public bool HasResidency => _regions.Any(region => region.Status != TenantRegionLifecycleStatus.None);

    /// <summary>
    /// Whether the committed residency holds any region (Provisioning, Backfilling or
    /// Online), as the cluster counts a resident region. Only then is emptying the
    /// residency refused.
    /// </summary>
    public bool IsResidentNow => _regions.Any(region => TenancyFormat.IsResident(region.Status));

    /// <summary>Whether any of the tenant's regions is Online now, so a plan can keep serving it by keeping that region.</summary>
    public bool HasOnlineRegion => _regions.Any(region => region.Status == TenantRegionLifecycleStatus.Online);

    /// <summary>
    /// Whether applying the plan would leave the tenant with residency and no
    /// Online region. A region the plan adds starts Provisioning and stays there
    /// until an operator of the hosting deployment promotes it, and a tenant with
    /// any residency is served only in Online regions, so such a plan stops (or
    /// keeps stopped) serving the tenant.
    /// </summary>
    public bool LeavesNoOnlineRegion =>
        _planned.Count > 0
        && !_regions.Any(region => _planned.Contains(region.RegionId) && region.Status == TenantRegionLifecycleStatus.Online);

    /// <summary>
    /// Whether applying the plan stops serving a tenant that is served now: it
    /// <see cref="LeavesNoOnlineRegion"/>, and today the tenant either has no
    /// residency (so every region serves it) or has an Online region. A tenant
    /// that is already served nowhere loses nothing by the change, so applying it
    /// is not a service-stopping action.
    /// </summary>
    public bool StopsServing => LeavesNoOnlineRegion && (!HasResidency || HasOnlineRegion);

    /// <summary>
    /// What applying the plan changes for each region, one sentence each, in the
    /// cluster's order: where the tenant stays served, joins or leaves the
    /// residency, and stops being served. Empty while the plan is unchanged.
    /// </summary>
    public IReadOnlyList<string> Preview { get; private set; } = [];

    /// <summary>
    /// The planned regions that would not be Online once the plan is applied,
    /// each with its state then, such as <c>east (added: starts Provisioning)</c>.
    /// </summary>
    public IReadOnlyList<string> NotOnlineAfter { get; private set; } = [];

    /// <summary>Replaces the plan with the cluster's reading, discarding any edit.</summary>
    /// <param name="regions">The per-region status.</param>
    public void Reset(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);
        _regions.Clear();
        _regions.AddRange(regions);
        Revert();
    }

    /// <summary>
    /// Takes a newer reading of the same tenant's regions, as a page following a
    /// change reads it, without discarding an edit in progress: an unchanged plan
    /// follows the reading, and a changed one keeps the regions it plans that
    /// still exist.
    /// </summary>
    /// <param name="regions">The per-region status.</param>
    public void Update(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);
        if (!IsChanged)
        {
            Reset(regions);
            return;
        }

        _regions.Clear();
        _regions.AddRange(regions);
        _planned.RemoveWhere(id => !_regions.Any(region => string.Equals(region.RegionId, id, StringComparison.Ordinal)));
        Project();
    }

    /// <summary>Returns the plan to the committed residency.</summary>
    public void Revert()
    {
        _planned.Clear();
        foreach (var region in _regions.Where(region => TenancyFormat.IsResident(region.Status)))
        {
            _planned.Add(region.RegionId);
        }

        Project();
    }

    /// <summary>
    /// Adds or removes <paramref name="regionId"/> from the plan when the
    /// invariants allow it.
    /// </summary>
    /// <param name="regionId">The region id.</param>
    /// <returns><see langword="null"/> when the plan changed, or the refusal that left it unchanged.</returns>
    public string? Toggle(string regionId)
    {
        ArgumentNullException.ThrowIfNull(regionId);
        var region = _regions.FirstOrDefault(candidate => string.Equals(candidate.RegionId, regionId, StringComparison.Ordinal));
        if (region is null)
        {
            return NotAllowedRefusal;
        }

        if (RefusalFor(region) is { } refusal)
        {
            return refusal;
        }

        if (!_planned.Remove(regionId))
        {
            _planned.Add(regionId);
        }

        Project();
        return null;
    }

    private string? RefusalFor(TenantRegionStatusDescriptor region)
    {
        if (_planned.Contains(region.RegionId))
        {
            // The cluster refuses an empty residency only while the tenant is resident
            // in some region, so the last planned region is held only then (#4412).
            return _planned.Count <= 1 && IsResidentNow ? LastRegionRefusal : null;
        }

        return region.IsAllowed ? null : NotAllowedRefusal;
    }

    private void Project()
    {
        var changed = false;
        var hasResidency = HasResidency;
        var rows = new List<TenancyRegionRow>(_regions.Count);
        foreach (var region in _regions)
        {
            var resident = TenancyFormat.IsResident(region.Status);
            var planned = _planned.Contains(region.RegionId);
            changed |= resident != planned;
            rows.Add(new TenancyRegionRow(
                region.RegionId, region.Status, region.IsAllowed, resident, planned, RefusalFor(region),
                TenancyFormat.IsServedIn(region.Status, hasResidency)));
        }

        Rows = rows;
        IsChanged = changed;
        Preview = changed ? [.. rows.Select(Describe).OfType<string>()] : [];
        NotOnlineAfter = changed
            ? [.. rows.Where(row => row.IsPlanned && !ServedAfter(row)).Select(row => row.IsResident
                ? $"{row.RegionId} ({TenancyFormat.RegionStatusLabel(row.Status)})"
                : $"{row.RegionId} (added: starts Provisioning)")]
            : [];
    }

    // Once the plan is applied the tenant has residency, so a region serves it only
    // where it is Online: a kept region keeps its status, an added one starts
    // Provisioning, and a removed one starts Draining.
    private static bool ServedAfter(TenancyRegionRow row) =>
        row.IsPlanned && row.IsResident && row.Status == TenantRegionLifecycleStatus.Online;

    private static string? Describe(TenancyRegionRow row)
    {
        var region = row.RegionId;
        var servedAfter = ServedAfter(row);
        if (row.IsPlanned && !row.IsResident)
        {
            return row.IsServed
                ? $"{region} joins the residency as Provisioning, and stops being served there until a platform operator promotes it to Online."
                : $"{region} joins the residency as Provisioning; it is served there once a platform operator promotes it to Online.";
        }

        if (!row.IsPlanned && row.IsResident)
        {
            return row.IsServed ? $"{region} starts draining, and stops being served there." : $"{region} starts draining.";
        }

        if (row.IsPlanned)
        {
            return servedAfter
                ? $"{region} stays in the residency, and is still served there."
                : $"{region} stays in the residency, and is not served there until it is Online.";
        }

        return row.IsServed ? $"{region} stops being served, because it is not in the residency." : null;
    }
}
