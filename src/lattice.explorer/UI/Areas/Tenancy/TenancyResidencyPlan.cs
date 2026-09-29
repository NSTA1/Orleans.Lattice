using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// A tenant's residency being edited: the committed set as the cluster reported
/// it, the set the caller is composing, and the two invariants the cluster
/// enforces, applied at the control so they are legible before anything is
/// sent. Residency stays inside the allowed set, and a tenant stays resident in
/// at least one region. The cluster still enforces both.
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

    /// <summary>Replaces the plan with the cluster's reading, discarding any edit.</summary>
    /// <param name="regions">The per-region status.</param>
    public void Reset(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);
        _regions.Clear();
        _regions.AddRange(regions);
        Revert();
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
            return _planned.Count <= 1 ? LastRegionRefusal : null;
        }

        return region.IsAllowed ? null : NotAllowedRefusal;
    }

    private void Project()
    {
        var changed = false;
        var rows = new List<TenancyRegionRow>(_regions.Count);
        foreach (var region in _regions)
        {
            var resident = TenancyFormat.IsResident(region.Status);
            var planned = _planned.Contains(region.RegionId);
            changed |= resident != planned;
            rows.Add(new TenancyRegionRow(region.RegionId, region.Status, region.IsAllowed, resident, planned, RefusalFor(region)));
        }

        Rows = rows;
        IsChanged = changed;
    }
}
