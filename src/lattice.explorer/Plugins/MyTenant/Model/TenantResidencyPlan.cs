using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The editable residency plan for one tenant: the per-region rows the surface
/// renders, the pending residency set the caller is composing, and the two
/// invariants the plugin enforces client-side <em>before</em> the cluster is
/// asked.
/// <para>
/// Those invariants are the whole point of the type. Residency is a subset of
/// the operator-authorized allowed set, and a tenant must stay resident
/// somewhere - so a region outside the allowed set never offers an add control,
/// and the last planned resident region never offers a remove control. The
/// cluster still refuses both (<see cref="TenantOperationStatus.RegionNotAllowed"/>
/// and <see cref="TenantOperationStatus.LastRegion"/>) and remains the
/// enforcement point; this makes the model legible at the control rather than
/// only in the error that follows one.
/// </para>
/// </summary>
/// <remarks>
/// The plan holds one exactly-sized row array and one planned-residency set,
/// re-sized only when a reload changes the region count, so a toggle re-projects
/// into the buffers it already has and <see cref="Rows"/> is handed out as the
/// array itself - no per-render wrapper, no boxing.
/// </remarks>
public sealed class TenantResidencyPlan
{
    private readonly HashSet<string> _planned = new(StringComparer.Ordinal);
    private readonly List<ExplorerTenantRegion> _regions = [];

    private TenantRegionRow[] _rows = [];
    private int _plannedResidentCount;
    private int _committedResidentCount;

    /// <summary>
    /// The rows in the order the cluster reported them, which is ascending by
    /// region id. Empty until <see cref="Reset"/> supplies a reading.
    /// </summary>
    public IReadOnlyList<TenantRegionRow> Rows => _rows;

    /// <summary>The number of regions the caller's plan keeps a residency in.</summary>
    public int PlannedResidentCount => _plannedResidentCount;

    /// <summary>The number of regions the cluster currently reports a residency in.</summary>
    public int ResidentCount => _committedResidentCount;

    /// <summary>The number of regions in the operator-authorized allowed set.</summary>
    public int AllowedCount { get; private set; }

    /// <summary>
    /// Whether the plan differs from the residency the cluster currently holds,
    /// so the surface knows whether there is anything to apply or reset.
    /// </summary>
    public bool IsChanged { get; private set; }

    /// <summary>
    /// Replaces the plan with the cluster's reading, discarding any pending
    /// edit. The planned residency starts equal to the committed residency, so a
    /// freshly loaded plan is never dirty.
    /// </summary>
    /// <param name="regions">The per-region rows the cluster reported. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="regions"/> is <see langword="null"/>.</exception>
    public void Reset(IReadOnlyList<ExplorerTenantRegion> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);

        _regions.Clear();
        _planned.Clear();
        _committedResidentCount = 0;
        AllowedCount = 0;

        for (var i = 0; i < regions.Count; i++)
        {
            var region = regions[i];
            _regions.Add(region);

            if (region.IsAllowed)
            {
                AllowedCount++;
            }

            if (!region.IsResident)
            {
                continue;
            }

            _committedResidentCount++;
            _planned.Add(region.RegionId);
        }

        Project();
    }

    /// <summary>
    /// Reads the refusal that would block toggling <paramref name="regionId"/>,
    /// without changing the plan.
    /// </summary>
    /// <param name="regionId">The region to test. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// The refusal, or <see cref="TenantResidencyRefusal.None"/> when the toggle
    /// is permitted. A region the plan does not know reports
    /// <see cref="TenantResidencyRefusal.NotAllowed"/>, because it is by
    /// definition outside the allowed set the cluster reported.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="regionId"/> is <see langword="null"/>.</exception>
    public TenantResidencyRefusal Refusal(string regionId)
    {
        ArgumentNullException.ThrowIfNull(regionId);

        for (var i = 0; i < _regions.Count; i++)
        {
            if (string.Equals(_regions[i].RegionId, regionId, StringComparison.Ordinal))
            {
                return Refusal(_regions[i]);
            }
        }

        return TenantResidencyRefusal.NotAllowed;
    }

    /// <summary>
    /// Toggles the planned residency of <paramref name="regionId"/> when the two
    /// invariants permit it.
    /// </summary>
    /// <param name="regionId">The region to toggle. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// <see cref="TenantResidencyRefusal.None"/> when the plan changed, or the
    /// refusal that blocked it - in which case the plan is untouched.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="regionId"/> is <see langword="null"/>.</exception>
    public TenantResidencyRefusal Toggle(string regionId)
    {
        ArgumentNullException.ThrowIfNull(regionId);

        var refusal = Refusal(regionId);
        if (refusal != TenantResidencyRefusal.None)
        {
            return refusal;
        }

        if (!_planned.Remove(regionId))
        {
            _planned.Add(regionId);
        }

        Project();
        return TenantResidencyRefusal.None;
    }

    /// <summary>
    /// Discards the pending edit, returning the plan to the residency the
    /// cluster reported.
    /// </summary>
    public void Revert()
    {
        if (!IsChanged)
        {
            return;
        }

        _planned.Clear();
        for (var i = 0; i < _regions.Count; i++)
        {
            if (_regions[i].IsResident)
            {
                _planned.Add(_regions[i].RegionId);
            }
        }

        Project();
    }

    /// <summary>
    /// Materialises the planned residency set as the complete list the cluster
    /// expects, ascending by region id so the request is deterministic.
    /// </summary>
    /// <returns>The planned residency region ids.</returns>
    public IReadOnlyList<string> PlannedResidency()
    {
        if (_plannedResidentCount == 0)
        {
            return Array.Empty<string>();
        }

        var planned = new string[_plannedResidentCount];
        var next = 0;

        // Walked in the cluster's own row order rather than the hash set's, so
        // the request is stable across runs.
        for (var i = 0; i < _regions.Count && next < planned.Length; i++)
        {
            var regionId = _regions[i].RegionId;
            if (_planned.Contains(regionId))
            {
                planned[next++] = regionId;
            }
        }

        return planned;
    }

    private TenantResidencyRefusal Refusal(in ExplorerTenantRegion region)
    {
        var isPlanned = _planned.Contains(region.RegionId);

        if (isPlanned)
        {
            // Removing the last planned residency would leave the tenant resident
            // nowhere, which the cluster refuses outright.
            if (_plannedResidentCount <= 1)
            {
                return TenantResidencyRefusal.LastRegion;
            }

            return TenantResidencyRefusal.None;
        }

        if (region.IsAllowed)
        {
            return TenantResidencyRefusal.None;
        }

        // Not planned, not allowed. Distinguish the ordinary case from the one
        // where the allowed set was narrowed under a live residency, because the
        // caller's remedy differs.
        return region.IsResident
            ? TenantResidencyRefusal.ResidentButNoLongerAllowed
            : TenantResidencyRefusal.NotAllowed;
    }

    private void Project()
    {
        _plannedResidentCount = 0;
        for (var i = 0; i < _regions.Count; i++)
        {
            if (_planned.Contains(_regions[i].RegionId))
            {
                _plannedResidentCount++;
            }
        }

        // Sized exactly, and only when the reading's region count moved, so the
        // array can be handed out as IReadOnlyList without a wrapper.
        if (_rows.Length != _regions.Count)
        {
            _rows = new TenantRegionRow[_regions.Count];
        }

        var changed = false;
        for (var i = 0; i < _regions.Count; i++)
        {
            var region = _regions[i];
            var isPlanned = _planned.Contains(region.RegionId);
            changed |= isPlanned != region.IsResident;

            _rows[i] = new TenantRegionRow(
                region.RegionId,
                region.Status,
                region.IsAllowed,
                region.IsResident,
                isPlanned,
                Refusal(region));
        }

        IsChanged = changed;
    }
}
