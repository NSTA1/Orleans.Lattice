using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;

/// <summary>
/// The Regions surface: where the tenant is resident, within the regions a
/// platform operator has authorized for it.
/// </summary>
/// <remarks>
/// The two-set model is enforced in the plan before it is enforced on the wire.
/// Residency is a subset of the allowed set, and the last resident region can
/// never be removed - both are knowable from the rows the cluster already
/// returned, so the surface refuses at the control and says why, rather than
/// spending a round trip to be told in a message the gRPC binding has already
/// flattened onto a single precondition status.
/// </remarks>
public sealed partial class MyTenantWorkspace
{
    /// <summary>
    /// The refusal shown for a region outside the operator-authorized allowed
    /// set.
    /// </summary>
    public const string RegionNotAllowedRefusal =
        "That region is not in the allowed set a platform operator authorized for your tenant, so "
        + "you cannot become resident in it.";

    /// <summary>
    /// The refusal shown for a region the tenant is resident in but which is no
    /// longer allowed, so it can be left but not re-entered.
    /// </summary>
    public const string RegionNoLongerAllowedRefusal =
        "Your tenant is still resident here, but the region is no longer in the allowed set. You can "
        + "remove the residency; you cannot restore it without an operator allowing the region again.";

    /// <summary>The refusal shown when removing the last planned resident region.</summary>
    public const string LastRegionRefusal =
        "This is the only region your tenant would be resident in, so it cannot be removed.";

    /// <summary>The refusal shown when applying a plan that changes nothing.</summary>
    public const string NoResidencyChangeMessage = "Residency is already as planned; nothing to apply.";

    private bool _regionsLoaded;

    /// <summary>
    /// The editable residency plan: the per-region rows, the pending residency
    /// set, and the two invariants that gate every toggle.
    /// </summary>
    public TenantResidencyPlan Regions { get; } = new();

    /// <summary>
    /// Whether the plan differs from the residency the cluster holds, so the
    /// Apply and Revert controls are meaningful.
    /// </summary>
    public bool HasPendingResidencyChange => Regions.IsChanged;

    /// <summary>
    /// Toggles the planned residency of <paramref name="regionId"/>, naming the
    /// invariant when the plan refuses.
    /// </summary>
    /// <param name="regionId">The region to toggle.</param>
    public void ToggleRegion(string regionId)
    {
        if (!Allowed || Busy || string.IsNullOrEmpty(regionId))
        {
            return;
        }

        var refusal = Regions.Toggle(regionId);
        switch (refusal)
        {
            case TenantResidencyRefusal.None:
                LastNotice = null;
                RaiseChanged();
                return;
            case TenantResidencyRefusal.LastRegion:
                Refuse(
                    TenantOperationStatus.LastRegion,
                    LastRegionRefusal,
                    MyTenantNotice.LastRegionGuidance);
                return;
            case TenantResidencyRefusal.ResidentButNoLongerAllowed:
                Refuse(
                    TenantOperationStatus.RegionNotAllowed,
                    RegionNoLongerAllowedRefusal,
                    MyTenantNotice.RegionNotAllowedGuidance);
                return;
            default:
                Refuse(
                    TenantOperationStatus.RegionNotAllowed,
                    RegionNotAllowedRefusal,
                    MyTenantNotice.RegionNotAllowedGuidance);
                return;
        }
    }

    /// <summary>Discards the pending residency edit.</summary>
    public void RevertResidency()
    {
        if (Busy || !Regions.IsChanged)
        {
            return;
        }

        Regions.Revert();
        LastNotice = null;
        RaiseChanged();
    }

    /// <summary>
    /// Applies the planned residency set. The cluster remains the enforcement
    /// point, so a refusal it makes anyway is rendered with its own message
    /// verbatim.
    /// </summary>
    public async Task ApplyResidencyAsync()
    {
        if (!Allowed || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        if (!Regions.IsChanged)
        {
            Refuse(TenantOperationStatus.InvalidRequest, NoResidencyChangeMessage);
            return;
        }

        // Guarded again at the point of sending, not only at the toggle: a plan
        // that somehow reached zero residencies must never be sent, whatever
        // sequence of interactions produced it.
        if (Regions.PlannedResidentCount == 0)
        {
            Refuse(
                TenantOperationStatus.LastRegion,
                LastRegionRefusal,
                MyTenantNotice.LastRegionGuidance);
            return;
        }

        var tenantId = TenantId;
        var planned = Regions.PlannedResidency();

        await RunAsync(
            () => _domain.Tenants.SetResidencyAsync(tenantId, planned),
            change => Regions.Reset(change.Regions)).ConfigureAwait(false);
    }

    private async Task LoadRegionsAsync(bool force)
    {
        if ((!force && _regionsLoaded) || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        _regionsLoaded = true;

        var regions = await _domain.Tenants.GetRegionStatusAsync(TenantId).ConfigureAwait(false);
        if (regions.IsSuccess && regions.Value is { } rows)
        {
            Regions.Reset(rows);
            return;
        }

        Regions.Reset(Array.Empty<ExplorerTenantRegion>());
        LastNotice = MyTenantNotice.For(regions);
    }
}
