using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

/// <summary>
/// The allowed-regions surface: the operator-authorized allowed set, the
/// tenant's per-region residency lifecycle beside it, and the editor that
/// replaces the allowed set.
/// <para>
/// Allowed and resident are two sets, not one. Residency is always a subset of
/// the allowed set, so revoking an authorization for a region the tenant is
/// still resident in is refused by the cluster. That refusal is <b>predicted
/// client-side from state already in hand</b> and reported with its specific
/// reason, rather than round-tripped to come back as a generic precondition
/// failure - the gRPC binding collapses every precondition refusal onto one
/// code, so a caller reached over the wire could not otherwise tell which rule
/// it broke.
/// </para>
/// </summary>
public sealed partial class TenantsWorkspace
{
    /// <summary>The message shown when the regions surface has no tenant selected.</summary>
    public const string RegionsNeedTenantMessage =
        "Select a tenant to read and authorize its allowed regions.";

    /// <summary>The message shown when the allowed set is saved with nothing changed.</summary>
    public const string RegionsUnchangedMessage = "No change to the allowed region set.";

    /// <summary>The message shown when the region to add is blank or already listed.</summary>
    public const string RegionAlreadyListedMessage =
        "Enter a region id that is not already listed.";

    private static readonly IReadOnlyList<TenantRegionRow> NoRegions = Array.Empty<TenantRegionRow>();

    private readonly List<TenantRegionRow> _regions = [];

    private bool _regionsLoaded;

    /// <summary>
    /// The selected tenant's regions: every region that is either allowed or
    /// carries a residency, ordered by region id, plus any the operator has
    /// added but not yet authorized.
    /// </summary>
    public IReadOnlyList<TenantRegionRow> Regions => _regionsLoaded ? _regions : NoRegions;

    /// <summary>The region id to add to the allowed set, as typed into the surface.</summary>
    public string AddRegionId { get; set; } = string.Empty;

    /// <summary>Whether the pending allowed set differs from what the cluster holds.</summary>
    public bool HasRegionChanges
    {
        get
        {
            for (var i = 0; i < _regions.Count; i++)
            {
                if (_regions[i].IsChanged)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>
    /// Whether the pending allowed set would revoke a region the tenant is still
    /// resident in, which the cluster refuses. The surface warns on this before
    /// the call rather than translating the refusal after it.
    /// </summary>
    public bool WouldStrandResidency
    {
        get
        {
            for (var i = 0; i < _regions.Count; i++)
            {
                if (_regions[i].WouldRevokeResident)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>Re-reads the selected tenant's per-region status, discarding pending edits.</summary>
    public async Task RefreshRegionsAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            await LoadRegionsAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Adds <see cref="AddRegionId"/> to the surface as a pending authorization,
    /// so a region the tenant has never been allowed into can be authorized.
    /// </summary>
    public void AddRegion()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        var regionId = AddRegionId.Trim();
        if (regionId.Length == 0 || ContainsRegion(regionId))
        {
            Report(TenantOperationStatus.InvalidRequest, RegionAlreadyListedMessage);
            RaiseChanged();
            return;
        }

        _regions.Add(new TenantRegionRow(
            new ExplorerTenantRegion(regionId, ExplorerTenantRegionLifecycle.None, IsAllowed: false))
        {
            Allow = true,
        });

        AddRegionId = string.Empty;
        ClearResult();
        RaiseChanged();
    }

    /// <summary>
    /// Sets the pending authorization intent for <paramref name="regionId"/>.
    /// </summary>
    /// <param name="regionId">The region whose intent to set.</param>
    /// <param name="allow">Whether the tenant should be allowed to be resident there.</param>
    public void SetRegionAllowed(string regionId, bool allow)
    {
        ArgumentNullException.ThrowIfNull(regionId);

        for (var i = 0; i < _regions.Count; i++)
        {
            if (string.Equals(_regions[i].RegionId, regionId, StringComparison.Ordinal))
            {
                _regions[i].Allow = allow;
                ClearResult();
                RaiseChanged();
                return;
            }
        }
    }

    /// <summary>
    /// Applies the pending allowed set. A change that only adds authorizations is
    /// applied directly; one that revokes any is held for explicit confirmation;
    /// and one that would revoke a region the tenant is still resident in is
    /// refused here, with the rule it breaks, rather than being sent for the
    /// cluster to refuse generically.
    /// </summary>
    public async Task RequestAuthorizeRegionsAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            Report(TenantOperationStatus.InvalidRequest, RegionsNeedTenantMessage);
            RaiseChanged();
            return;
        }

        if (!HasRegionChanges)
        {
            Report(TenantOperationStatus.Succeeded, RegionsUnchangedMessage);
            RaiseChanged();
            return;
        }

        // Predicted from state already held. Reported under the same
        // classification the facade would have used, so the operator reads the
        // rule rather than a generic precondition failure the wire would have
        // flattened it into.
        var stranded = CollectStrandedRegions();
        if (stranded is not null)
        {
            Report(
                TenantOperationStatus.RegionNotAllowed,
                "Cannot revoke " + string.Join(", ", stranded) + ": the tenant is still resident there. "
                    + TenantRefusal.ResidentRegionRule);
            RaiseChanged();
            return;
        }

        var revoked = CollectRevokedRegions();
        if (revoked is null)
        {
            // Purely additive, so nothing is taken away and nothing needs
            // confirming.
            await AuthorizeRegionsConfirmedAsync(tenantId).ConfigureAwait(false);
            return;
        }

        Confirmation = new TenantConfirmation
        {
            Kind = TenantConfirmationKind.RevokeRegion,
            TenantId = tenantId,
            Title = "Revoke region authorization?",
            Body = "This removes " + string.Join(", ", revoked) + " from the regions "
                + tenantId + " is authorized to be resident in. The tenant will not be able to "
                + "place residency there until an operator authorizes it again.",
            ConfirmLabel = "Revoke authorization",
        };

        ClearResult();
        RaiseChanged();
    }

    private async Task AuthorizeRegionsConfirmedAsync(string tenantId)
    {
        BeginBusy();
        try
        {
            var desired = new List<string>(_regions.Count);
            for (var i = 0; i < _regions.Count; i++)
            {
                if (_regions[i].Allow)
                {
                    desired.Add(_regions[i].RegionId);
                }
            }

            var authorized = await _domain.Tenants
                .AuthorizeAllowedRegionsAsync(tenantId, desired)
                .ConfigureAwait(false);

            if (!authorized.IsSuccess)
            {
                Report(authorized.Status, TenantRefusal.DescribeRegionChange(authorized));
                return;
            }

            var allowed = authorized.Value ?? Array.Empty<string>();
            Report(
                TenantOperationStatus.Succeeded,
                allowed.Count == 0
                    ? "Tenant " + tenantId + " is now authorized for no regions."
                    : "Tenant " + tenantId + " is now authorized for " + string.Join(", ", allowed) + ".");

            await LoadRegionsAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    private async Task LoadRegionsAsync(bool force)
    {
        if (!force && _regionsLoaded)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            ResetRegions();
            return;
        }

        var read = await _domain.Tenants.GetRegionStatusAsync(tenantId).ConfigureAwait(false);
        _regions.Clear();
        _regionsLoaded = true;

        if (!read.IsSuccess || read.Value is null)
        {
            Report(read.Status, TenantRefusal.DescribeRegionChange(read));
            return;
        }

        var rows = read.Value;
        for (var i = 0; i < rows.Count; i++)
        {
            _regions.Add(new TenantRegionRow(rows[i]));
        }
    }

    private void ResetRegions()
    {
        _regionsLoaded = false;
        _regions.Clear();
        AddRegionId = string.Empty;
    }

    private bool ContainsRegion(string regionId)
    {
        for (var i = 0; i < _regions.Count; i++)
        {
            if (string.Equals(_regions[i].RegionId, regionId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private List<string>? CollectStrandedRegions()
    {
        List<string>? stranded = null;
        for (var i = 0; i < _regions.Count; i++)
        {
            if (_regions[i].WouldRevokeResident)
            {
                stranded ??= [];
                stranded.Add(_regions[i].RegionId);
            }
        }

        return stranded;
    }

    private List<string>? CollectRevokedRegions()
    {
        List<string>? revoked = null;
        for (var i = 0; i < _regions.Count; i++)
        {
            if (_regions[i].IsAllowed && !_regions[i].Allow)
            {
                revoked ??= [];
                revoked.Add(_regions[i].RegionId);
            }
        }

        return revoked;
    }
}
