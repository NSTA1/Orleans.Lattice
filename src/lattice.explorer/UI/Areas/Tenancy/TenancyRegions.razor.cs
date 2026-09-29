using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One tenant's regions, bound to <c>ILatticeTenantRegionAdmin</c>, in two
/// labelled parts: the allowed set a platform operator sets (editable when
/// <see cref="CanAuthorize"/>; revoking a region is confirmed), and the
/// residency the tenant's admins choose within it, applied with
/// <c>SetResidencyAsync</c>. Each region shows its lifecycle and what it means
/// for the tenant. A change that removes a region (it drains the tenant's data
/// there) or leaves the tenant with residency and no Online region (it is then
/// served nowhere, because an added region stays Provisioning until an operator
/// of the hosting deployment promotes it) is confirmed with that consequence.
/// </summary>
public partial class TenancyRegions
{
    /// <summary>The hint under the operator's allowed-region picker.</summary>
    internal const string AllowedHint =
        "Choose every region this tenant may use; saving replaces the whole set. A region the tenant is resident in cannot be revoked.";

    /// <summary>What a residency change that leaves no Online region adds to its outcome.</summary>
    internal const string NotServedYet = "It is not served anywhere until one of its regions is Online.";

    private IReadOnlyList<TenantRegionStatusDescriptor>? _regions;
    private readonly TenancyResidencyPlan _plan = new();
    private TenancyFailure? _failure;
    private string? _loadedFor;
    private bool _busy;
    private bool _confirmResidency;
    private bool _confirmAllowed;
    private IReadOnlyList<string> _allowed = [];
    private LtMultiComboBox? _allowedBox;
    private TenancyRegionSuggestionSource? _regionSource;
    private IReadOnlyList<string> _regionIds = [];
    private string? _allowedError;
    private IReadOnlyList<string> _pendingAllowed = [];
    private IReadOnlyList<string> _revokedAllowed = [];

    /// <summary>The tenant whose regions to show.</summary>
    [Parameter, EditorRequired]
    public string TenantId { get; set; } = string.Empty;

    /// <summary>Whether the caller may change the allowed set: a platform operator. The cluster still authorizes it.</summary>
    [Parameter]
    public bool CanAuthorize { get; set; }

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private string HeadingId => "tenancy-regions-heading-" + TenantId;

    private string AllowedHeadingId => "tenancy-allowed-heading-" + TenantId;

    private string ResidencyHeadingId => "tenancy-residency-heading-" + TenantId;

    private string ConfirmTitle => _plan.LeavesNoOnlineRegion ? $"Stop serving tenant {TenantId}?" : "Remove regions from the residency?";

    private string ConfirmText => _plan.LeavesNoOnlineRegion ? "Apply and stop serving" : "Drain and apply";

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;


    private string PlanSummary
    {
        get
        {
            var parts = new List<string>(2);
            if (_plan.Added is { Count: > 0 } added)
            {
                parts.Add("add " + string.Join(", ", added));
            }

            if (_plan.Removed is { Count: > 0 } removed)
            {
                parts.Add("remove " + string.Join(", ", removed));
            }

            return "Not applied: " + string.Join("; ", parts) + ".";
        }
    }

    /// <inheritdoc />
    protected override void OnInitialized() =>
        _regionSource = new TenancyRegionSuggestionSource(Suggestions.Regions, () => _regionIds);

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedFor, TenantId, StringComparison.Ordinal))
        {
            return;
        }

        _loadedFor = TenantId;
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _regions = null;
        try
        {
            var regions = Catalog.Regions ?? throw new NotSupportedException();
            var report = await regions.GetTenantRegionStatusAsync(TenantId).ConfigureAwait(true);
            Adopt(report.Regions);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private void Adopt(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        _regions = [.. regions.OrderBy(region => region.RegionId, StringComparer.Ordinal)];
        _plan.Reset(_regions);
        _allowed = [.. TenancyFormat.AllowedRegions(_regions)];
        _regionIds = [.. _regions.Select(region => region.RegionId)];
        _allowedError = null;
    }

    private void Toggle(string regionId)
    {
        if (_plan.Toggle(regionId) is { } refusal)
        {
            Toasts.Show(refusal, LtToastTone.Warning);
        }
    }

    private async Task ApplyResidency()
    {
        if (!_plan.IsChanged)
        {
            return;
        }

        if (_plan.Removed.Count > 0 || _plan.LeavesNoOnlineRegion)
        {
            _confirmResidency = true;
            return;
        }

        await SetResidencyAsync().ConfigureAwait(true);
    }

    private async Task SetResidencyAsync()
    {
        _confirmResidency = false;
        if (_busy)
        {
            return;
        }

        _busy = true;
        try
        {
            var result = await Catalog.Regions!.SetResidencyAsync(TenantId, _plan.Planned).ConfigureAwait(true);
            Adopt(result.Regions);
            Toasts.Show(ResidencyDone(result), LtToastTone.Success);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
        }
    }

    private string ResidencyDone(TenantResidencyChangeResult result)
    {
        var parts = new List<string>(2);
        if (result.AddedRegions.Count > 0)
        {
            parts.Add("adding " + string.Join(", ", result.AddedRegions));
        }

        if (result.RemovedRegions.Count > 0)
        {
            parts.Add("draining " + string.Join(", ", result.RemovedRegions));
        }

        var done = parts.Count == 0
            ? $"The residency of tenant {TenantId} was already as planned."
            : $"Tenant {TenantId} is {string.Join(" and ", parts)}.";
        return TenancyFormat.IsServedNowhere(result.Regions) ? done + " " + NotServedYet : done;
    }

    private async Task SaveAllowed()
    {
        _allowedError = null;
        if (_allowedBox is not null && !await _allowedBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        var requested = _allowed.Distinct(StringComparer.Ordinal).ToArray();
        var resident = TenancyFormat.ResidentRegions(_regions ?? []);
        var stillResident = resident.Where(region => !requested.Contains(region, StringComparer.Ordinal)).ToArray();
        if (stillResident.Length > 0)
        {
            _allowedError = $"Tenant {TenantId} is still resident in {string.Join(", ", stillResident)}. Remove the residency first.";
            return;
        }

        _pendingAllowed = requested;
        _revokedAllowed = [.. TenancyFormat.AllowedRegions(_regions ?? []).Where(region => !requested.Contains(region, StringComparer.Ordinal))];
        if (_revokedAllowed.Count > 0)
        {
            _confirmAllowed = true;
            return;
        }

        await AuthorizeAsync().ConfigureAwait(true);
    }

    private async Task AuthorizeAsync()
    {
        _confirmAllowed = false;
        if (_busy)
        {
            return;
        }

        _busy = true;
        StateHasChanged();
        try
        {
            var result = await Catalog.Regions!.AuthorizeAllowedRegionsAsync(TenantId, _pendingAllowed).ConfigureAwait(true);
            Toasts.Show($"Tenant {TenantId} is allowed {TenancyFormat.RegionList(result.AllowedRegions, "no region")}.", LtToastTone.Success);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _allowedError = failure.Message;
            _busy = false;
            return;
        }
        catch
        {
            _busy = false;
            throw;
        }

        // The controls stay disabled until the regions are read again, so a residency edit
        // made meanwhile is not silently replaced by the re-read.
        try
        {
            await LoadAsync().ConfigureAwait(true);
        }
        finally
        {
            _busy = false;
        }
    }
}
