using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One tenant's regions, bound to <c>ILatticeTenantRegionAdmin</c>: the
/// per-region residency lifecycle, a residency plan applied with
/// <c>SetResidencyAsync</c> (removing a region is confirmed, because it drains
/// the tenant's data there), and, when <see cref="CanAuthorize"/>, the
/// operator's allowed-region set (revoking one is confirmed).
/// </summary>
public partial class TenancyRegions
{
    private IReadOnlyList<TenantRegionStatusDescriptor>? _regions;
    private readonly TenancyResidencyPlan _plan = new();
    private TenancyFailure? _failure;
    private string? _loadedFor;
    private bool _busy;
    private bool _confirmResidency;
    private bool _confirmAllowed;
    private string _allowedText = string.Empty;
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

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private string HeadingId => "tenancy-regions-heading-" + TenantId;

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private IReadOnlyList<string> Added => [.. _plan.Rows.Where(row => row.IsPlanned && !row.IsResident).Select(row => row.RegionId)];

    private IReadOnlyList<string> Removed => [.. _plan.Rows.Where(row => !row.IsPlanned && row.IsResident).Select(row => row.RegionId)];

    private string PlanSummary
    {
        get
        {
            var parts = new List<string>(2);
            if (Added is { Count: > 0 } added)
            {
                parts.Add("add " + string.Join(", ", added));
            }

            if (Removed is { Count: > 0 } removed)
            {
                parts.Add("remove " + string.Join(", ", removed));
            }

            return "Not applied: " + string.Join("; ", parts) + ".";
        }
    }

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
        _allowedText = string.Join(", ", TenancyFormat.AllowedRegions(_regions));
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

        if (Removed.Count > 0)
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

        return parts.Count == 0
            ? $"The residency of tenant {TenantId} was already as planned."
            : $"Tenant {TenantId} is {string.Join(" and ", parts)}.";
    }

    private async Task SaveAllowed()
    {
        _allowedError = null;
        var requested = _allowedText
            .Split([',', ' ', ';', '\n', '\r', '\t'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
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
        try
        {
            var result = await Catalog.Regions!.AuthorizeAllowedRegionsAsync(TenantId, _pendingAllowed).ConfigureAwait(true);
            Toasts.Show($"Tenant {TenantId} is allowed {TenancyFormat.RegionList(result.AllowedRegions, "no region")}.", LtToastTone.Success);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _allowedError = failure.Message;
            return;
        }
        finally
        {
            _busy = false;
        }

        await LoadAsync().ConfigureAwait(true);
    }
}
