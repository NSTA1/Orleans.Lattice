using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One tenant's regions, bound to <c>ILatticeTenantRegionAdmin</c>, in two
/// labelled parts: the allowed set a platform operator sets (editable when
/// <see cref="CanAuthorize"/>; revoking a region is confirmed), and the
/// residency the tenant's admins choose within it, applied with
/// <c>SetResidencyAsync</c>. Each region shows its lifecycle, what it means for
/// the tenant and whether the region serves it: every region does while no
/// residency is set, and only an Online one once it is. A changed plan is
/// previewed region by region before it is applied. A plan that removes a
/// region (it drains the tenant's data there) is confirmed; one that would
/// leave the tenant with residency and no Online region (an added region stays
/// Provisioning until an operator of the hosting deployment promotes it) turns
/// Apply off, says which regions are not Online and why, and can be applied
/// only through an explicit secondary path whose confirmation keeps serving by
/// default.
/// </summary>
/// <remarks>
/// While any region is part-way along a residency path (Provisioning,
/// Backfilling, Draining or Offline) the section shows the step it has reached and
/// follows the regions on the circuit's clock (<see cref="TenancyRegionFollower"/>),
/// announcing each stage change through its polite live region, until every
/// region is steady. The follow is keyed on the caller it started for and on the
/// reading it continues, so a sign-in as someone else, or a newer reading, ends it.
/// </remarks>
public partial class TenancyRegions : IDisposable
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
    private string? _savePhase;
    private bool _confirmResidency;
    private bool _confirmAllowed;
    private IReadOnlyList<string> _allowed = [];
    private LtMultiComboBox? _allowedBox;
    private TenancyRegionSuggestionSource? _regionSource;
    private IReadOnlyList<string> _regionIds = [];
    private string? _allowedError;
    private IReadOnlyList<string> _pendingAllowed = [];
    private IReadOnlyList<string> _revokedAllowed = [];
    private readonly ComponentLifetime _lifetime = new();
    private TenancyRegionFollower? _follower;
    private int _reading;
    private bool _following;
    private string? _stageAnnouncement;

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

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private string HeadingId => "tenancy-regions-heading-" + TenantId;

    private string AllowedHeadingId => "tenancy-allowed-heading-" + TenantId;

    private string ResidencyHeadingId => "tenancy-residency-heading-" + TenantId;

    private string ConfirmTitle => _plan.StopsServing ? $"Stop serving tenant {TenantId}?" : "Remove regions from the residency?";

    private string StopServingOpenText => $"Apply anyway and stop serving {TenantId}...";

    private string PreviewHeadingId => "tenancy-residency-preview-" + TenantId;

    private static string ServedText(TenancyRegionRow row) => row.IsServed ? TenancyFormat.ServedLabel : TenancyFormat.NotServedLabel;

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private TenancyRegionFollower Follower => _follower ??= new TenancyRegionFollower(Time);

    private static string CompactSecondary(TenancyRegionRow row)
    {
        var text = ServedText(row) + ", " + (row.IsAllowed ? "allowed" : "not allowed");
        if (TenancyRegionStep.For(row.Status) is { } step)
        {
            text += ", " + step.Short;
        }

        if (row.IsPlanned != row.IsResident)
        {
            text += row.IsPlanned ? ", to be added" : ", to be removed";
        }

        return text;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _follower?.Dispose();
        _lifetime.Leave();
    }


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
        StopFollowing();
        var token = _lifetime.Renew();
        try
        {
            var regions = Catalog.Regions ?? throw new NotSupportedException();
            var report = await regions.GetTenantRegionStatusAsync(TenantId, token).ConfigureAwait(true);
            if (_lifetime.IsLeft || token.IsCancellationRequested)
            {
                return;
            }

            Adopt(report.Regions);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Replaced by a newer read, or the section was left.
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
        _reading++;
        FollowIfTransitional();
    }

    private void FollowIfTransitional()
    {
        if (_regions is { } regions && regions.Any(region => TenancyRegionStep.IsTransitional(region.Status)) && !_lifetime.IsLeft)
        {
            var caller = Catalog.Caller.Current;
            var reading = _reading;
            _following = true;
            Follower.Follow(token => FollowReadAsync(caller, reading, token));
        }
        else
        {
            StopFollowing();
        }
    }

    private void StopFollowing()
    {
        _following = false;
        _follower?.Stop();
    }

    private async Task<TenancyRegionFollowOutcome> FollowReadAsync(ShellCallerKey caller, int reading, CancellationToken token)
    {
        if (Catalog.Regions is not { } facade)
        {
            return TenancyRegionFollowOutcome.Steady;
        }

        TenantRegionStatusReport report;
        try
        {
            report = await facade.GetTenantRegionStatusAsync(TenantId, token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!token.IsCancellationRequested && TenancyFailure.From(exception) is { } failure)
        {
            // A read that did not get an answer is tried again after a longer wait; a
            // refusal will not change by asking again, so the follow ends.
            if (failure.IsRetryable)
            {
                return TenancyRegionFollowOutcome.Unchanged;
            }

            await InvokeAsync(() =>
            {
                if (reading == _reading)
                {
                    _following = false;
                    StateHasChanged();
                }
            }).ConfigureAwait(false);
            return TenancyRegionFollowOutcome.Steady;
        }

        var outcome = TenancyRegionFollowOutcome.Steady;
        await InvokeAsync(() => outcome = Absorb(report.Regions, caller, reading, token)).ConfigureAwait(false);
        return outcome;
    }

    // Runs on the renderer's dispatcher: takes a followed reading in, keeping any
    // residency edit in progress, and announces the regions whose stage changed.
    private TenancyRegionFollowOutcome Absorb(
        IReadOnlyList<TenantRegionStatusDescriptor> regions, ShellCallerKey caller, int reading, CancellationToken token)
    {
        if (_lifetime.IsLeft || token.IsCancellationRequested || reading != _reading || _regions is not { } before)
        {
            return TenancyRegionFollowOutcome.Steady;
        }

        if (Catalog.Caller.Current != caller)
        {
            // Read for a caller who is no longer signed in here: never shown.
            _following = false;
            StateHasChanged();
            return TenancyRegionFollowOutcome.Steady;
        }

        IReadOnlyList<TenantRegionStatusDescriptor> after = [.. regions.OrderBy(region => region.RegionId, StringComparer.Ordinal)];
        var changes = StageChanges(before, after);
        _regions = after;
        _plan.Update(after);
        _regionIds = [.. after.Select(region => region.RegionId)];
        if (changes.Count > 0)
        {
            _stageAnnouncement = string.Join(" ", changes);
        }

        var transitional = after.Any(region => TenancyRegionStep.IsTransitional(region.Status));
        _following = transitional;
        StateHasChanged();
        return !transitional
            ? TenancyRegionFollowOutcome.Steady
            : changes.Count > 0 ? TenancyRegionFollowOutcome.Changed : TenancyRegionFollowOutcome.Unchanged;
    }

    private List<string> StageChanges(IReadOnlyList<TenantRegionStatusDescriptor> before, IReadOnlyList<TenantRegionStatusDescriptor> after)
    {
        var changes = new List<string>();
        foreach (var region in after)
        {
            var previous = before.FirstOrDefault(candidate => string.Equals(candidate.RegionId, region.RegionId, StringComparison.Ordinal));
            if (previous is not null && previous.Status != region.Status)
            {
                changes.Add($"Region {region.RegionId} of tenant {TenantId} is now {TenancyFormat.RegionStatusLabel(region.Status, _plan.HasResidency)}.");
            }
        }

        return changes;
    }

    private void Toggle(string regionId)
    {
        if (_plan.Toggle(regionId) is { } refusal)
        {
            Toasts.Show(refusal, LtToastTone.Warning);
        }

        StateHasChanged();
    }

    private async Task ApplyResidency()
    {
        // A plan that stops serving the tenant is applied only through the
        // explicit "Apply anyway" path and its confirmation, never from Apply.
        if (!_plan.IsChanged || _plan.StopsServing)
        {
            return;
        }

        if (_plan.Removed.Count > 0)
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
        _savePhase = $"Applying residency for {TenantId}";
        StopFollowing();
        StateHasChanged();
        try
        {
            var result = await Catalog.Regions!.SetResidencyAsync(TenantId, _plan.Planned).ConfigureAwait(true);
            if (_lifetime.IsLeft)
            {
                return;
            }

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
            _savePhase = null;
            FollowIfTransitional();
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
        _savePhase = $"Saving allowed regions for {TenantId}";
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
            _savePhase = null;
            return;
        }
        catch
        {
            _busy = false;
            _savePhase = null;
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
            _savePhase = null;
        }
    }
}
