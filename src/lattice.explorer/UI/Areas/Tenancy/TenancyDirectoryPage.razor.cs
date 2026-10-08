using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The tenant directory (<c>/tenancy</c>) for a platform operator: every tenant
/// the caller can reach with its state, then its quota use and residency
/// (linking to its regions) as each is read - the installed-app count is read
/// only for the active tenant, so it is the label of a row's Apps action rather
/// than a column that would be unknown on every other row - a search, a "New tenant"
/// form - the visible control of the palette's create command - that can also
/// set the new tenant's allowed regions and initial residency, and a "Set
/// regions" picker, the visible control of the palette's set-regions command.
/// Creating a tenant with a residency is confirmed, because its regions start
/// Provisioning and the tenant is served nowhere until backfill completes to
/// Online; each region step after the creation reports its own outcome. The
/// active tenant is marked as current. A caller without operator standing is
/// sent, replacing the history entry, to its own tenant's workspace.
/// </summary>
public partial class TenancyDirectoryPage
{
    private List<TenancyDirectoryRow>? _rows;
    private IReadOnlyList<TenancyDirectoryRow>? _visible;
    private string? _visibleSearch;
    private TenancyFailure? _failure;
    private string _search = string.Empty;
    private bool _createOpen;
    private bool _saving;
    private string _newId = string.Empty;
    private IReadOnlyList<string> _newAdmins = [];
    private IReadOnlyList<string> _newAllowed = [];
    private IReadOnlyList<string> _newResidency = [];
    private LtNameInput? _idBox;
    private LtMultiComboBox? _adminsBox;
    private LtMultiComboBox? _allowedBox;
    private LtMultiComboBox? _residencyBox;
    private TenancyChosenRegionSource? _residencySource;
    private string? _idError;
    private string? _adminsError;
    private string? _allowedError;
    private string? _residencyError;
    private string? _formError;
    private bool _confirmCreate;
    private string _pendingId = string.Empty;
    private IReadOnlyList<string> _pendingAdmins = [];
    private IReadOnlyList<string> _pendingAllowed = [];
    private IReadOnlyList<string> _pendingResidency = [];
    private bool _setRegionsOpen;
    private string _pickedTenant = string.Empty;
    private LtComboBox? _pickBox;
    private string? _pickError;

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private IReadOnlyList<TenancyDirectoryRow> Visible
    {
        get
        {
            if (_rows is null)
            {
                return [];
            }

            // Memoised per search, so the table keeps one items instance across renders.
            if (_visible is not null && _visibleSearch == _search)
            {
                return _visible;
            }

            var term = _search.Trim();
            _visible = term.Length == 0 ? _rows : [.. _rows.Where(row => row.TenantId.Contains(term, StringComparison.OrdinalIgnoreCase))];
            _visibleSearch = _search;
            return _visible;
        }
    }

    private string CountText
    {
        get
        {
            var total = _rows?.Count ?? 0;
            var suspended = _rows?.Count(row => row.Tenant.Status == TenantLifecycleStatus.Suspended) ?? 0;
            var shown = Visible.Count;
            var noun = total == 1 ? "tenant" : "tenants";
            var head = shown == total ? $"{TenancyFormat.Count(total)} {noun}" : $"{TenancyFormat.Count(shown)} of {TenancyFormat.Count(total)} {noun}";
            return suspended == 0 ? head : $"{head}, {TenancyFormat.Count(suspended)} suspended";
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _residencySource = new TenancyChosenRegionSource(() => _newAllowed);
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _rows = null;
        _visible = null;
        IReadOnlyList<TenantDescriptor> tenants;
        try
        {
            var standing = await Catalog.GetStandingAsync(CancellationToken.None).ConfigureAwait(true);
            if (!standing.IsOperator)
            {
                Navigator.NavigateTo(TenancyRoutes.MyTenant(standing.Workspace), replace: true);
                return;
            }

            tenants = await Catalog.GetTenantsAsync(CancellationToken.None).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
            return;
        }

        _rows = [.. tenants.Select(tenant => new TenancyDirectoryRow(tenant))];
        _createOpen = string.Equals(Address.GetQuery(TenancyRoutes.NewQuery), TenancyRoutes.NewValue, StringComparison.Ordinal);
        if (!_createOpen && string.Equals(Address.GetQuery(TenancyRoutes.SetRegionsQuery), TenancyRoutes.NewValue, StringComparison.Ordinal) && _rows.Count > 0)
        {
            OpenSetRegions();
        }
        StateHasChanged();
        await Task.WhenAll(_rows.Select(ReadDetailsAsync)).ConfigureAwait(true);
    }

    private async Task ReadDetailsAsync(TenancyDirectoryRow row)
    {
        try
        {
            if (Catalog.SelfService is { } selfService)
            {
                var status = await selfService.GetTenantAsync(row.TenantId).ConfigureAwait(true);
                row.Regions = TenancyFormat.ResidentRegions(status.Regions);
                row.NoRegionText = TenancyFormat.HasResidency(status.Regions) ? TenancyFormat.NoResidentRegion : TenancyFormat.NoResidency;
            }

            if (Catalog.Quota is { } quota)
            {
                row.Quota = TenancyFormat.QuotaHeadline(await quota.GetQuotaUsageAsync(row.TenantId).ConfigureAwait(true));
            }

            row.Apps = await Catalog.GetInstalledAppCountAsync(row.TenantId, CancellationToken.None).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // A row whose details could not be read says so in place; the list itself stands.
        }
        finally
        {
            row.IsSettled = true;
            StateHasChanged();
        }
    }

    private bool IsActive(TenancyDirectoryRow row) => string.Equals(row.TenantId, Catalog.ActiveTenant, StringComparison.Ordinal);

    private static string Pending(TenancyDirectoryRow row, string? value) => value ?? (row.IsSettled ? "Unknown" : "Reading...");

    private static string AppsText(TenancyDirectoryRow row) => row.Apps switch
    {
        null => "Apps",
        1 => "1 app",
        { } count => $"{TenancyFormat.Count(count)} apps",
    };

    private static string RegionsLabel(TenancyDirectoryRow row, IReadOnlyList<string> regions) => regions.Count == 0
        ? $"{row.NoRegionText}; open the regions of tenant {row.TenantId}"
        : $"Resident in {TenancyFormat.RegionList(regions)}; open the regions of tenant {row.TenantId}";

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private string ResidencyHint => _newAllowed.Count == 0
        ? "Choose allowed regions first: residency is chosen from them."
        : "Where the tenant's data is kept, chosen from its allowed regions. Left blank, it is served in every region.";

    private void OpenCreate()
    {
        _newId = string.Empty;
        _newAdmins = [];
        _newAllowed = [];
        _newResidency = [];
        _idError = null;
        _adminsError = null;
        _allowedError = null;
        _residencyError = null;
        _formError = null;
        _createOpen = true;
    }

    private void AllowedChanged(IReadOnlyList<string> values)
    {
        _newAllowed = values;

        // Residency is chosen from the allowed regions, so a region no longer allowed leaves it too.
        _newResidency = [.. _newResidency.Where(region => values.Contains(region, StringComparer.Ordinal))];
    }

    private void OpenSetRegions()
    {
        var active = Catalog.ActiveTenant;
        _pickedTenant = active is null || string.Equals(active, global::Orleans.Lattice.TenantId.DefaultId, StringComparison.Ordinal) ? string.Empty : active;
        _pickError = null;
        _setRegionsOpen = true;
    }

    private async Task GoToRegionsAsync()
    {
        _pickError = null;
        var id = _pickedTenant.Trim();
        if (id.Length == 0)
        {
            _pickError = "Choose a tenant.";
            return;
        }

        if (_pickBox is not null && !await _pickBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        if (!global::Orleans.Lattice.TenantId.TryParse(id, out _))
        {
            _pickError = "A tenant id is 1 to 63 lower-case letters, digits and hyphens, and does not start or end with a hyphen.";
            return;
        }

        _setRegionsOpen = false;
        Navigator.NavigateTo(TenancyRoutes.TenantRegions(id));
    }

    private async Task CreateAsync()
    {
        if (_saving)
        {
            return;
        }

        _formError = null;
        _adminsError = null;
        _allowedError = null;
        _residencyError = null;
        var id = _newId.Trim();
        _idError = id.Length == 0 ? "Enter the tenant id."
            : !global::Orleans.Lattice.TenantId.TryParse(id, out var parsed) ? "A tenant id is 1 to 63 lower-case letters, digits and hyphens, and does not start or end with a hyphen."
            : parsed.IsDefault ? "The default tenant already exists."
            : _rows?.Any(row => string.Equals(row.TenantId, id, StringComparison.Ordinal)) == true ? $"A tenant with the id {id} already exists."
            : null;
        if (_idError is not null)
        {
            return;
        }

        if (_idBox is not null && !await _idBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        if (_adminsBox is not null && !await _adminsBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        if (_allowedBox is not null && !await _allowedBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        if (_residencyBox is not null && !await _residencyBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        var allowed = _newAllowed.Distinct(StringComparer.Ordinal).ToArray();
        var residency = _newResidency.Distinct(StringComparer.Ordinal).ToArray();
        var outside = residency.Where(region => !allowed.Contains(region, StringComparer.Ordinal)).ToArray();
        if (outside.Length > 0)
        {
            _residencyError = $"Residency is chosen from the allowed regions; {string.Join(", ", outside)} is not allowed.";
            return;
        }

        _pendingId = id;
        _pendingAdmins = [.. _newAdmins.Distinct(StringComparer.Ordinal)];
        _pendingAllowed = allowed;
        _pendingResidency = residency;
        if (residency.Length > 0)
        {
            // A new tenant's residency starts Provisioning, so it is served nowhere until verified backfill completes.
            _createOpen = false;
            _confirmCreate = true;
            return;
        }

        await CreateCoreAsync().ConfigureAwait(true);
    }

    private void BackToCreate()
    {
        _confirmCreate = false;
        _createOpen = true;
    }

    private Task CreateConfirmedAsync() => _saving ? Task.CompletedTask : CreateCoreAsync();

    private async Task CreateCoreAsync()
    {
        var id = _pendingId;
        _saving = true;
        TenantCreationResult created;
        try
        {
            var admin = Catalog.Admin ?? throw new NotSupportedException();
            created = await admin.CreateTenantAsync(id, _pendingAdmins.Count == 0 ? null : _pendingAdmins).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            if (failure.Kind == TenancyFailureKind.Refused || failure.Kind == TenancyFailureKind.Invalid)
            {
                _idError = failure.Message;
            }
            else
            {
                _formError = failure.Message;
            }

            _saving = false;
            BackToCreate();
            return;
        }
        catch
        {
            _saving = false;
            throw;
        }

        _createOpen = false;
        _confirmCreate = false;
        Catalog.Invalidate();
        Suggestions.InvalidateTenants();
        var seeded = created.AdminSubjects.Count == 0 ? "with no admin subject" : "administered by " + string.Join(", ", created.AdminSubjects);
        Toasts.Show($"Tenant {id} created, {seeded}.", LtToastTone.Success);

        try
        {
            var allowedSet = _pendingAllowed.Count == 0 || await AuthorizeNewAsync(id).ConfigureAwait(true);
            if (_pendingResidency.Count > 0)
            {
                await SetNewResidencyAsync(id, allowedSet).ConfigureAwait(true);
            }
        }
        finally
        {
            _saving = false;
        }

        Navigator.NavigateTo(_pendingAllowed.Count + _pendingResidency.Count > 0 ? TenancyRoutes.TenantRegions(id) : TenancyRoutes.Tenant(id));
    }

    private async Task<bool> AuthorizeNewAsync(string id)
    {
        try
        {
            var regions = Catalog.Regions ?? throw new NotSupportedException(TenancyFailure.NotServedMessage);
            var result = await regions.AuthorizeAllowedRegionsAsync(id, _pendingAllowed).ConfigureAwait(true);
            Toasts.Show($"Tenant {id} is allowed {TenancyFormat.RegionList(result.AllowedRegions, "no region")}.", LtToastTone.Success);
            return true;
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            Toasts.Show($"Tenant {id} was created, but its allowed regions were not set: {failure.Message}", LtToastTone.Danger);
            return false;
        }
    }

    private async Task SetNewResidencyAsync(string id, bool allowedSet)
    {
        if (!allowedSet)
        {
            Toasts.Show($"The residency of tenant {id} was not set, because its allowed regions were not.", LtToastTone.Warning);
            return;
        }

        try
        {
            var regions = Catalog.Regions ?? throw new NotSupportedException(TenancyFailure.NotServedMessage);
            var result = await regions.SetResidencyAsync(id, _pendingResidency).ConfigureAwait(true);
            var outcome = $"Tenant {id} is adding {TenancyFormat.RegionList(result.AddedRegions, "no region")}.";
            Toasts.Show(TenancyFormat.IsServedNowhere(result.Regions) ? outcome + " " + TenancyRegions.NotServedYet : outcome, LtToastTone.Success);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            Toasts.Show($"Tenant {id} was created, but its residency was not set: {failure.Message}", LtToastTone.Danger);
        }
    }
}
