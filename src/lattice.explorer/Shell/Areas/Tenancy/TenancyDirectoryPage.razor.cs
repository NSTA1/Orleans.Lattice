using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// The tenant directory (<c>/tenancy</c>) for a platform operator: every tenant
/// the caller can reach with its state, then its quota use, residency and
/// installed apps as each is read, a search, and a "New tenant" form - the
/// visible control of the palette's create command. The active tenant is marked
/// as current. A caller without operator standing is sent, replacing the
/// history entry, to its own tenant's workspace.
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
    private string _newAdmins = string.Empty;
    private string? _idError;
    private string? _adminsError;
    private string? _formError;

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

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

    private static string AppsLabel(TenancyDirectoryRow row) => $"{AppsText(row)} installed for tenant {row.TenantId}";

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private void OpenCreate()
    {
        _newId = string.Empty;
        _newAdmins = string.Empty;
        _idError = null;
        _adminsError = null;
        _formError = null;
        _createOpen = true;
    }

    private async Task CreateAsync()
    {
        if (_saving)
        {
            return;
        }

        _formError = null;
        _adminsError = null;
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

        var admins = _newAdmins
            .Split([',', ';', '\n', '\r'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        _saving = true;
        TenantCreationResult created;
        try
        {
            var admin = Catalog.Admin ?? throw new NotSupportedException();
            created = await admin.CreateTenantAsync(id, admins.Length == 0 ? null : admins).ConfigureAwait(true);
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

            return;
        }
        finally
        {
            _saving = false;
        }

        _createOpen = false;
        Catalog.Invalidate();
        var seeded = created.AdminSubjects.Count == 0 ? "with no admin subject" : "administered by " + string.Join(", ", created.AdminSubjects);
        Toasts.Show($"Tenant {id} created, {seeded}.", LtToastTone.Success);
        Navigator.NavigateTo(TenancyRoutes.Tenant(id));
    }
}
