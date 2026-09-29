using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// One tenant's administration overview (<c>/tenancy/{tenant}</c>) for a
/// platform operator: its state with suspend (confirmed) and resume, delete
/// (confirmed by typing the tenant id, and cascading to its trees), its regions
/// and installed apps, and its quota with the editor. A tenant the cluster does
/// not list for the caller - one with no admin subject the caller holds - is
/// still administered through its region and quota facades.
/// </summary>
public partial class TenancyTenantPage
{
    private ExplorerAddress? _loaded;
    private TenancyTenantView? _view;
    private TenancyFailure? _failure;
    private int? _apps;
    private bool _busy;
    private bool _confirmSuspend;
    private bool _confirmDelete;

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    // Read from the address the page loaded, so a navigation away never re-renders it for another tenant.
    private string TenantId => TenancyAdministrationGate.TenantOf(_loaded ?? Address);

    private bool IsActive => string.Equals(TenantId, Catalog.ActiveTenant, StringComparison.Ordinal);

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Equals(_loaded, Address))
        {
            return;
        }

        _loaded = Address;
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _view = null;
        var tenantId = TenantId;
        if (!global::Orleans.Lattice.TenantId.TryParse(tenantId, out _))
        {
            Navigation.NotFound();
            return;
        }

        try
        {
            if (!await TenancyAdministrationGate.AdmitAsync(Catalog, Navigator, tenantId, null).ConfigureAwait(true))
            {
                return;
            }

            _view = await ReadAsync(tenantId).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            if (failure.Kind == TenancyFailureKind.NotFound)
            {
                Navigation.NotFound();
                return;
            }

            _failure = failure;
            return;
        }

        _apps = await Catalog.GetInstalledAppCountAsync(tenantId, CancellationToken.None).ConfigureAwait(true);
    }

    private async Task<TenancyTenantView> ReadAsync(string tenantId)
    {
        var selfService = Catalog.SelfService ?? throw new NotSupportedException();
        try
        {
            var status = await selfService.GetTenantAsync(tenantId).ConfigureAwait(true);
            return new TenancyTenantView(status.Status, status.IsDefault, status.Regions);
        }
        catch (TenantNotFoundException) when (Catalog.Regions is { } regions)
        {
            // Self-service lists only tenants whose admin subjects include the
            // caller; an operator still administers the rest, whose existence the
            // region facade establishes (and refuses, not-found, when there is none).
            var report = await regions.GetTenantRegionStatusAsync(tenantId).ConfigureAwait(true);
            return new TenancyTenantView(null, string.Equals(tenantId, global::Orleans.Lattice.TenantId.DefaultId, StringComparison.Ordinal), report.Regions);
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private async Task SuspendAsync()
    {
        _confirmSuspend = false;
        await ChangeStatusAsync(suspend: true).ConfigureAwait(true);
    }

    private Task ResumeAsync() => ChangeStatusAsync(suspend: false);

    private async Task ChangeStatusAsync(bool suspend)
    {
        if (_busy || _view is null)
        {
            return;
        }

        _busy = true;
        try
        {
            var admin = Catalog.Admin ?? throw new NotSupportedException();
            var result = suspend
                ? await admin.SuspendTenantAsync(TenantId).ConfigureAwait(true)
                : await admin.ResumeTenantAsync(TenantId).ConfigureAwait(true);
            _view = _view with { Status = result.NewStatus };
            Catalog.Invalidate();
            var state = TenancyFormat.TenantStateLabel(result.NewStatus).ToLowerInvariant();
            Toasts.Show(result.Changed ? $"Tenant {TenantId} is {state}." : $"Tenant {TenantId} was already {state}.", LtToastTone.Success);
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

    private async Task DeleteAsync()
    {
        _confirmDelete = false;
        if (_busy)
        {
            return;
        }

        _busy = true;
        TenantDeletionResult result;
        try
        {
            var admin = Catalog.Admin ?? throw new NotSupportedException();
            result = await admin.DeleteTenantAsync(TenantId).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
            return;
        }
        finally
        {
            _busy = false;
        }

        Catalog.Invalidate();
        var trees = result.CascadedTreeCount == 1 ? "1 tree" : $"{TenancyFormat.Count(result.CascadedTreeCount)} trees";
        Toasts.Show($"Tenant {TenantId} deleted; {trees} soft-deleted.", LtToastTone.Success);
        Navigator.NavigateTo(TenancyRoutes.Directory);
    }
}
