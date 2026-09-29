using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// "My tenant" (<c>/t/{tenant}/tenancy[/{section}]</c>): the workspace of the
/// tenant the address is rooted at - the active tenant, since the layout has
/// already switched to it or refused. Its overview shows the tenant's state,
/// residency, quota use and installed apps with a link to its Apps area, and the
/// other tenants the caller can reach; its sections are the members, quota,
/// regions and sharing of the tenant. It also serves tenant admins who cannot
/// see the directory.
/// </summary>
public partial class MyTenantPage
{
    private ExplorerAddress? _loaded;
    private TenancyStanding? _standing;
    private TenantStatusReport? _status;
    private string? _quota;
    private int? _apps;
    private IReadOnlyList<TenantDescriptor> _others = [];
    private TenancyFailure? _failure;

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    // Read from the address the page loaded, so a navigation away never re-renders it for another tenant.
    private string TenantId => (_loaded ?? Address).Tenant ?? string.Empty;

    private string? Section => (_loaded ?? Address).Path is { Count: > 0 } path ? path[0] : null;

    private bool OfferRequested => string.Equals((_loaded ?? Address).GetQuery(TenancyRoutes.NewQuery), TenancyRoutes.NewValue, StringComparison.Ordinal);

    private string AppsText => _apps switch
    {
        null => "Installed apps",
        1 => "1 app installed",
        { } count => $"{TenancyFormat.Count(count)} apps installed",
    };

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Equals(_loaded, Address))
        {
            return;
        }

        _loaded = Address;
        if (string.IsNullOrEmpty(TenantId) || Address.Path.Count > 1 || (Section is not null && !TenancyRoutes.IsMyTenantSection(Section)))
        {
            Navigation.NotFound();
            return;
        }

        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _standing = null;
        _status = null;
        try
        {
            _standing = await Catalog.GetStandingAsync(CancellationToken.None).ConfigureAwait(true);
            if (Section is null)
            {
                await LoadOverviewAsync().ConfigureAwait(true);
            }
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task LoadOverviewAsync()
    {
        var selfService = Catalog.SelfService ?? throw new NotSupportedException();
        _status = await selfService.GetTenantAsync(TenantId).ConfigureAwait(true);
        _quota = null;
        if (Catalog.Quota is { } quota)
        {
            try
            {
                _quota = TenancyFormat.QuotaHeadline(await quota.GetQuotaUsageAsync(TenantId).ConfigureAwait(true));
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                _quota = null;
            }
        }

        _apps = await Catalog.GetInstalledAppCountAsync(TenantId, CancellationToken.None).ConfigureAwait(true);
        try
        {
            var tenants = await Catalog.GetTenantsAsync(CancellationToken.None).ConfigureAwait(true);
            _others = [.. tenants.Where(tenant => !string.Equals(tenant.TenantId, TenantId, StringComparison.Ordinal))];
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _others = [];
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();
}
