using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
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
public partial class MyTenantPage : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
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
            _lifetime.Renew();
            Navigation.NotFound();
            return;
        }

        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        var token = _lifetime.Renew();
        var tenantId = TenantId;
        var overview = Section is null;
        _failure = null;
        _standing = null;
        _status = null;
        _quota = null;
        _apps = null;
        _others = [];
        try
        {
            var standing = await Catalog.GetStandingAsync(token).ConfigureAwait(true);
            token.ThrowIfCancellationRequested();
            _standing = standing;
            if (overview)
            {
                await LoadOverviewAsync(tenantId, token).ConfigureAwait(true);
            }
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task LoadOverviewAsync(string tenantId, CancellationToken token)
    {
        var selfService = Catalog.SelfService ?? throw new NotSupportedException();
        var status = await selfService.GetTenantAsync(tenantId, token).ConfigureAwait(true);
        token.ThrowIfCancellationRequested();
        _status = status;
        string? headline = null;
        if (Catalog.Quota is { } quota)
        {
            try
            {
                headline = TenancyFormat.QuotaHeadline(await quota.GetQuotaUsageAsync(tenantId, token).ConfigureAwait(true));
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                headline = null;
            }
        }

        token.ThrowIfCancellationRequested();
        _quota = headline;
        var apps = await Catalog.GetInstalledAppCountAsync(tenantId, token).ConfigureAwait(true);
        token.ThrowIfCancellationRequested();
        _apps = apps;
        IReadOnlyList<TenantDescriptor> others = [];
        try
        {
            var tenants = await Catalog.GetTenantsAsync(token).ConfigureAwait(true);
            others = [.. tenants.Where(tenant => !string.Equals(tenant.TenantId, tenantId, StringComparison.Ordinal))];
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            others = [];
        }

        token.ThrowIfCancellationRequested();
        _others = others;
    }

    /// <summary>Cancels the workspace reads when the page is left.</summary>
    public void Dispose() => _lifetime.Leave();

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();
}
