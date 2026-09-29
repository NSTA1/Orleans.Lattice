using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// A tenant's cross-tenant grants (<c>/tenancy/{tenant}/grants</c>) for a platform operator: the offers made to it and by it, with every verb of the two-step agreement. Anyone else is sent to the same section of their tenant's workspace.
/// </summary>
public partial class TenancyGrantsPage
{
    private ExplorerAddress? _loaded;
    private bool _admitted;
    private TenancyFailure? _failure;

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    // Read from the address the page loaded, so a navigation away never re-renders it for another tenant.
    private string TenantId => TenancyAdministrationGate.TenantOf(_loaded ?? Address);

    private string DirectoryHref => Navigator.Canonicalize(TenancyRoutes.Directory).ToHref();

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Equals(_loaded, Address))
        {
            return;
        }

        _loaded = Address;
        _admitted = false;
        _failure = null;
        if (!global::Orleans.Lattice.TenantId.TryParse(TenantId, out _))
        {
            Navigation.NotFound();
            return;
        }

        try
        {
            _admitted = await TenancyAdministrationGate.AdmitAsync(Catalog, Navigator, TenantId, TenancyRoutes.SharingSegment).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }
}
