using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// A tenant's regions (<c>/tenancy/{tenant}/regions</c>) for a platform operator: its residency and the allowed-region set only an operator may change. Anyone else is sent to the same section of their tenant's workspace.
/// </summary>
public partial class TenancyRegionsPage
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
            _admitted = await TenancyAdministrationGate.AdmitAsync(Catalog, Navigator, TenantId, TenancyRoutes.RegionsSegment).ConfigureAwait(true);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }
}
