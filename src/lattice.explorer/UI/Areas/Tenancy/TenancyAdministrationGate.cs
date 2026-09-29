using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The gate every administration page (<c>/tenancy/{tenant}/...</c>) passes
/// first: a platform operator stays, and anyone else - who reaches the same
/// facts through their own tenant's workspace - is sent there, replacing the
/// history entry, so a link to an administration page never dead-ends.
/// </summary>
internal static class TenancyAdministrationGate
{
    /// <summary>
    /// Returns <see langword="true"/> when the caller may stay; otherwise
    /// navigates to <paramref name="workspace"/>'s section for
    /// <paramref name="section"/> and returns <see langword="false"/>.
    /// </summary>
    /// <param name="catalog">The circuit's tenancy catalogue.</param>
    /// <param name="navigator">The navigator.</param>
    /// <param name="workspace">The tenant the administration page is about.</param>
    /// <param name="section">The my-tenant section equivalent to the page, or <see langword="null"/> for the overview.</param>
    /// <exception cref="Exception">The standing could not be proven; classify it with <see cref="TenancyFailure.From"/>.</exception>
    public static async Task<bool> AdmitAsync(TenancyCatalog catalog, ExplorerNavigator navigator, string workspace, string? section)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(navigator);

        var standing = await catalog.GetStandingAsync(CancellationToken.None).ConfigureAwait(true);
        if (standing.IsOperator)
        {
            return true;
        }

        navigator.NavigateTo(TenancyRoutes.MyTenant(string.IsNullOrEmpty(workspace) ? standing.Workspace : workspace, section), replace: true);
        return false;
    }

    /// <summary>The tenant id an administration address names, or an empty string.</summary>
    /// <param name="address">The page's address.</param>
    public static string TenantOf(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Path.Count > 0 ? address.Path[0] : string.Empty;
    }
}
