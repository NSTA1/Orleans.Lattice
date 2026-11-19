using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.App;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The bUnit context the app pages are tested under: the Shell registered as a head
/// registers it (<see cref="ShellChromeTestContext"/>), over fakes of the two app facades
/// the pages read, with the app frame replaced by a stub frame host so a test can read
/// what the page hands the frame and play back what the frame reports.
/// </summary>
public abstract class AppPageTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes and stubs the frame.</summary>
    protected AppPageTestContext()
    {
        Workspace = new FakeAppPagesWorkspace();
        Control = new FakeAppPagesControl();
        Catalog = new Catalogue.FakeAppCatalog();
        Services.AddKeyedSingleton<ILatticeAppWorkspace>(ShellFacades.Key, Workspace);
        Services.AddKeyedSingleton<ILatticeAppsControl>(ShellFacades.Key, Control);
        Services.AddKeyedSingleton<ILatticeAppCatalog>(ShellFacades.Key, Catalog);
        ComponentFactories.AddStub<AppFrame>();
    }

    /// <summary>The caller's workspace.</summary>
    internal FakeAppPagesWorkspace Workspace { get; }

    /// <summary>The app control an <c>AppInstall</c> holder reads.</summary>
    internal FakeAppPagesControl Control { get; }

    /// <summary>The app catalogue, which draws the installed version's icon for an <c>AppInstall</c> holder without a role.</summary>
    internal Catalogue.FakeAppCatalog Catalog { get; }

    /// <summary>Navigates to <paramref name="relative"/> and renders the app page there.</summary>
    /// <param name="relative">The base-relative address, such as <c>apps/crm/trees</c>.</param>
    /// <param name="compact">Whether the layout is in its compact (below 768px) width band.</param>
    /// <param name="shown">The areas the directory shows as visible, so links into them are offered; none cascades no location.</param>
    /// <returns>The rendered page.</returns>
    internal IRenderedComponent<AppPage> RenderAt(string relative, bool compact = false, params IExplorerArea[] shown)
    {
        Navigation.NavigateTo(relative);
        return Render<AppPage>(parameters =>
        {
            if (shown.Length > 0)
            {
                var address = Services.GetRequiredService<ExplorerNavigator>().Current ?? ExplorerAddress.Home;
                var entries = shown.Select(area => new ExplorerAreaEntry(area, AreaAvailability.Visible)).ToArray();
                parameters.AddCascadingValue(new ExplorerLocation(address, entries, EntriesLoaded: true, TenancyActive: address.Tenant is not null));
            }

            if (compact)
            {
                parameters.AddCascadingValue(LtBreakpointCascade.Name, LtBreakpoint.Compact);
            }
        });
    }

    /// <summary>The page's section tabs, by label.</summary>
    /// <param name="cut">The rendered page.</param>
    /// <returns>The tab labels, in order.</returns>
    internal static IReadOnlyList<string> TabLabels(IRenderedComponent<AppPage> cut) =>
        [.. cut.FindAll("[role=tab]").Select(tab => tab.TextContent)];
}
