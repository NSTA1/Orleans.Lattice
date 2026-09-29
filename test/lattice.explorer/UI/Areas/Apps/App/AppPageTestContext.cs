using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.App;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Framing;
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
        Services.AddKeyedSingleton<ILatticeAppWorkspace>(ShellFacades.Key, Workspace);
        Services.AddKeyedSingleton<ILatticeAppsControl>(ShellFacades.Key, Control);
        ComponentFactories.AddStub<AppFrame>();
    }

    /// <summary>The caller's workspace.</summary>
    internal FakeAppPagesWorkspace Workspace { get; }

    /// <summary>The app control an <c>AppInstall</c> holder reads.</summary>
    internal FakeAppPagesControl Control { get; }

    /// <summary>Navigates to <paramref name="relative"/> and renders the app page there.</summary>
    /// <param name="relative">The base-relative address, such as <c>apps/crm/trees</c>.</param>
    /// <param name="compact">Whether the layout is in its compact (below 768px) width band.</param>
    /// <returns>The rendered page.</returns>
    internal IRenderedComponent<AppPage> RenderAt(string relative, bool compact = false)
    {
        Navigation.NavigateTo(relative);
        return compact
            ? Render<AppPage>(parameters => parameters.AddCascadingValue(LtBreakpointCascade.Name, LtBreakpoint.Compact))
            : Render<AppPage>();
    }

    /// <summary>The page's section tabs, by label.</summary>
    /// <param name="cut">The rendered page.</param>
    /// <returns>The tab labels, in order.</returns>
    internal static IReadOnlyList<string> TabLabels(IRenderedComponent<AppPage> cut) =>
        [.. cut.FindAll("[role=tab]").Select(tab => tab.TextContent)];
}
