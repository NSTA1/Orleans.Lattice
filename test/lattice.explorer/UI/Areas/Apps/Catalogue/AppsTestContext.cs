using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The bUnit context the Apps catalogue is tested under: the Shell registered as a
/// head registers it (over S2's Core session fakes), with scripted fakes of every
/// facade the area reads registered after it, so no probe ever dials a transport.
/// </summary>
public abstract class AppsTestContext : ShellChromeTestContext
{
    /// <summary>Registers the fakes.</summary>
    protected AppsTestContext()
    {
        Catalog = new FakeAppCatalog();
        Control = new FakeAppsControl();
        Workspace = new FakeAppsWorkspace();
        Auth = Substitute.For<ILatticeAuthAdmin>();

        Services.AddKeyedSingleton<ILatticeAppCatalog>(ShellFacades.Key, Catalog);
        Services.AddKeyedSingleton<ILatticeAppsControl>(ShellFacades.Key, Control);
        Services.AddKeyedSingleton<ILatticeAppRoleBindings>(ShellFacades.Key, Control);
        Services.AddKeyedSingleton<ILatticeAppWorkspace>(ShellFacades.Key, Workspace);
        Services.AddKeyedSingleton(ShellFacades.Key, Auth);

        // The chrome context may clear the real areas so fake areas cannot collide
        // on a key; these tests are about the real Apps area, so it is registered
        // again (idempotent when it is still there).
        Services.AddExplorerArea<AppsArea>();
    }

    /// <summary>The scripted catalogue.</summary>
    internal FakeAppCatalog Catalog { get; }

    /// <summary>The scripted lifecycle facade, which is also the scripted role re-binding facade.</summary>
    internal FakeAppsControl Control { get; }

    /// <summary>The scripted workspace.</summary>
    internal FakeAppsWorkspace Workspace { get; }

    /// <summary>The auth facade whose group search binds roles.</summary>
    internal ILatticeAuthAdmin Auth { get; }

    /// <summary>
    /// Makes the caller a restricted identity: no <c>AppInstall</c>, so the
    /// catalogue and control facades grant nothing.
    /// </summary>
    internal void Restrict()
    {
        Catalog.Capabilities = new LatticeAppCatalogCapabilities();
        Control.Capabilities = new LatticeAppsCapabilities();
    }

    /// <summary>Renders <typeparamref name="TPage"/> at <paramref name="address"/>, as the layout would.</summary>
    /// <typeparam name="TPage">The page.</typeparam>
    /// <param name="address">The canonical address.</param>
    /// <param name="breakpoint">The width band the layout cascades.</param>
    internal IRenderedComponent<TPage> RenderAt<TPage>(string address, LtBreakpoint breakpoint = LtBreakpoint.Expanded)
        where TPage : IComponent
    {
        var parsed = ExplorerAddress.Parse(address);
        Navigation.NavigateTo(parsed.ToHref());
        var location = new ExplorerLocation(parsed, [], EntriesLoaded: true, TenancyActive: parsed.Tenant is not null);
        return Render<TPage>(parameters => parameters
            .AddCascadingValue(location)
            .AddCascadingValue(LtBreakpointCascade.Name, breakpoint));
    }
}
