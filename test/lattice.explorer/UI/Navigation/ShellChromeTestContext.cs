using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Session;
using Orleans.Lattice.Explorer.Tests.UI.Design;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The bUnit context the navigation chrome is tested under: the Shell registered
/// exactly as a head registers it - including S2's real session slots, over the
/// same Core fakes S2's own tests use - with a manual clock (so every timeout fires
/// only when the test advances it), a recording appearance applier, and helpers
/// to add scripted areas and to switch tenancy on.
/// </summary>
/// <remarks>
/// Test doubles are registered before the Shell, so its <c>TryAdd</c>
/// registrations keep them. bUnit locks the service collection at the first
/// render, so a fixture asks for an instance per test case.
/// </remarks>
public abstract class ShellChromeTestContext : ShellDesignTestContext
{
    /// <summary>Registers the Shell over the test doubles.</summary>
    protected ShellChromeTestContext()
    {
        Time = new ManualTimeProvider();
        Applier = new RecordingAppearanceApplier();

        Services.AddSingleton<TimeProvider>(Time);
        Services.AddSingleton<IShellAppearanceApplier>(Applier);

        // The session chrome's Core dependencies, which a head registers and the
        // layout's real session slots read. The session starts configured, so the
        // session overlay's first-run connection dialog stays closed and does not
        // stand in front of the navigation chrome under test.
        Explorer = new FakeExplorerSession(new FakeStateConnection())
            .Configured(SessionTestContext.RemoteConfiguration());
        Services.AddSingleton<IExplorerSession>(Explorer);
        Services.AddSingleton<IExplorerAuthSession>(new FakeAuthSession());
        Services.AddSingleton<IConnectionTester>(new FakeConnectionTester());
        Services.AddSingleton<AntiforgeryStateProvider, FakeAntiforgeryStateProvider>();
        Services.AddSingleton<IExplorerAuthMethod, BasicExplorerAuthMethod>();

        Services.AddLatticeExplorerShell();

        // Chrome tests register their own probe areas; drop the real areas the Shell's area partials add.
        Services.RemoveAll<IExplorerArea>();
    }

    /// <summary>The clock every chrome timeout is measured on.</summary>
    internal ManualTimeProvider Time { get; }

    /// <summary>The Explorer session the session slots read.</summary>
    internal FakeExplorerSession Explorer { get; }

    /// <summary>What the appearance state applied to the document.</summary>
    internal RecordingAppearanceApplier Applier { get; }

    /// <summary>The operator-gated switcher, once tenancy is on.</summary>
    internal IExplorerTenantSwitcher? Switcher { get; private set; }

    /// <summary>The navigation manager the components navigate with.</summary>
    internal NavigationManager Navigation => Services.GetRequiredService<NavigationManager>();

    /// <summary>Registers <paramref name="area"/> as a native area.</summary>
    /// <param name="area">The area.</param>
    /// <returns>The same area, for chaining.</returns>
    internal FakeArea AddArea(FakeArea area)
    {
        Services.AddSingleton<IExplorerArea>(area);
        return area;
    }

    /// <summary>
    /// Turns tenancy on with <paramref name="active"/> as the active tenant and
    /// <paramref name="accessible"/> as the reachable ones. The switcher refuses
    /// every switch unless <paramref name="allowSwitch"/>.
    /// </summary>
    /// <param name="active">The active tenant.</param>
    /// <param name="allowSwitch">Whether the switcher grants a switch.</param>
    /// <param name="accessible">The reachable tenants.</param>
    internal void UseTenancy(string active, bool allowSwitch = false, params string[] accessible)
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        view.ActiveTenant.Returns(new ExplorerTenantId(active));

        var switcher = Substitute.For<IExplorerTenantSwitcher>();
        switcher.SwitchTenantAsync(Arg.Any<ExplorerTenantId>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                if (allowSwitch)
                {
                    view.ActiveTenant.Returns(call.Arg<ExplorerTenantId>());
                }

                return new ValueTask<bool>(allowSwitch);
            });

        var source = Substitute.For<IExplorerAccessibleTenantSource>();
        source.GetAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<IReadOnlyList<ExplorerTenantId>>(
                accessible.Length == 0 ? [new ExplorerTenantId(active)] : [.. accessible.Select(tenant => new ExplorerTenantId(tenant))]));

        Services.AddSingleton(view);
        Services.AddSingleton(switcher);
        Services.AddSingleton(source);
        Switcher = switcher;
    }
}
