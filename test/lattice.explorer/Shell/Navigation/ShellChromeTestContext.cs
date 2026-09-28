using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Shell;
using Orleans.Lattice.Explorer.Shell.Layout.Appearance;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Tests.Shell.Design;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>
/// The bUnit context the navigation chrome is tested under: the Shell registered
/// exactly as a head registers it, with a manual clock (so every timeout fires
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
        Services.AddLatticeExplorerShell();
    }

    /// <summary>The clock every chrome timeout is measured on.</summary>
    internal ManualTimeProvider Time { get; }

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
