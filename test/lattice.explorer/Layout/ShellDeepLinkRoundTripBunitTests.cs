using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.Tests.Tenancy;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Pages;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// <b>The consume half of deep linking.</b> An address is not only produced when
/// a caller opens a tree - loading that address in a fresh shell must arrive at
/// the same view: the row selected, and the detail surface resolved.
/// </summary>
/// <remarks>
/// <para>
/// Producing and consuming are separate capabilities and they failed separately.
/// Copying the link worked while opening it did not, which is the worst shape a
/// deep link can take - the URL looks right and the shell shows nothing.
/// </para>
/// <para>
/// <b>What the rest of the suite could not see.</b> Coverage elsewhere adopts an
/// address <em>after</em> the panel has mounted and loaded. A fresh load is the
/// opposite ordering: the address is in place before any component exists, and
/// the first catalog read is always outstanding when it is adopted. Worse, on a
/// tenant-scoped head that first read answers under the fail-closed scope - the
/// caller's tenant is established after mount - so it legitimately does not
/// contain the row the address names. The row arrives on the reload that follows
/// the scope refresh.
/// </para>
/// <para>
/// The selection is therefore reconciled against the route on <em>every</em>
/// load rather than carried as a one-shot token, and these cases hold that: the
/// token was consumed by the first read that missed, and the link's intent was
/// gone by the time the row appeared.
/// </para>
/// <para>
/// Both halves are asserted every time. "The row is selected" alone would have
/// passed while the detail strip rendered nothing, which is exactly what the
/// journey suite reported.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellDeepLinkRoundTripBunitTests : LatticeComponentTestContext
{
    private const string DetailTabs = "[aria-label='Detail tabs'] [role=tab]";
    private const string ActiveDetailTab = "[aria-label='Detail tabs'] [role=tab][aria-selected=true]";

    private readonly ExplorerTenantContext _tenants = new();

    [Test]
    public void A_deep_link_opened_fresh_selects_its_row_and_resolves_a_surface()
    {
        Configure();
        Navigate("/explore/trees/orders");

        var cut = RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(Selection.Selected?.Id, Is.EqualTo("orders"));
            Assert.That(
                cut.Find(".lx-shell-nav-item.is-selected").TextContent,
                Does.Contain("orders"),
                "and the catalog marks the row");
            Assert.That(
                cut.FindAll(DetailTabs),
                Is.Not.Empty,
                "the detail strip must resolve a surface, not render an empty strip");
        });
    }

    [Test]
    public void A_deep_link_that_names_a_surface_opens_fresh_on_that_surface()
    {
        Configure();
        Navigate("/explore/trees/orders/data");

        var cut = RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(Selection.Selected?.Id, Is.EqualTo("orders"));
            Assert.That(
                cut.Find(ActiveDetailTab).TextContent.Trim(),
                Is.EqualTo("Data"),
                "the link named a surface, so a fresh shell must open on it rather than the default");
        });
    }

    [Test]
    public void A_deep_link_to_a_row_the_first_read_cannot_yet_see_still_arrives()
    {
        // The regression, and the shape a real tenant-scoped head produces: the
        // catalog is read under the caller's scope, which is established after
        // mount, so the first read answers fail-closed and sees nothing at all.
        Configure(scoped: true);
        Navigate("/explore/trees/acme-orders");

        var cut = RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lx-shell-nav-item"), Is.Empty, "the first read sees nothing");
            Assert.That(Selection.Selected, Is.Null);
        });

        ResolveTenantScope(cut, "acme");

        Assert.Multiple(() =>
        {
            Assert.That(
                Selection.Selected?.Id,
                Is.EqualTo("acme-orders"),
                "the row arrived on the reload that followed the scope refresh, and the address still "
                + "asks for it - so it must be selected, not silently forgotten");
            Assert.That(cut.FindAll(DetailTabs), Is.Not.Empty);
        });
    }

    [Test]
    public void A_deep_link_to_a_surface_survives_the_row_arriving_late()
    {
        Configure(scoped: true);
        Navigate("/explore/trees/acme-orders/data");

        var cut = RenderShell();
        ResolveTenantScope(cut, "acme");

        Assert.That(cut.Find(ActiveDetailTab).TextContent.Trim(), Is.EqualTo("Data"));
    }

    [Test]
    public void A_deep_link_to_a_row_that_never_appears_degrades_to_no_selection()
    {
        // The opposite failure, guarded deliberately. Reconciling on every load
        // must not become "hold the address hostage": a deleted tree has to
        // degrade to nothing selected rather than wedging the catalog or
        // selecting something else.
        Configure();
        Navigate("/explore/trees/no-such-tree");

        var cut = RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(Selection.Selected, Is.Null);
            Assert.That(
                cut.FindAll(".lx-shell-nav-item"),
                Is.Not.Empty,
                "the catalog itself still loads and is usable");
            Assert.That(cut.FindAll(".lx-shell-nav-item.is-selected"), Is.Empty);
        });
    }

    [Test]
    public void Following_a_link_to_another_row_after_a_fresh_deep_link_still_moves()
    {
        // Reconciling against the route on every load must not pin the shell to
        // the address it opened on.
        Configure();
        Navigate("/explore/trees/orders");

        var cut = RenderShell();
        var router = Services.GetRequiredService<IExplorerShellRouter>();

        cut.InvokeAsync(() => router.SetAddress("/explore/trees/payments")).GetAwaiter().GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(Selection.Selected?.Id, Is.EqualTo("payments"));
            Assert.That(cut.FindAll(DetailTabs), Is.Not.Empty);
        });
    }

    private IExplorerSelection Selection => Services.GetRequiredService<IExplorerSelection>();

    private void Navigate(string address) =>
        Services.GetRequiredService<NavigationManager>().NavigateTo(address);

    /// <summary>
    /// Establishes the caller's tenant and republishes the projected scope, which
    /// is what <see cref="ExplorerPluginTenantScopeRefresher"/> does once the
    /// identity resolves after mount.
    /// </summary>
    private void ResolveTenantScope(IRenderedComponent<MainLayout> cut, string tenant)
    {
        _tenants.ActiveTenant = new ExplorerTenantId(tenant);

        var host = Services.GetRequiredService<ExplorerPluginHostState>();
        cut.InvokeAsync(() => host.RefreshTenantScopeAsync()).GetAwaiter().GetResult();
    }

    private IRenderedComponent<MainLayout> RenderShell() =>
        Render<MainLayout>(parameters => parameters.Add(
            layout => layout.Body,
            (RenderFragment)(builder =>
            {
                builder.OpenComponent<Home>(0);
                builder.CloseComponent();
            })));

    private void Configure(bool scoped = false)
    {
        // Registered before the shared shell services so these claim the slots.
        Services.AddSingleton<IExplorerSelection>(new ExplorerSelection());

        if (scoped)
        {
            Services.AddSingleton<IExplorerTenantContext>(_tenants);
            Services.AddSingleton<IExplorerTenantView>(
                new ExplorerTenantView(_tenants, new StubOperatorGate(isOperator: true)));
        }

        ConfigureShellServices(
            SelectionPlugin("orleans.lattice.metrics", "Metrics", 100),
            SelectionPlugin("orleans.lattice.data", "Data", 200));

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);

        var session = Substitute.For<IExplorerSession>();
        session.IsConfigured.Returns(true);

        var catalog = Substitute.For<ICatalogReader>();
        catalog
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var kind = call.ArgAt<CatalogKind>(0);

                if (!scoped)
                {
                    return Task.FromResult(new CatalogPage
                    {
                        Items =
                        [
                            new CatalogItem { Id = "orders", Kind = kind },
                            new CatalogItem { Id = "payments", Kind = kind },
                        ],
                    });
                }

                // Fail-closed while no tenant is established, exactly as the real
                // tenant-scoped reader is: nothing is visible until the scope
                // resolves.
                return Task.FromResult(_tenants.ActiveTenant is { } tenant
                    ? new CatalogPage { Items = [new CatalogItem { Id = tenant.Value + "-orders", Kind = kind }] }
                    : new CatalogPage());
            });

        Services.AddSingleton(connection);
        Services.AddSingleton(catalog);
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(session);
        Services.AddSingleton(new SelectionViewLog());

        if (scoped)
        {
            // Registered after the shared shell services so this host state wins:
            // the base harness builds one with no tenant view, which can therefore
            // never publish a scope change.
            Services.AddSingleton(provider => new ExplorerPluginHostState(
                provider.GetRequiredService<IExplorerSelection>(),
                connection,
                provider.GetRequiredService<IExplorerTenantView>()));
        }
    }

    private static IExplorerPlugin SelectionPlugin(string id, string label, int order) =>
        new FakeExplorerPlugin(
            id,
            ExplorerPluginSurface.Selection,
            order,
            label,
            ExplorerPluginAccessGates.Allowed,
            domainContract: null,
            typeof(AlphaProbeView),
            ExplorerPluginSelectionKinds.All);
}
