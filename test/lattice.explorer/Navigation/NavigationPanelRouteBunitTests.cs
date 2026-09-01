using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Tenancy;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// <b>Selection is addressable.</b> The catalog publishes what is selected - and
/// which catalog it is selected from - into the route, and follows the route when
/// it changes underneath: a deep link, Back, Forward, or a restore.
/// </summary>
/// <remarks>
/// <para>
/// The epic shipped the route grammar and declared the <c>shell.selection</c> and
/// <c>shell.catalog-kind</c> keys, but nothing produced them:
/// <c>ExplorerRoute.WithSelection</c> was called from exactly one place in the
/// whole product - the preference store reading its own remembered value back -
/// and the catalog held its state in an ad hoc <c>nav-selected</c> key instead.
/// Opening a tree therefore left the address at <c>/</c>, and
/// <c>/explore/trees/{id}</c> rendered "Nothing selected". Every case here fails
/// against that shape.
/// </para>
/// <para>
/// Everything runs on the renderer's dispatcher and the router compares routes by
/// value, so a navigation and its echo are told apart structurally. No delay, no
/// polling and no ordering assumption appears anywhere in this fixture. Browser
/// Back is modelled the way the head models it - a location change fed into
/// <see cref="IExplorerShellRouter.SetAddress"/> - because that is literally all
/// Back is to this shell.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class NavigationPanelRouteBunitTests : BunitContext
{
    private readonly FakeExplorerSelection _selection = new();
    private readonly ExplorerTenantContext _tenants = new();
    private readonly List<string> _addresses = [];

    private ScopedCatalogReader _catalog = default!;
    private IExplorerShellRouter _router = default!;

    [Test]
    public void Selecting_a_tree_publishes_a_selection_route()
    {
        Configure();
        var cut = Render<NavigationPanel>();

        cut.FindAll(".lx-shell-nav-item").First(item => item.TextContent.Contains("orders")).Click();

        Assert.Multiple(() =>
        {
            Assert.That(_router.Current.Kind, Is.EqualTo(ExplorerRouteSegments.Trees));
            Assert.That(_router.Current.Id, Is.EqualTo("orders"));
            Assert.That(
                _addresses,
                Does.Contain("/explore/trees/orders"),
                "the head is asked to put the selection in the address bar");
        });
    }

    [Test]
    public void A_deep_link_to_a_tree_restores_that_selection()
    {
        Configure(address: "/explore/trees/payments");

        var cut = Render<NavigationPanel>();

        Assert.Multiple(() =>
        {
            Assert.That(_selection.Selected?.Id, Is.EqualTo("payments"));
            Assert.That(
                cut.Find(".lx-shell-nav-item.is-selected").TextContent,
                Does.Contain("payments"),
                "and the list shows it as the selected row");
        });
    }

    [Test]
    public void A_deep_link_to_a_catalog_kind_opens_on_that_kind()
    {
        Configure(address: "/explore/views");

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find("[role=tab][aria-selected=true]").TextContent.Trim(),
            Is.EqualTo("Views"));
    }

    [Test]
    public void Switching_the_catalog_kind_publishes_it()
    {
        Configure();
        var cut = Render<NavigationPanel>();

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent.Trim() == "Views").Click();

        Assert.Multiple(() =>
        {
            Assert.That(_router.Current.Kind, Is.EqualTo(ExplorerRouteSegments.Views));
            Assert.That(_addresses, Does.Contain("/explore/views"));
            Assert.That(
                cut.Find("[role=tab][aria-selected=true]").TextContent.Trim(),
                Is.EqualTo("Views"),
                "the toggle follows the route it published");
        });
    }

    [Test]
    public async Task Back_returns_to_the_previously_selected_tree()
    {
        Configure();
        var cut = Render<NavigationPanel>();

        cut.FindAll(".lx-shell-nav-item").First(item => item.TextContent.Contains("orders")).Click();
        cut.FindAll(".lx-shell-nav-item").First(item => item.TextContent.Contains("payments")).Click();

        // Back is a location change and nothing else, so that is exactly how it
        // is driven: no history stack to simulate and no timing to depend on.
        await cut.InvokeAsync(() => _router.SetAddress("/explore/trees/orders"));

        Assert.Multiple(() =>
        {
            Assert.That(_selection.Selected?.Id, Is.EqualTo("orders"));
            Assert.That(
                cut.Find(".lx-shell-nav-item.is-selected").TextContent,
                Does.Contain("orders"));
        });
    }

    [Test]
    public async Task Backing_out_of_a_selection_clears_it()
    {
        Configure();
        var cut = Render<NavigationPanel>();

        cut.FindAll(".lx-shell-nav-item").First(item => item.TextContent.Contains("orders")).Click();
        await cut.InvokeAsync(() => _router.SetAddress("/explore/trees"));

        Assert.Multiple(() =>
        {
            Assert.That(_selection.Selected, Is.Null);
            Assert.That(cut.FindAll(".lx-shell-nav-item.is-selected"), Is.Empty);
        });
    }

    [Test]
    public async Task A_tenant_switch_re_scopes_the_catalog_without_a_reload()
    {
        Configure(tenant: "acme");
        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.FindAll(".lx-shell-nav-item").Select(item => item.TextContent),
            Has.Some.Contains("acme-orders"),
            "the catalog opens scoped to the caller's tenant");

        // The real switch path: the switcher mutates the per-circuit context and
        // raises the scope refresher, which republishes the projected scope. No
        // page reload, and nothing here waits on one.
        var switcher = ActiveSwitcher();
        await cut.InvokeAsync(() => switcher.SwitchTenantAsync(new ExplorerTenantId("globex")).AsTask());

        var rows = cut.FindAll(".lx-shell-nav-item").Select(item => item.TextContent).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                rows,
                Has.Some.Contains("globex-orders"),
                "the listing follows the switch in place");
            Assert.That(
                rows,
                Has.None.Contains("acme-orders"),
                "and stops describing the tenant the caller just left");
        });
    }

    private ExplorerTenantSwitcher ActiveSwitcher()
    {
        var view = new ExplorerTenantView(_tenants, new StubOperatorGate(isOperator: true));
        var hostState = Services.GetRequiredService<ExplorerPluginHostState>();

        return new ExplorerTenantSwitcher(
            view,
            _tenants,
            new StubOperatorGate(isOperator: true),
            new ExplorerPluginTenantScopeRefresher(
                hostState,
                () => Services.GetRequiredService<IExplorerPluginAccessRefresher>()));
    }

    private void Configure(string? address = null, string? tenant = null)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        if (tenant is not null)
        {
            _tenants.ActiveTenant = new ExplorerTenantId(tenant);
        }

        _catalog = new ScopedCatalogReader(_tenants);

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);

        var hostState = new ExplorerPluginHostState(
            _selection,
            connection,
            new ExplorerTenantView(_tenants, new StubOperatorGate(isOperator: true)));

        Services.AddSingleton<ICatalogReader>(_catalog);
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton<IExplorerSelection>(_selection);
        Services.AddSingleton(Substitute.For<IExplorerSession>());
        Services.AddSingleton(hostState);
        Services.AddSingleton(Substitute.For<IExplorerPluginAccessRefresher>());
        Services.AddExplorerSession();

        _router = Services.GetRequiredService<IExplorerShellRouter>();
        _router.NavigationRequested += request => _addresses.Add(request.Address);

        if (address is not null)
        {
            _router.SetAddress(address);
        }
    }

    /// <summary>
    /// A catalog whose rows depend on the caller's active tenant, so a scope
    /// change is observable as different content rather than only as a reload
    /// count - which is what the defect was about.
    /// </summary>
    private sealed class ScopedCatalogReader(IExplorerTenantContext tenants) : ICatalogReader
    {
        public Task<CatalogPage> LoadAsync(
            CatalogKind kind,
            string? pageToken,
            int pageSize,
            CancellationToken cancellationToken = default)
        {
            var prefix = tenants.ActiveTenant is { } active ? active.Value + "-" : string.Empty;

            return Task.FromResult(new CatalogPage
            {
                Items =
                [
                    new CatalogItem { Id = prefix + "orders", Kind = kind },
                    new CatalogItem { Id = prefix + "payments", Kind = kind },
                ],
            });
        }
    }
}
