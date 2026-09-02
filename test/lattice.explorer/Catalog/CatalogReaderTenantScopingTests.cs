using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.Tests.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Catalog;

/// <summary>
/// Verifies that <see cref="CatalogReader"/> scopes the trees catalog through an
/// active <see cref="IExplorerTenantView"/> and is byte-for-byte unchanged when no
/// view (or the inactive view) is supplied.
/// </summary>
[TestFixture]
public class CatalogReaderTenantScopingTests
{
    private static TreeCatalogEntry Tree(string id) => new()
    {
        TreeId = id,
        Lifecycle = TreeLifecycleState.Active,
        ShardCount = 1,
        Config = new TreeConfigSummary { ShardCount = 1 },
    };

    private static FakeStateClient ClientWithTrees(params string[] ids) => new()
    {
        ListTreesHandler = _ => Task.FromResult(new TreeCatalogPage
        {
            Entries = ids.Select(Tree).ToArray(),
            NextPageToken = null,
        }),
    };

    private static ExplorerTenantView ActiveView(ExplorerTenantId? activeTenant, ExplorerTenantVisibility requested, bool isOperator)
    {
        var context = new ExplorerTenantContext
        {
            ActiveTenant = activeTenant,
            RequestedVisibility = requested,
        };
        return new ExplorerTenantView(context, new StubOperatorGate(isOperator));
    }

    [Test]
    public async Task LoadAsync_Trees_noTenantView_returnsEveryTreeUnchanged()
    {
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders", "legacy");
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.Items.Select(i => i.Id), Is.EqualTo(new[] { "t/acme/orders", "t/globex/orders", "legacy" }));
    }

    [Test]
    public async Task LoadAsync_Trees_inactiveView_returnsEveryTreeUnchanged()
    {
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders");
        var reader = new CatalogReader(client, NullExplorerTenantView.Instance);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.Items.Select(i => i.Id), Is.EqualTo(new[] { "t/acme/orders", "t/globex/orders" }));
    }

    [Test]
    public async Task LoadAsync_Trees_activeTenantView_scopesToOwnTenant()
    {
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders", "legacy");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.ActiveTenant, isOperator: false);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.Items.Select(i => i.Id), Is.EqualTo(new[] { "t/acme/orders" }));
    }

    [Test]
    public async Task LoadAsync_Trees_operatorAllTenantView_returnsEveryTree()
    {
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders", "legacy");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.AllTenants, isOperator: true);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.Items.Select(i => i.Id), Is.EqualTo(new[] { "t/acme/orders", "t/globex/orders", "legacy" }));
    }

    [Test]
    public async Task LoadAsync_Trees_nonOperatorRequestingAllTenants_failsClosedToOwnTenant()
    {
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.AllTenants, isOperator: false);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.Items.Select(i => i.Id), Is.EqualTo(new[] { "t/acme/orders" }));
    }

    [Test]
    public async Task LoadAsync_Trees_activeViewPreservesNextPageToken()
    {
        var client = new FakeStateClient
        {
            ListTreesHandler = _ => Task.FromResult(new TreeCatalogPage
            {
                Entries = new[] { Tree("t/acme/orders") },
                NextPageToken = "cursor",
            }),
        };
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.ActiveTenant, isOperator: false);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.NextPageToken, Is.EqualTo("cursor"));
    }

    [Test]
    public async Task LoadAsync_Trees_activeViewThatFilteredNothing_reportsAZeroScopeCount()
    {
        // A tenant-scoped cluster that genuinely holds only this tenant's trees
        // must not claim a filter removed something. The count is the fact that
        // separates "your scope is hiding trees" from "there are none".
        var client = ClientWithTrees("t/acme/orders", "t/acme/invoices");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.ActiveTenant, isOperator: false);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.ScopeFilteredCount, Is.Zero);
            Assert.That(page.ScopedToTenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public async Task LoadAsync_Trees_activeViewThatFilteredEverything_reportsTheCountAndTheTenant()
    {
        var client = ClientWithTrees("t/globex/orders", "t/initech/orders");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.ActiveTenant, isOperator: false);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.Items, Is.Empty);
            Assert.That(page.ScopeFilteredCount, Is.EqualTo(2));
            Assert.That(page.ScopedToTenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public async Task LoadAsync_Trees_activeViewThatFilteredSome_reportsOnlyWhatItRemoved()
    {
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders", "legacy");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.ActiveTenant, isOperator: false);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.ScopeFilteredCount, Is.EqualTo(2));
    }

    [Test]
    public async Task LoadAsync_Trees_noTenantView_reportsNoScopeAtAll()
    {
        // The non-tenant path must stay byte-for-byte what it was, so an
        // untenanted cluster can never be told a scope filtered its catalog.
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders");
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.ScopedToTenantId, Is.Null);
            Assert.That(page.ScopeFilteredCount, Is.Zero);
        });
    }

    [Test]
    public async Task LoadAsync_Trees_inactiveView_reportsNoScopeAtAll()
    {
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders");
        var reader = new CatalogReader(client, NullExplorerTenantView.Instance);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.ScopedToTenantId, Is.Null);
            Assert.That(page.ScopeFilteredCount, Is.Zero);
        });
    }

    [Test]
    public async Task LoadAsync_Trees_operatorViewingAllTenants_reportsNothingFiltered()
    {
        // The view is active, so the tenant is reported, but the all-tenant
        // toggle removed nothing and must not be described as a filter.
        var client = ClientWithTrees("t/acme/orders", "t/globex/orders", "legacy");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.AllTenants, isOperator: true);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.ScopeFilteredCount, Is.Zero);
    }

    [TestCase(CatalogKind.Views)]
    [TestCase(CatalogKind.TagIndexes)]
    public async Task LoadAsync_kindsThatAreNotTenantScoped_reportNoScope(CatalogKind kind)
    {
        var client = ClientWithTrees("t/acme/orders");
        var view = ActiveView(new ExplorerTenantId("acme"), ExplorerTenantVisibility.ActiveTenant, isOperator: false);
        var reader = new CatalogReader(client, view);

        var page = await reader.LoadAsync(kind, pageToken: null, pageSize: 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.ScopedToTenantId, Is.Null);
            Assert.That(page.ScopeFilteredCount, Is.Zero);
        });
    }
}
