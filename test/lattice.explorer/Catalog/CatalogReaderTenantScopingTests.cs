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
}
