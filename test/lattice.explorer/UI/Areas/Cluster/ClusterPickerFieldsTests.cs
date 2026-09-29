using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// Issue #3949: every Cluster field that names an existing tree or provider key is
/// a type-ahead picker over the area's own tree catalogue or the WAL placement
/// audit; the snapshot destination must be new, so an existing tree is refused.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterPickerFieldsTests : ClusterTestContext
{
    private const string Orders = "a/crm/orders";

    [Test]
    public void Orphans_offer_the_catalogues_trees_and_refuse_one_that_does_not_exist()
    {
        UseTrees(Tree(Orders), Tree("a/crm/customers"), Tree("billing"));
        var cut = RenderAt("/cluster/orphans");

        Assert.That(SuggestionFields.Offers(cut, "Tree", "a/crm"), Is.EqualTo(new[] { "a/crm/customers", Orders }));

        SuggestionFields.Box(cut, "Tree").Input("a/crm/order");
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Is.EqualTo("No tree is named a/crm/order. Choose one from the list.")));
        Assert.That(Navigation.Uri, Does.EndWith("/cluster/orphans"));
    }

    [Test]
    public void The_wal_plan_offers_the_known_provider_keys_and_refuses_an_unknown_one()
    {
        UseTrees(Tree(Orders));
        Admin.AuditWalPlacementAsync(Orders, Arg.Any<CancellationToken>())
            .Returns(new TreeWalPlacementAudit { TreeId = Orders, PartitionCount = 1, KnownProviderKeys = ["blob-a", "blob-b"] });
        var cut = RenderAt("/cluster/wal");
        ClusterTestContext.Button(cut, "Plan a move...").Click();
        cut.FindAll(".lt-dialog input")[0].Input(Orders);

        Assert.That(SuggestionFields.Offers(cut, "Target provider key", "blob", atLeast: 2), Is.EqualTo(new[] { "blob-a", "blob-b" }));

        cut.FindAll(".lt-dialog input")[1].Input("0");
        SuggestionFields.Box(cut, "Target provider key").Input("blob-z");
        cut.Find(".lt-dialog form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Target provider key"), Is.EqualTo("No provider key is named blob-z. Choose one from the list.")));
    }

    [Test]
    public void The_provider_key_source_asks_for_a_tree_first_and_fails_closed_to_a_note()
    {
        var facades = new ClusterFacades(Services);
        string? tree = null;
        var source = new ClusterProviderKeySuggestionSource(facades, () => tree);

        var noTree = source.SuggestAsync(string.Empty, 5, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        tree = "broken";
        Admin.AuditWalPlacementAsync("broken", Arg.Any<CancellationToken>()).Returns<TreeWalPlacementAudit>(_ => throw new InvalidOperationException("down"));
        var failed = source.SuggestAsync(string.Empty, 5, CancellationToken.None).AsTask().GetAwaiter().GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(noTree.UnavailableReason, Is.EqualTo(ClusterProviderKeySuggestionSource.NoTreeReason));
            Assert.That(failed.UnavailableReason, Is.EqualTo(ClusterProviderKeySuggestionSource.UnavailableReason));
        });
    }

    [Test]
    public void A_snapshot_destination_that_already_exists_is_refused()
    {
        UseTrees(Tree(Orders), Tree("a/crm/orders-copy"));
        Admin.GetSnapshotStatusAsync(Orders, Arg.Any<CancellationToken>()).Returns(new TreeSnapshotStatus { TreeId = Orders });
        var cut = RenderAt("/cluster/trees/a/crm/orders/snapshot");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No snapshot is running.")));

        SuggestionFields.Box(cut, "Destination tree").Input("a/crm/orders-copy");
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Destination tree"), Is.EqualTo("A tree with this name already exists; a snapshot needs a new one.")));
        Assert.That(cut.FindAll(".lt-cluster-review"), Is.Empty);

        SuggestionFields.Box(cut, "Destination tree").Input("a/crm/orders-new");
        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-cluster-review"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void The_alias_target_offers_trees_and_refuses_one_that_does_not_exist()
    {
        UseTrees(Tree(Orders), Tree("a/crm/orders-v2"));
        Admin.GetTreeDeletionStatusAsync(Orders, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = Orders });
        var cut = Render<Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeLifecycle>(parameters => parameters
            .Add(tab => tab.TreeId, Orders)
            .Add(tab => tab.Capabilities, new LatticeTreeAdminCapabilities
            {
                TreeId = Orders,
                Schema = new Orleans.Lattice.Api.Schema.LatticeSchemaCapabilities { TreeId = Orders },
                CanViewDiagnostics = true,
                CanAdministerTree = true,
                CanManageTreeLifecycle = true,
            }));
        cut.WaitUntil(() => Assert.That(cut.FindAll("form[aria-label='Set alias']"), Has.Count.EqualTo(1)));

        Assert.That(SuggestionFields.Offers(cut, "Target tree", "a/crm/orders-"), Is.EqualTo(new[] { "a/crm/orders-v2" }));

        SuggestionFields.Box(cut, "Target tree").Input("a/crm/orders-v3");
        cut.Find("form[aria-label='Set alias']").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Target tree"), Does.StartWith("No tree is named a/crm/orders-v3.")));
        Assert.That(cut.FindAll(".lt-confirm__consequence"), Is.Empty);
    }

    [Test]
    public void The_reshard_picker_offers_the_catalogues_trees()
    {
        UseTrees(Tree(Orders), Tree("billing"));
        var cut = RenderAt("/cluster/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-command]"), Is.Not.Empty));
        cut.Find($"[data-lt-command=\"{ClusterArea.ReshardCommandId}\"]").Click();

        Assert.That(SuggestionFields.Offers(cut, "Tree", "bil"), Is.EqualTo(new[] { "billing" }));
    }

    [Test]
    public void The_cluster_tree_source_fails_closed_without_a_connection()
    {
        var source = new ClusterTreeSuggestionSource(new ClusterTreeCatalog(new ClusterFacades(new ServiceProviderStub()), TimeProvider.System));

        var answer = source.SuggestAsync("a", 5, CancellationToken.None).AsTask().GetAwaiter().GetResult();

        Assert.That(answer.UnavailableReason, Is.EqualTo(ClusterTreeSuggestionSource.UnavailableReason));
    }

    /// <summary>A service provider that resolves nothing: no session, no facade.</summary>
    private sealed class ServiceProviderStub : IServiceProvider, Microsoft.Extensions.DependencyInjection.IKeyedServiceProvider
    {
        public object? GetService(Type serviceType) => null;

        public object? GetKeyedService(Type serviceType, object? serviceKey) => null;

        public object GetRequiredKeyedService(Type serviceType, object? serviceKey) => throw new InvalidOperationException();
    }
}
