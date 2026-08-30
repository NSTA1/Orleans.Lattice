using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Fail-closed visibility and best-effort cancellation edge cases for state API reads.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    [Test]
    public async Task Tag_catalog_verbs_return_empty_pages_when_no_tag_index_factory_is_registered()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>("tree-a").Returns(tree);
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);
        var query = CreateQuery(grainFactory: grainFactory);

        var tagValues = await query.ListTagValuesAsync(new CatalogRequest { SourceTreeId = "tree-a", IndexName = "by-status" });
        var coveredTrees = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = "by-status" });
        var indexTags = await query.ListIndexTagsAsync(new CatalogRequest { IndexName = "by-status" });
        var tagMembers = await query.ScanTagMembersAsync(new TagMemberScanRequest { IndexName = "by-status", Tag = "open" });

        Assert.Multiple(() =>
        {
            Assert.That(tagValues.Entries, Is.Empty);
            Assert.That(coveredTrees.Entries, Is.Empty);
            Assert.That(indexTags.Entries, Is.Empty);
            Assert.That(tagMembers.Entries, Is.Empty);
        });
    }


    [Test]
    public async Task ListIndexTagsAsync_hides_the_index_when_any_covered_tree_is_unreadable()
    {
        var index = Substitute.For<ILatticeMultiTreeTagIndex>();
        index.CoveredTreesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["allowed", "denied"]));
        var tagFactory = Substitute.For<ILatticeTagIndexFactory>();
        tagFactory.CreateMultiTree("by-status").Returns(index);

        var indexTree = Substitute.For<ILattice>();
        indexTree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(LatticeConstants.TagIndexTreePrefix + "by-status").Returns(indexTree);

        var services = new ServiceCollection();
        services.AddSingleton(tagFactory);
        services.AddSingleton<ILatticeAccessGate>(new AllowMatchingGate(treeId => treeId == "allowed"));
        services.AddSingleton<ILatticeMembershipContext>(new FixedMembership(NamedSubject));
        var query = CreateQuery(services: services.BuildServiceProvider(), grainFactory: grainFactory);

        var page = await query.ListIndexTagsAsync(new CatalogRequest { IndexName = "by-status" });

        Assert.That(page.Entries, Is.Empty);
    }

    [Test]
    public async Task Shard_summary_reads_fail_closed_when_visibility_hides_the_tree()
    {
        var query = CreateQuery(services: VisibilityServices(_ => false));

        var summaries = await query.GetShardSummariesAsync("hidden");
        var physicalCount = await query.GetPhysicalShardCountAsync("hidden");

        Assert.Multiple(() =>
        {
            Assert.That(summaries.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(physicalCount, Is.Null);
        });
    }


    [Test]
    public async Task Any_covered_tree_readable_returns_as_soon_as_one_tree_is_allowed()
    {
        var query = CreateQuery(services: VisibilityServices(treeId => treeId == "allowed"));

        var readable = await InvokeInstanceAsync<bool>(
            query,
            "AnyCoveredTreeReadableAsync",
            (IReadOnlyList<string>)["denied", "allowed", "unreached"],
            NamedSubject,
            CancellationToken.None);

        var unreadable = await InvokeInstanceAsync<bool>(
            query,
            "AnyCoveredTreeReadableAsync",
            (IReadOnlyList<string>)["denied-a", "denied-b"],
            NamedSubject,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(readable, Is.True);
            Assert.That(unreadable, Is.False);
        });
    }

    [Test]
    public async Task ListTagValuesAsync_hides_tags_for_an_unreadable_source_tree()
    {
        var query = CreateQuery(services: VisibilityServices(_ => false));

        var page = await query.ListTagValuesAsync(new CatalogRequest
        {
            SourceTreeId = "hidden",
            IndexName = "by-status",
        });

        Assert.That(page.Entries, Is.Empty);
    }

    [Test]
    public async Task ScanTagMembersAsync_returns_an_empty_page_for_anonymous_subjects()
    {
        var tagFactory = Substitute.For<ILatticeTagIndexFactory>();
        var indexTree = Substitute.For<ILattice>();
        indexTree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(LatticeConstants.TagIndexTreePrefix + "by-status").Returns(indexTree);

        var services = new ServiceCollection();
        services.AddSingleton(tagFactory);
        services.AddSingleton<ILatticeAccessGate>(new AllowMatchingGate(_ => true));
        services.AddSingleton<ILatticeMembershipContext>(new FixedMembership(LatticeSubject.Anonymous));
        var query = CreateQuery(services: services.BuildServiceProvider(), grainFactory: grainFactory);

        var page = await query.ScanTagMembersAsync(new TagMemberScanRequest
        {
            IndexName = "by-status",
            Tag = "open",
        });

        Assert.That(page.Entries, Is.Empty);
    }

    [Test]
    public async Task ScanEntriesAsync_reports_not_found_when_a_view_has_no_read_tree()
    {
        var query = CreateQuery();

        var result = await query.ScanEntriesAsync(new EntryScanRequest { TreeId = "view-" });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
    }

    [Test]
    public async Task ScanEntriesAsync_tag_filter_returns_empty_when_tag_factory_is_absent()
    {
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("tree").Returns(tree);
        var query = CreateQuery(grainFactory: grainFactory);

        var result = await query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = "tree",
            IndexName = "by-status",
            Tag = "open",
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task ScanEntriesAsync_tag_filter_reports_not_found_when_the_source_tree_is_missing()
    {
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(false);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("missing").Returns(tree);
        var query = CreateQuery(grainFactory: grainFactory);

        var result = await query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = "missing",
            IndexName = "by-status",
            Tag = "open",
        });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
    }

    [Test]
    public async Task Entry_history_and_dead_letter_reads_hide_unreadable_trees()
    {
        var query = CreateQuery(services: VisibilityServices(_ => false));

        var history = await query.GetEntryHistoryAsync(new EntryHistoryRequest
        {
            TreeId = "hidden",
            Key = "key",
        });
        var count = await query.GetDeadLetterCountAsync("hidden");
        var page = await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "hidden" });

        Assert.Multiple(() =>
        {
            Assert.That(history.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(count, Is.Zero);
            Assert.That(page.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task Entry_history_for_a_view_applies_the_source_key_filter()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IViewCatalog>(new FixedViewCatalog(Registration("orders", "orders-source")));
        services.AddSingleton<IReadOnlyList<StartupViewRegistration>>(Array.Empty<StartupViewRegistration>());
        services.AddSingleton<ILatticeAccessGate>(new PrefixGate("allowed/"));
        services.AddSingleton<ILatticeMembershipContext>(new FixedMembership(NamedSubject));
        var query = CreateQuery(services: services.BuildServiceProvider());

        var history = await query.GetEntryHistoryAsync(new EntryHistoryRequest
        {
            TreeId = "view-orders",
            Key = "denied/key",
        });

        Assert.That(history.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
    }

    [Test]
    public async Task CancelScanAsync_treats_unknown_cursors_as_noops()
    {
        var tree = Substitute.For<ILattice>();
        tree.CloseCursorAsync("cursor", Arg.Any<CancellationToken>())
            .Returns<Task>(_ => throw new InvalidOperationException("unknown cursor"));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("tree").Returns(tree);
        var query = CreateQuery(grainFactory: grainFactory);

        await query.CancelScanAsync("tree", "cursor");

        await tree.Received(1).CloseCursorAsync("cursor", Arg.Any<CancellationToken>());
    }


    [Test]
    public async Task Catalog_entry_visibility_gates_view_trees_by_their_source_tree()
    {
        var query = CreateQuery(
            services: ServicesWithCatalog(
                new FixedViewCatalog(Registration("orders-view", "orders")),
                accessGate: new AllowMatchingGate(treeId => treeId == "orders")));

        var visible = await InvokeInstanceAsync<bool>(
            query,
            "IsCatalogEntryVisibleAsync",
            "view-orders-view",
            NamedSubject,
            CancellationToken.None);

        var hidden = await InvokeInstanceAsync<bool>(
            query,
            "IsCatalogEntryVisibleAsync",
            "view-missing-view",
            NamedSubject,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(visible, Is.True);
            Assert.That(hidden, Is.False);
        });
    }
}
