using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the tag-index administration operations on
/// <see cref="LatticeTreeAdmin"/>. The listing verb authorizes the cluster-wide
/// <c>Telemetry</c> capability fail-closed; the per-index verbs derive each
/// index's backing membership tree id authoritatively by prefixing the reserved
/// <c>tag-</c> namespace onto the caller-supplied name (never trusting a caller
/// tree id) and authorize whole-tree <c>Read</c> (status) or <c>Admin</c>
/// (reconcile) over that backing tree, then probe the registry fail-closed to a
/// <see cref="KeyNotFoundException"/> for an unknown index. When the tag-index
/// subsystem is not available every verb throws
/// <see cref="InvalidOperationException"/>. Driven purely with substitutes and a
/// hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminTagIndexTests
{
    private const string IndexName = "by-tag";
    private static readonly string IndexTree = LatticeConstants.TagIndexTreePrefix + IndexName;

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(
        IGrainFactory factory,
        bool allow = true,
        ILatticeTagIndexFactory? tagIndexFactory = null,
        bool tagIndexEnabled = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver(),
            restoreService: null,
            viewCatalog: null,
            viewFactory: null,
            tagIndexFactory: tagIndexEnabled ? (tagIndexFactory ?? Substitute.For<ILatticeTagIndexFactory>()) : null);

    private static ILatticeRegistry WireRegistry(
        IGrainFactory factory,
        IReadOnlyList<string>? allTreeIds = null,
        (string TreeId, TreeRegistryEntry? Entry)[]? entries = null)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        var ids = allTreeIds ?? [];
        registry.GetAllTreeIdsAsync().Returns(ids);

        // Model the prefix pushdown rather than ignoring the argument, so a wrong
        // prefix shows up as a wrong result here instead of being masked.
        registry.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(call =>
        {
            var prefix = call.Arg<string?>();
            return Task.FromResult<IReadOnlyList<string>>(
                string.IsNullOrEmpty(prefix)
                    ? ids
                    : ids.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList());
        });

        if (entries is not null)
        {
            foreach (var (treeId, entry) in entries)
            {
                registry.GetEntryAsync(treeId).Returns(entry);
            }
        }

        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return registry;
    }

    private static ILatticeTagIndexFactory WireFactory(params string[] coveredTrees)
    {
        var multiTree = Substitute.For<ILatticeMultiTreeTagIndex>();
        multiTree.CoveredTreesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(coveredTrees));
        var tagFactory = Substitute.For<ILatticeTagIndexFactory>();
        tagFactory.CreateMultiTree(Arg.Any<string>(), Arg.Any<IReadOnlyCollection<string>?>())
            .Returns(multiTree);
        return tagFactory;
    }

    private static ITagIndexReconcileGrain WireReconcile(
        IGrainFactory factory, string indexName, bool idle = true, TagReconcileReport report = default)
    {
        var grain = Substitute.For<ITagIndexReconcileGrain>();
        grain.IsIdleAsync().Returns(idle);
        grain.RunSweepAsync().Returns(report);
        factory.GetGrain<ITagIndexReconcileGrain>(indexName).Returns(grain);
        return grain;
    }

    // ----- ListTagIndexes -----

    [Test]
    public async Task ListTagIndexesAsync_projects_the_tag_prefixed_registry_trees()
    {
        var factory = Substitute.For<IGrainFactory>();
        var indexTree = LatticeConstants.TagIndexTreePrefix + "colour";
        WireRegistry(factory,
            allTreeIds: ["orders", indexTree, "widgets"],
            entries: [(indexTree, new TreeRegistryEntry { ShardCount = 8 })]);
        var facade = Create(factory, tagIndexFactory: WireFactory("orders", "widgets"));

        var catalog = await facade.ListTagIndexesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Indexes, Has.Length.EqualTo(1));
            Assert.That(catalog.Indexes[0].IndexName, Is.EqualTo("colour"));
            Assert.That(catalog.Indexes[0].TreeId, Is.EqualTo(indexTree));
            Assert.That(catalog.Indexes[0].ShardCount, Is.EqualTo(8));
            Assert.That(catalog.Indexes[0].CoveredTrees, Is.EquivalentTo(new[] { "orders", "widgets" }));
        });
    }

    [Test]
    public async Task ListTagIndexesAsync_defaults_shard_count_when_registry_entry_absent()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, allTreeIds: [IndexTree], entries: [(IndexTree, null)]);
        var facade = Create(factory, tagIndexFactory: WireFactory());

        var catalog = await facade.ListTagIndexesAsync();

        Assert.That(catalog.Indexes, Has.Length.EqualTo(1));
        Assert.That(catalog.Indexes[0].ShardCount, Is.EqualTo(LatticeConstants.DefaultShardCount));
    }

    [Test]
    public void ListTagIndexesAsync_denied_by_telemetry_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.ListTagIndexesAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void ListTagIndexesAsync_without_subsystem_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, tagIndexEnabled: false);

        Assert.That(async () => await facade.ListTagIndexesAsync(),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- GetTagIndexStatus -----

    [Test]
    public async Task GetTagIndexStatusAsync_resolves_backing_tree_and_projects_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, entries: [(IndexTree, new TreeRegistryEntry { ShardCount = 4 })]);
        WireReconcile(factory, IndexName, idle: false);
        var facade = Create(factory, tagIndexFactory: WireFactory("orders"));

        var status = await facade.GetTagIndexStatusAsync(IndexName);

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo(IndexName));
            Assert.That(status.TreeId, Is.EqualTo(IndexTree));
            Assert.That(status.ShardCount, Is.EqualTo(4));
            Assert.That(status.CoveredTrees, Is.EquivalentTo(new[] { "orders" }));
            Assert.That(status.ReconcileIdle, Is.False);
        });
    }

    [Test]
    public void GetTagIndexStatusAsync_unknown_index_throws_key_not_found()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, entries: [(IndexTree, null)]);
        WireReconcile(factory, IndexName);
        var facade = Create(factory, tagIndexFactory: WireFactory());

        Assert.That(async () => await facade.GetTagIndexStatusAsync(IndexName),
            Throws.TypeOf<KeyNotFoundException>());
    }

    [Test]
    public void GetTagIndexStatusAsync_denied_by_read_gate_throws_and_does_not_probe_registry()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory, entries: [(IndexTree, new TreeRegistryEntry())]);
        var facade = Create(factory, allow: false, tagIndexFactory: WireFactory());

        Assert.That(async () => await facade.GetTagIndexStatusAsync(IndexName),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public void GetTagIndexStatusAsync_empty_index_name_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.GetTagIndexStatusAsync(""),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void GetTagIndexStatusAsync_without_subsystem_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, tagIndexEnabled: false);

        Assert.That(async () => await facade.GetTagIndexStatusAsync(IndexName),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- ReconcileTagIndex -----

    [Test]
    public async Task ReconcileTagIndexAsync_admin_gated_runs_sweep_and_projects_counts()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, entries: [(IndexTree, new TreeRegistryEntry())]);
        var grain = WireReconcile(factory, IndexName, report: new TagReconcileReport(2, 100, 40, 3));
        var facade = Create(factory, tagIndexFactory: WireFactory());

        var report = await facade.ReconcileTagIndexAsync(IndexName);

        await grain.Received(1).RunSweepAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.IndexName, Is.EqualTo(IndexName));
            Assert.That(report.TreeId, Is.EqualTo(IndexTree));
            Assert.That(report.TreesCovered, Is.EqualTo(2));
            Assert.That(report.KeysScanned, Is.EqualTo(100));
            Assert.That(report.MembershipRowsScanned, Is.EqualTo(40));
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(3));
        });
    }

    [Test]
    public void ReconcileTagIndexAsync_unknown_index_throws_key_not_found_and_does_not_sweep()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, entries: [(IndexTree, null)]);
        var grain = WireReconcile(factory, IndexName);
        var facade = Create(factory, tagIndexFactory: WireFactory());

        Assert.That(async () => await facade.ReconcileTagIndexAsync(IndexName),
            Throws.TypeOf<KeyNotFoundException>());
        grain.DidNotReceive().RunSweepAsync();
    }

    [Test]
    public void ReconcileTagIndexAsync_denied_by_admin_gate_throws_and_does_not_sweep()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, entries: [(IndexTree, new TreeRegistryEntry())]);
        var grain = WireReconcile(factory, IndexName);
        var facade = Create(factory, allow: false, tagIndexFactory: WireFactory());

        Assert.That(async () => await facade.ReconcileTagIndexAsync(IndexName),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        grain.DidNotReceive().RunSweepAsync();
    }

    [Test]
    public void ReconcileTagIndexAsync_empty_index_name_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.ReconcileTagIndexAsync(""),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void ReconcileTagIndexAsync_without_subsystem_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, tagIndexEnabled: false);

        Assert.That(async () => await facade.ReconcileTagIndexAsync(IndexName),
            Throws.TypeOf<InvalidOperationException>());
    }
}
