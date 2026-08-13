using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the compaction and durable-history retention administration
/// operations on <see cref="LatticeTreeAdmin"/>. The compaction and retention-set
/// verbs authorize whole-tree <c>Admin</c> fail-closed and reject a reserved system
/// tree id; the retention read authorizes whole-tree <c>Read</c>. Each wraps the
/// public <see cref="ILattice"/> surface: compaction bypasses the policy cooldown via
/// the operator trigger, and retention-set reads the effective policy back after
/// applying the change. Driven purely with substitutes and a hand-written access gate
/// - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminCompactionTests
{
    private const string TreeId = "orders";

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory, bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            restoreService: null,
            viewCatalog: null,
            viewFactory: null,
            tagIndexFactory: Substitute.For<ILatticeTagIndexFactory>());

    private static ILattice WireTree(IGrainFactory factory, string treeId = TreeId)
    {
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(treeId).Returns(tree);
        return tree;
    }

    // ----- TriggerShardCompaction -----

    [Test]
    public async Task TriggerShardCompactionAsync_admin_gated_wraps_compact_and_projects_result()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.CompactShardAsync(3, Arg.Any<CancellationToken>()).Returns(true);
        var facade = Create(factory);

        var result = await facade.TriggerShardCompactionAsync(TreeId, 3);

        await tree.Received(1).CompactShardAsync(3, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(TreeId));
            Assert.That(result.ShardIndex, Is.EqualTo(3));
            Assert.That(result.Accepted, Is.True);
        });
    }

    [Test]
    public async Task TriggerShardCompactionAsync_reports_not_accepted_when_core_declines()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.CompactShardAsync(0, Arg.Any<CancellationToken>()).Returns(false);
        var facade = Create(factory);

        var result = await facade.TriggerShardCompactionAsync(TreeId, 0);

        Assert.That(result.Accepted, Is.False);
    }

    [Test]
    public void TriggerShardCompactionAsync_denied_by_admin_gate_throws_and_does_not_compact()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.TriggerShardCompactionAsync(TreeId, 1),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        tree.DidNotReceive().CompactShardAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void TriggerShardCompactionAsync_empty_tree_id_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.TriggerShardCompactionAsync("", 0),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void TriggerShardCompactionAsync_reserved_tree_id_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.TriggerShardCompactionAsync(LatticeConstants.RegistryTreeId, 0),
            Throws.TypeOf<ArgumentException>());
    }

    // ----- GetHistoryRetention -----

    [Test]
    public async Task GetHistoryRetentionAsync_read_gated_projects_effective_policy()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.GetHistoryRetentionAsync(Arg.Any<CancellationToken>())
            .Returns(new HistoryRetentionSettings { Mode = HistoryRetentionMode.Hybrid, Window = TimeSpan.FromHours(6) });
        var facade = Create(factory);

        var retention = await facade.GetHistoryRetentionAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(retention.TreeId, Is.EqualTo(TreeId));
            Assert.That(retention.Mode, Is.EqualTo(TreeHistoryRetentionMode.Hybrid));
            Assert.That(retention.Window, Is.EqualTo(TimeSpan.FromHours(6)));
        });
    }

    [Test]
    public void GetHistoryRetentionAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireTree(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetHistoryRetentionAsync(TreeId),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void GetHistoryRetentionAsync_empty_tree_id_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.GetHistoryRetentionAsync(""),
            Throws.TypeOf<ArgumentException>());
    }

    // ----- SetHistoryRetention -----

    [Test]
    public async Task SetHistoryRetentionAsync_admin_gated_applies_mapped_override_and_reads_back()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.GetHistoryRetentionAsync(Arg.Any<CancellationToken>())
            .Returns(new HistoryRetentionSettings { Mode = HistoryRetentionMode.FullValue, Window = TimeSpan.FromDays(1) });
        var facade = Create(factory);

        var retention = await facade.SetHistoryRetentionAsync(TreeId, TreeHistoryRetentionMode.FullValue, TimeSpan.FromDays(1));

        await tree.Received(1).SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, TimeSpan.FromDays(1), Arg.Any<CancellationToken>());
        await tree.Received(1).GetHistoryRetentionAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(retention.Mode, Is.EqualTo(TreeHistoryRetentionMode.FullValue));
            Assert.That(retention.Window, Is.EqualTo(TimeSpan.FromDays(1)));
        });
    }

    [Test]
    public async Task SetHistoryRetentionAsync_passes_null_overrides_through_to_clear()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        tree.GetHistoryRetentionAsync(Arg.Any<CancellationToken>())
            .Returns(new HistoryRetentionSettings { Mode = HistoryRetentionMode.MetadataOnly, Window = TimeSpan.Zero });
        var facade = Create(factory);

        await facade.SetHistoryRetentionAsync(TreeId, null, null);

        await tree.Received(1).SetHistoryRetentionAsync(null, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void SetHistoryRetentionAsync_denied_by_admin_gate_throws_and_does_not_set()
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = WireTree(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.SetHistoryRetentionAsync(TreeId, TreeHistoryRetentionMode.FullValue, null),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        tree.DidNotReceive().SetHistoryRetentionAsync(Arg.Any<HistoryRetentionMode?>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void SetHistoryRetentionAsync_non_positive_window_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.SetHistoryRetentionAsync(TreeId, null, TimeSpan.Zero),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void SetHistoryRetentionAsync_reserved_tree_id_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.SetHistoryRetentionAsync(LatticeConstants.RegistryTreeId, null, null),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void SetHistoryRetentionAsync_empty_tree_id_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.SetHistoryRetentionAsync("", null, null),
            Throws.TypeOf<ArgumentException>());
    }
}
