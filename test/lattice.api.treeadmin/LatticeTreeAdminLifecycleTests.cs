using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the tree-lifecycle and per-tree registry-configuration operations
/// on <see cref="LatticeTreeAdmin"/>: explicit creation, existence, alias
/// assignment / resolution, config read / update, and the registry-persisted
/// shard-map read. Each wraps the internal <see cref="ILatticeRegistry"/> grain,
/// authorizing first (whole-tree <c>Admin</c> for a mutation, <c>Read</c> for a
/// read) through the shared fail-closed access gate. Driven purely with substitutes
/// and a hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminLifecycleTests
{
    private const string Tree = "orders";

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
        bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()));

    private static ILatticeRegistry Registry(IGrainFactory factory)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return registry;
    }

    // ----- CreateTree -----

    [Test]
    public async Task CreateTreeAsync_registers_a_new_tree_and_reports_created_true()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.ExistsAsync(Tree).Returns(false);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry
        {
            ShardCount = 8,
            MaxLeafKeys = 64,
            MaxInternalChildren = 32,
        });
        var facade = Create(factory);

        var result = await facade.CreateTreeAsync(Tree, shardCount: 8, maxLeafKeys: 64, maxInternalChildren: 32);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.Created, Is.True);
            Assert.That(result.ShardCount, Is.EqualTo(8));
            Assert.That(result.MaxLeafKeys, Is.EqualTo(64));
            Assert.That(result.MaxInternalChildren, Is.EqualTo(32));
        });
        await registry.Received(1).RegisterAsync(Tree, Arg.Is<TreeRegistryEntry>(e =>
            e.ShardCount == 8 && e.MaxLeafKeys == 64 && e.MaxInternalChildren == 32));
    }

    [Test]
    public async Task CreateTreeAsync_is_idempotent_and_reports_created_false_for_an_existing_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.ExistsAsync(Tree).Returns(true);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry
        {
            ShardCount = 64,
            MaxLeafKeys = 128,
            MaxInternalChildren = 128,
        });
        var facade = Create(factory);

        var result = await facade.CreateTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(result.Created, Is.False);
            Assert.That(result.ShardCount, Is.EqualTo(64));
        });
        // No sizing supplied -> a null entry is passed to the idempotent register.
        await registry.Received(1).RegisterAsync(Tree, null);
    }

    [Test]
    public async Task CreateTreeAsync_defaults_effective_sizing_when_entry_unset()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.ExistsAsync(Tree).Returns(false);
        registry.GetEntryAsync(Tree).Returns((TreeRegistryEntry?)null);
        var facade = Create(factory);

        var result = await facade.CreateTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(result.ShardCount, Is.EqualTo(LatticeConstants.DefaultShardCount));
            Assert.That(result.MaxLeafKeys, Is.EqualTo(LatticeConstants.DefaultMaxLeafKeys));
            Assert.That(result.MaxInternalChildren, Is.EqualTo(LatticeConstants.DefaultMaxInternalChildren));
        });
    }

    [Test]
    public void CreateTreeAsync_reserved_tree_id_is_rejected_and_does_not_dial_registry()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.CreateTreeAsync(LatticeConstants.SystemTreePrefix + "trees"),
            Throws.ArgumentException);
        factory.DidNotReceive().GetGrain<ILatticeRegistry>(Arg.Any<string>());
    }

    [Test]
    public void CreateTreeAsync_denied_by_admin_gate_throws_and_does_not_dial_registry()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.CreateTreeAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        factory.DidNotReceive().GetGrain<ILatticeRegistry>(Arg.Any<string>());
    }

    [Test]
    public void CreateTreeAsync_non_positive_sizing_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.CreateTreeAsync(Tree, shardCount: 0),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(async () => await facade.CreateTreeAsync(Tree, maxLeafKeys: -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(async () => await facade.CreateTreeAsync(Tree, maxInternalChildren: 0),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void CreateTreeAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.CreateTreeAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.CreateTreeAsync(""), Throws.ArgumentException);
        });
    }

    // ----- CheckTreeExists -----

    [Test]
    public async Task CheckTreeExistsAsync_projects_the_registry_result()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.ExistsAsync(Tree).Returns(true);
        var facade = Create(factory);

        var result = await facade.CheckTreeExistsAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.Exists, Is.True);
        });
    }

    [Test]
    public void CheckTreeExistsAsync_denied_by_read_gate_throws_and_does_not_dial_registry()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.CheckTreeExistsAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        factory.DidNotReceive().GetGrain<ILatticeRegistry>(Arg.Any<string>());
    }

    // ----- SetTreeAlias / ResolveTreeAlias -----

    [Test]
    public async Task SetTreeAliasAsync_sets_and_reports_the_resolved_alias()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.ResolveAsync(Tree).Returns("phys-orders");
        var facade = Create(factory);

        var result = await facade.SetTreeAliasAsync(Tree, "phys-orders");

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.PhysicalTreeId, Is.EqualTo("phys-orders"));
            Assert.That(result.IsAliased, Is.True);
        });
        await registry.Received(1).SetAliasAsync(Tree, "phys-orders");
    }

    [Test]
    public void SetTreeAliasAsync_rejects_a_self_alias()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.SetTreeAliasAsync(Tree, Tree), Throws.ArgumentException);
    }

    [Test]
    public void SetTreeAliasAsync_reserved_tree_id_is_rejected()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.SetTreeAliasAsync(LatticeConstants.SystemTreePrefix + "x", "phys"),
            Throws.ArgumentException);
    }

    [Test]
    public void SetTreeAliasAsync_denied_by_admin_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.SetTreeAliasAsync(Tree, "phys-orders"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task ResolveTreeAliasAsync_reports_not_aliased_when_resolving_to_itself()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.ResolveAsync(Tree).Returns(Tree);
        var facade = Create(factory);

        var result = await facade.ResolveTreeAliasAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(result.PhysicalTreeId, Is.EqualTo(Tree));
            Assert.That(result.IsAliased, Is.False);
        });
    }

    [Test]
    public void ResolveTreeAliasAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.ResolveTreeAliasAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- GetTreeConfig -----

    [Test]
    public async Task GetTreeConfigAsync_projects_every_registry_field()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry
        {
            PhysicalTreeId = "phys-orders",
            ShardCount = 8,
            MaxLeafKeys = 64,
            MaxInternalChildren = 32,
            PublishEvents = true,
            MaintainProjectionDigest = false,
            ProjectionDigestPermanentlyDisabled = true,
            HistoryRetentionMode = HistoryRetentionMode.FullValue,
            HistoryRetentionWindowTicks = 123,
        });
        var facade = Create(factory);

        var report = await facade.GetTreeConfigAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.Exists, Is.True);
            Assert.That(report.PhysicalTreeId, Is.EqualTo("phys-orders"));
            Assert.That(report.ShardCount, Is.EqualTo(8));
            Assert.That(report.MaxLeafKeys, Is.EqualTo(64));
            Assert.That(report.MaxInternalChildren, Is.EqualTo(32));
            Assert.That(report.PublishEvents, Is.True);
            Assert.That(report.MaintainProjectionDigest, Is.False);
            Assert.That(report.ProjectionDigestPermanentlyDisabled, Is.True);
            Assert.That(report.HistoryRetentionMode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(report.HistoryRetentionWindowTicks, Is.EqualTo(123));
        });
    }

    [Test]
    public async Task GetTreeConfigAsync_reports_not_existing_for_a_null_entry()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.GetEntryAsync(Tree).Returns((TreeRegistryEntry?)null);
        var facade = Create(factory);

        var report = await facade.GetTreeConfigAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.Exists, Is.False);
            Assert.That(report.ShardCount, Is.Null);
            Assert.That(report.ProjectionDigestPermanentlyDisabled, Is.False);
            Assert.That(report.HistoryRetentionMode, Is.Null);
        });
    }

    [Test]
    public void GetTreeConfigAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetTreeConfigAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- SetTreeConfig -----

    [Test]
    public async Task SetTreeConfigAsync_applies_only_flagged_dimensions()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry { PublishEvents = true });
        var facade = Create(factory);

        var update = new TreeConfigurationUpdate
        {
            ApplyPublishEvents = true,
            PublishEvents = true,
            // MaintainProjectionDigest not applied.
            ApplyHistoryRetention = true,
            HistoryRetentionMode = HistoryRetentionMode.FullValue,
            HistoryRetentionWindowTicks = 500,
        };

        await facade.SetTreeConfigAsync(Tree, update);

        await registry.Received(1).SetPublishEventsAsync(Tree, true);
        await registry.DidNotReceive().SetMaintainProjectionDigestAsync(Tree, Arg.Any<bool?>());
        await registry.Received(1).SetHistoryRetentionAsync(
            Tree, HistoryRetentionMode.FullValue, TimeSpan.FromTicks(500));
    }

    [Test]
    public async Task SetTreeConfigAsync_clears_an_override_with_a_null_applied_value()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.GetEntryAsync(Tree).Returns((TreeRegistryEntry?)null);
        var facade = Create(factory);

        var update = new TreeConfigurationUpdate
        {
            ApplyMaintainProjectionDigest = true,
            MaintainProjectionDigest = null,
        };

        await facade.SetTreeConfigAsync(Tree, update);

        await registry.Received(1).SetMaintainProjectionDigestAsync(Tree, null);
    }

    [Test]
    public void SetTreeConfigAsync_non_positive_window_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory);

        var update = new TreeConfigurationUpdate
        {
            ApplyHistoryRetention = true,
            HistoryRetentionWindowTicks = 0,
        };

        Assert.That(async () => await facade.SetTreeConfigAsync(Tree, update),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void SetTreeConfigAsync_reserved_tree_id_is_rejected()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.SetTreeConfigAsync(
                LatticeConstants.SystemTreePrefix + "x", new TreeConfigurationUpdate()),
            Throws.ArgumentException);
    }

    [Test]
    public void SetTreeConfigAsync_null_update_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.That(async () => await facade.SetTreeConfigAsync(Tree, null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void SetTreeConfigAsync_denied_by_admin_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.SetTreeConfigAsync(Tree, new TreeConfigurationUpdate()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- GetShardMap -----

    [Test]
    public async Task GetShardMapAsync_reports_default_identity_map_when_registry_returns_null()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.GetShardMapAsync(Tree).Returns((ShardMap?)null);
        var facade = Create(factory);

        var view = await facade.GetShardMapAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(view.TreeId, Is.EqualTo(Tree));
            Assert.That(view.HasCustomMap, Is.False);
            Assert.That(view.MapVersion, Is.EqualTo(0));
            Assert.That(view.PhysicalShardIndices, Is.Empty);
        });
    }

    [Test]
    public async Task GetShardMapAsync_projects_a_persisted_map_with_distinct_sorted_shards()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.GetShardMapAsync(Tree).Returns(new ShardMap { Slots = new[] { 1, 0, 1, 0 }, Version = 5 });
        var facade = Create(factory);

        var view = await facade.GetShardMapAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(view.HasCustomMap, Is.True);
            Assert.That(view.MapVersion, Is.EqualTo(5));
            Assert.That(view.VirtualShardCount, Is.EqualTo(4));
            Assert.That(view.PhysicalShardCount, Is.EqualTo(2));
            Assert.That(view.PhysicalShardIndices, Is.EqualTo(new[] { 0, 1 }));
        });
    }

    [Test]
    public void GetShardMapAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Registry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetShardMapAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }
}
