using System.Reflection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The registry entry a derived physical copy is registered with: a snapshot's and a
/// resize's destination (#3880) and a schema remediation's (#4379).
/// </summary>
[TestFixture]
public sealed class DerivedTreeEntriesTests
{
    private const string Logical = "tree";
    private const string Physical = "tree/resized/op";

    /// <summary>
    /// The registry fields that are runtime configuration overrides. Every other field
    /// is a structural pin, routing state, or alias / lifecycle bookkeeping that the
    /// copy must not take over from its source.
    /// </summary>
    private static readonly string[] Overrides =
    [
        nameof(TreeRegistryEntry.PublishEvents),
        nameof(TreeRegistryEntry.MaintainProjectionDigest),
        nameof(TreeRegistryEntry.ProjectionDigestPermanentlyDisabled),
        nameof(TreeRegistryEntry.HistoryRetentionMode),
        nameof(TreeRegistryEntry.HistoryRetentionWindowTicks),
        nameof(TreeRegistryEntry.MaxCacheValueBytes),
        nameof(TreeRegistryEntry.WalMaxRetainedBytes),
    ];

    private static readonly string[] NotOverrides =
    [
        nameof(TreeRegistryEntry.MaxLeafKeys),
        nameof(TreeRegistryEntry.MaxInternalChildren),
        nameof(TreeRegistryEntry.ShardCount),
        nameof(TreeRegistryEntry.PhysicalTreeId),
        nameof(TreeRegistryEntry.ShardMap),
        nameof(TreeRegistryEntry.NextShardIndex),
        nameof(TreeRegistryEntry.WalPartitions),
        nameof(TreeRegistryEntry.WalPlacement),
        nameof(TreeRegistryEntry.RestoreShadowOfTreeId),
        nameof(TreeRegistryEntry.DerivedFrom),
        nameof(TreeRegistryEntry.ReplacedShardMap),
        nameof(TreeRegistryEntry.ReplacedNextShardIndex),
        nameof(TreeRegistryEntry.AliasCutoverTarget),
        // Content identity, not configuration (#4537): a derived copy gets its own
        // lineage, and carrying one tree's onto another would claim the copy holds
        // that tree's contents.
        nameof(TreeRegistryEntry.Lineage),
        // Replication admission state of one physical tree (#4549), not
        // configuration: a copy's shards are armed by its own bootstraps, and a
        // re-stamp that adopts a copy re-seeds it, which raises it afresh.
        nameof(TreeRegistryEntry.ReplicationFloorEpoch),
    ];

    private static TreeRegistryEntry WithEveryOverride() => new()
    {
        PublishEvents = true,
        MaintainProjectionDigest = true,
        ProjectionDigestPermanentlyDisabled = true,
        HistoryRetentionMode = HistoryRetentionMode.FullValue,
        HistoryRetentionWindowTicks = 1234,
        MaxCacheValueBytes = 4096,
        WalMaxRetainedBytes = 65536,
    };

    [Test]
    public void ForCopy_copies_the_map_and_split_mark_and_pins()
    {
        var map = ShardMap.CreateDefault(256, 3);
        map.Version = 9;

        var entry = DerivedTreeEntries.ForCopy(map, 5, 3, 16, 8, derivedFrom: Logical);

        Assert.Multiple(() =>
        {
            Assert.That(entry.ShardMap!.Slots, Is.EqualTo(map.Slots));
            Assert.That(entry.ShardMap.Slots, Is.Not.SameAs(map.Slots), "the copy owns its map");
            Assert.That(entry.ShardMap.Version, Is.EqualTo(9));
            Assert.That(entry.NextShardIndex, Is.EqualTo(5));
            Assert.That(entry.ShardCount, Is.EqualTo(3));
            Assert.That(entry.MaxLeafKeys, Is.EqualTo(16));
            Assert.That(entry.MaxInternalChildren, Is.EqualTo(8));
            Assert.That(entry.DerivedFrom, Is.EqualTo(Logical));
        });
    }

    [Test]
    public void ForCopy_with_no_map_leaves_the_copy_on_the_default_map()
    {
        var entry = DerivedTreeEntries.ForCopy(null, null, 4);

        Assert.Multiple(() =>
        {
            Assert.That(entry.ShardMap, Is.Null);
            Assert.That(entry.ShardCount, Is.EqualTo(4));
            Assert.That(entry.MaxLeafKeys, Is.Null);
            Assert.That(entry.DerivedFrom, Is.Null);
        });
    }

    [Test]
    public void WithConfigurationOverridesOf_copies_every_override_and_nothing_else()
    {
        var target = new TreeRegistryEntry { ShardCount = 2, MaxLeafKeys = 7 };

        var carried = target.WithConfigurationOverridesOf(WithEveryOverride() with { ShardCount = 9, MaxLeafKeys = 99 });

        Assert.Multiple(() =>
        {
            foreach (var name in Overrides)
            {
                var property = typeof(TreeRegistryEntry).GetProperty(name)!;
                Assert.That(property.GetValue(carried), Is.EqualTo(property.GetValue(WithEveryOverride())), name);
            }

            Assert.That(carried.ShardCount, Is.EqualTo(2));
            Assert.That(carried.MaxLeafKeys, Is.EqualTo(7));
        });
    }

    [Test]
    public void WithConfigurationOverridesOf_a_null_source_changes_nothing()
    {
        var target = new TreeRegistryEntry { ShardCount = 2 };

        Assert.That(target.WithConfigurationOverridesOf(null), Is.SameAs(target));
    }

    [Test]
    public void Every_registry_field_is_classified_as_an_override_or_not()
    {
        // A new registry field must be classified here, so a new runtime override is
        // carried onto derived copies rather than silently reset by a remediation.
        var fields = typeof(TreeRegistryEntry)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetCustomAttribute<IdAttribute>() is not null)
            .Select(p => p.Name);

        Assert.That(fields, Is.EquivalentTo(Overrides.Concat(NotOverrides)));
    }

    [Test]
    public async Task InheritingAsync_takes_routing_from_the_logical_tree_and_sizing_from_its_physical_copy()
    {
        var map = ShardMap.CreateDefault(512, 7);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Logical).Returns(WithEveryOverride() with
        {
            ShardCount = 7,
            NextShardIndex = 11,
            MaxLeafKeys = 32,
            MaxInternalChildren = 32,
            WalPartitions = null,
            PhysicalTreeId = Physical,
            ShardMap = map,
        });
        registry.GetEntryAsync(Physical).Returns(new TreeRegistryEntry
        {
            ShardCount = 4,
            MaxLeafKeys = 4,
            MaxInternalChildren = 5,
            WalPartitions = 3,
        });
        var factory = Factory(registry, new RoutingInfo(Physical, map));

        var entry = await DerivedTreeEntries.InheritingAsync(factory, Logical, derivedFrom: Logical);

        Assert.Multiple(() =>
        {
            Assert.That(entry.ShardMap!.Slots, Is.EqualTo(map.Slots));
            Assert.That(entry.NextShardIndex, Is.EqualTo(11));
            Assert.That(entry.ShardCount, Is.EqualTo(7));
            Assert.That(entry.MaxLeafKeys, Is.EqualTo(4));
            Assert.That(entry.MaxInternalChildren, Is.EqualTo(5));
            Assert.That(entry.WalPartitions, Is.EqualTo(3));
            Assert.That(entry.MaxCacheValueBytes, Is.EqualTo(4096));
            Assert.That(entry.PublishEvents, Is.True);
            Assert.That(entry.DerivedFrom, Is.EqualTo(Logical));
            Assert.That(entry.PhysicalTreeId, Is.Null, "the copy is not an alias");
        });
    }

    [Test]
    public async Task InheritingAsync_of_a_never_aliased_tree_reads_one_entry_and_the_effective_map()
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 4);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Logical).Returns(new TreeRegistryEntry
        {
            ShardCount = 4,
            MaxLeafKeys = 4,
            MaxInternalChildren = 5,
            WalPartitions = 2,
        });
        var factory = Factory(registry, new RoutingInfo(Logical, map));

        var entry = await DerivedTreeEntries.InheritingAsync(factory, Logical, derivedFrom: Logical);

        Assert.Multiple(() =>
        {
            Assert.That(entry.ShardMap!.Slots, Is.EqualTo(map.Slots),
                "the effective map is persisted even when the source has none");
            Assert.That(entry.ShardCount, Is.EqualTo(4));
            Assert.That(entry.MaxLeafKeys, Is.EqualTo(4));
            Assert.That(entry.MaxInternalChildren, Is.EqualTo(5));
            Assert.That(entry.WalPartitions, Is.EqualTo(2));
        });
        await registry.Received(1).GetEntryAsync(Arg.Any<string>());
    }

    private static IGrainFactory Factory(ILatticeRegistry registry, RoutingInfo routing)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(true, Arg.Any<CancellationToken>()).Returns(new ValueTask<RoutingInfo>(routing));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        factory.GetGrain<ILattice>(Logical).Returns(lattice);
        return factory;
    }
}
