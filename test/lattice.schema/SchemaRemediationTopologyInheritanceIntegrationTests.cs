using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A schema remediation only rewrites values, so it must hand the tree back with the
/// topology and structural pins it had. The destination copy used to be registered
/// with library defaults, so the cutover, which carries the destination's map onto
/// the logical tree (#4250), silently reset a resharded tree to the default shard
/// count and discarded its leaf sizing, WAL partition count, virtual slot count and
/// runtime overrides. The destination now inherits all of them from the source, as a
/// resize's and a snapshot's copy do (#3880, #4379).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationTopologyInheritanceIntegrationTests
{
    private const int PinnedShards = 4;
    private const int ReshardedShards = 7;
    private const int PinnedMaxLeafKeys = 4;
    private const int PinnedMaxInternalChildren = 5;
    private const int PinnedWalPartitions = 3;
    private const int AppVirtualShardCount = 256;
    private const long PinnedMaxCacheValueBytes = 8192;
    private const long PinnedWalMaxRetainedBytes = 1_048_576;
    private const int KeyCount = 64;

    private SchemaRemediationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeRegistry Registry => Grains.GetLatticeRegistry();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaRemediationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string Key(int i) => $"k-{i:D4}";

    [Test]
    public async Task Remediation_of_a_resharded_pinned_tree_keeps_its_map_shard_count_and_pins()
    {
        var treeId = $"remediate-topology-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = PinnedShards,
            MaxLeafKeys = PinnedMaxLeafKeys,
            MaxInternalChildren = PinnedMaxInternalChildren,
            WalPartitions = PinnedWalPartitions,
            MaxCacheValueBytes = PinnedMaxCacheValueBytes,
            WalMaxRetainedBytes = PinnedWalMaxRetainedBytes,
            PublishEvents = true,
        });
        var tree = Grains.GetGrain<ILattice>(treeId);
        await tree.ReshardAsync(ReshardedShards);
        await WriteAsync(treeId);
        var before = await tree.GetRoutingAsync(forceRefresh: true);
        var logicalBefore = await Registry.GetEntryAsync(treeId);
        Assert.That(before.Map.GetPhysicalShardIndices(), Has.Count.EqualTo(ReshardedShards),
            "precondition: the tree was resharded");

        await RemediateAsync(treeId);

        var after = await tree.GetRoutingAsync(forceRefresh: true);
        var destination = await Registry.GetEntryAsync(after.PhysicalTreeId);
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.Not.EqualTo(treeId), "precondition: the tree was cut over");
            Assert.That(after.Map.GetPhysicalShardIndices(), Has.Count.EqualTo(ReshardedShards),
                "the remediation must not reset the tree's shard topology");
            Assert.That(after.Map.Slots.SequenceEqual(before.Map.Slots), Is.True,
                "every slot must route to the shard it did before the remediation");
            Assert.That(destination, Is.Not.Null);
            Assert.That(destination!.ShardMap?.Slots.SequenceEqual(before.Map.Slots), Is.True,
                "the destination must be built under the source's map");
            Assert.That(destination.NextShardIndex, Is.EqualTo(logicalBefore!.NextShardIndex),
                "the destination must inherit the split allocation mark");
            Assert.That(destination.ShardCount, Is.EqualTo(logicalBefore.ShardCount));
            Assert.That(destination.MaxLeafKeys, Is.EqualTo(PinnedMaxLeafKeys));
            Assert.That(destination.MaxInternalChildren, Is.EqualTo(PinnedMaxInternalChildren));
            Assert.That(destination.WalPartitions, Is.EqualTo(PinnedWalPartitions));
            Assert.That(destination.MaxCacheValueBytes, Is.EqualTo(PinnedMaxCacheValueBytes));
            Assert.That(destination.WalMaxRetainedBytes, Is.EqualTo(PinnedWalMaxRetainedBytes));
            Assert.That(destination.PublishEvents, Is.True);
            Assert.That(destination.DerivedFrom, Is.EqualTo(treeId));
        });
        Assert.That(await MissingKeysAsync(treeId), Is.Empty);
        Assert.That(await MisplacedKeysAsync(after), Is.Empty,
            "the build must lay every key out on the shard the inherited map routes it to");
    }

    [Test]
    public async Task Remediation_keeps_a_declared_virtual_slot_count()
    {
        var treeId = $"remediate-slots-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = PinnedShards,
            ShardMap = ShardMap.CreateDefault(AppVirtualShardCount, PinnedShards),
        });
        await WriteAsync(treeId);
        var tree = Grains.GetGrain<ILattice>(treeId);
        var before = await tree.GetRoutingAsync(forceRefresh: true);

        await RemediateAsync(treeId);

        var after = await tree.GetRoutingAsync(forceRefresh: true);
        Assert.Multiple(() =>
        {
            Assert.That(after.Map.Slots, Has.Length.EqualTo(AppVirtualShardCount),
                "the remediation must keep the tree's declared virtual slot count");
            Assert.That(after.Map.Slots.SequenceEqual(before.Map.Slots), Is.True,
                "every slot must route to the shard it did before the remediation");
        });
        Assert.That(await MissingKeysAsync(treeId), Is.Empty);
    }

    [Test]
    public async Task Repeated_remediation_follows_a_reshard_through_the_alias()
    {
        var treeId = $"remediate-twice-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = PinnedShards });
        var tree = Grains.GetGrain<ILattice>(treeId);
        await tree.ReshardAsync(ReshardedShards);
        await WriteAsync(treeId);
        await RemediateAsync(treeId);
        Assert.That((await tree.GetRoutingAsync(forceRefresh: true)).Map.GetPhysicalShardIndices(),
            Has.Count.EqualTo(ReshardedShards), "precondition: the first cutover kept the topology");

        await RemediateAsync(treeId);

        Assert.That((await tree.GetRoutingAsync(forceRefresh: true)).Map.GetPhysicalShardIndices(),
            Has.Count.EqualTo(ReshardedShards),
            "a remediation of a remediated tree must inherit the map the logical tree routes by");
        Assert.That(await MissingKeysAsync(treeId), Is.Empty);
    }

    private async Task WriteAsync(string treeId)
    {
        var tree = Grains.GetGrain<ILattice>(treeId);
        for (var i = 0; i < KeyCount; i++)
        {
            await tree.SetAsync(Key(i), Encoding.UTF8.GetBytes($"{{\"i\":{i}}}"));
        }
    }

    private async Task RemediateAsync(string treeId)
    {
        var report = await Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId).StartAsync(
            LatticeValueTransform.Passthrough(),
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));
        Assert.That(report.Succeeded, Is.True, "precondition: the remediation cut over");
    }

    private async Task<List<string>> MissingKeysAsync(string treeId)
    {
        var tree = Grains.GetGrain<ILattice>(treeId);
        var missing = new List<string>();
        for (var i = 0; i < KeyCount; i++)
        {
            if (await tree.GetAsync(Key(i)) is null)
            {
                missing.Add(Key(i));
            }
        }

        return missing;
    }

    /// <summary>
    /// The keys the physical copy does not hold on the shard its map routes them to,
    /// read directly from that shard rather than through routing.
    /// </summary>
    private async Task<List<string>> MisplacedKeysAsync(RoutingInfo routing)
    {
        var misplaced = new List<string>();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            for (var i = 0; i < KeyCount; i++)
            {
                var shard = Grains.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{routing.Map.Resolve(Key(i))}");
                if (await shard.GetAsync(Key(i)) is null)
                {
                    misplaced.Add(Key(i));
                }
            }
        }

        return misplaced;
    }
}
