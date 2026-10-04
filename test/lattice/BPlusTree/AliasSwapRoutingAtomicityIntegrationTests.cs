using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4336: an alias swap must never leave routing pairing one physical copy
/// with another copy's shard map. Pins the single-write registry verb
/// (<see cref="ILatticeRegistry.SwapAliasAsync"/>), that a warmed routing
/// activation re-resolves the alias together with the map when a map-version
/// guard discards its cached map, and that an explicit alias swap moves warmed
/// activations off the copy it replaced and back again.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AliasSwapRoutingAtomicityIntegrationTests
{
    private const int KeyCount = 16;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private IGrainFactory Grains => _cluster.Client;

    private ILatticeRegistry Registry => Grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private static List<string> Keys { get; } = Enumerable.Range(0, KeyCount).Select(i => $"k{i:D2}").ToList();

    [Test]
    public async Task SwapAliasAsync_writes_the_alias_and_the_map_in_one_row()
    {
        var (logical, target) = await TwoTreesAsync(logicalShards: 4, targetShards: 3);
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 3);
        map.Version = 7;

        TreeRegistryEntry? before;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            before = await Registry.SwapAliasAsync(logical, target, map, nextShardIndex: 5, expectedPhysicalTreeId: logical);
        }

        var after = (await Registry.GetEntryAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(before, Is.Not.Null);
            Assert.That(before!.PhysicalTreeId, Is.Null, "the row as it stood before the swap");
            Assert.That(after.PhysicalTreeId, Is.EqualTo(target));
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(map.Slots));
            Assert.That(after.ShardMap.Version, Is.EqualTo(8), "re-versioned above the supplied map");
            Assert.That(after.NextShardIndex, Is.EqualTo(5));
            Assert.That(after.AliasCutoverTarget, Is.Null);
        });
    }

    [Test]
    public async Task SwapAliasAsync_back_onto_the_logical_id_removes_the_alias_with_its_map()
    {
        var (logical, target) = await TwoTreesAsync(logicalShards: 4, targetShards: 3);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SwapAliasAsync(logical, target, Map(3), null, expectedPhysicalTreeId: null);
            await Registry.SwapAliasAsync(logical, logical, Map(4), 4, expectedPhysicalTreeId: target);
        }

        var after = (await Registry.GetEntryAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.Null);
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(Map(4).Slots));
            Assert.That(after.ShardMap.Version, Is.EqualTo(2), "never runs backwards across the two swaps");
            Assert.That(after.NextShardIndex, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task SwapAliasAsync_refuses_a_stale_expected_physical_and_writes_nothing()
    {
        var (logical, target) = await TwoTreesAsync(logicalShards: 4, targetShards: 3);
        var before = await Registry.GetEntryAsync(logical);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.ThrowsAsync<InvalidOperationException>(() =>
                Registry.SwapAliasAsync(logical, target, Map(3), null, expectedPhysicalTreeId: "somewhere-else"));
        }

        AssertUnchanged(before, await Registry.GetEntryAsync(logical));
    }

    [Test]
    public async Task SwapAliasAsync_refuses_an_aliased_target_and_writes_nothing()
    {
        var (logical, target) = await TwoTreesAsync(logicalShards: 4, targetShards: 3);
        var elsewhere = $"{target}-elsewhere";
        await Registry.RegisterAsync(elsewhere, new TreeRegistryEntry { ShardCount = 2, DerivedFrom = target });
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SetAliasAsync(target, elsewhere);
        }

        var before = await Registry.GetEntryAsync(logical);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.ThrowsAsync<InvalidOperationException>(() =>
                Registry.SwapAliasAsync(logical, target, Map(3), null, expectedPhysicalTreeId: null));
        }

        AssertUnchanged(before, await Registry.GetEntryAsync(logical));
    }

    [Test]
    public void SwapAliasAsync_back_onto_an_unregistered_tree_refuses()
    {
        var logical = $"swap-unregistered-{Guid.NewGuid():N}";
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(() =>
                Registry.SwapAliasAsync(logical, logical, Map(4), null, expectedPhysicalTreeId: null));
        }
    }

    /// <summary>
    /// The permanent torn view: an activation warmed on the logical tree caches its
    /// physical copy and map; the swap re-versions the map, the multi-get's
    /// map-version guard discards the cached map and retries. Re-reading only the
    /// map paired the new copy's map with the cached old copy and read every key the
    /// two layouts place differently as absent, with nothing left to signal it.
    /// </summary>
    [Test]
    public async Task A_warm_multi_get_after_a_swap_reads_the_new_copy_whole()
    {
        var (logical, target) = await TwoTreesAsync(logicalShards: 4, targetShards: 3);
        var lattice = Grains.GetGrain<ILattice>(logical);
        Assert.That((await lattice.GetManyAsync(Keys)).Values.Select(Text), Is.All.EqualTo(logical),
            "precondition: the logical tree serves its own copy");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SwapAliasAsync(logical, target, Map(3), null, expectedPhysicalTreeId: logical);
        }

        var read = await lattice.GetManyAsync(Keys);
        var routing = await lattice.GetRoutingAsync();
        Assert.Multiple(() =>
        {
            Assert.That(read.Count, Is.EqualTo(KeyCount), "no key reads as absent");
            Assert.That(read.Values.Select(Text), Is.All.EqualTo(target), "every key comes from the new copy");
            Assert.That(routing.PhysicalTreeId, Is.EqualTo(target));
            Assert.That(routing.Map.Slots, Is.EqualTo(Map(3).Slots));
        });
    }

    /// <summary>
    /// The swap must take the supplied map even when the logical row already
    /// persists one of its own - the row of any tree that has been split,
    /// resharded or resized. A swap that kept the row's persisted slots under the
    /// new version would pair the new copy with the old copy's layout (#4336),
    /// and a row with no persisted map cannot show it (shard-ownership review
    /// #4435, finding F8).
    /// </summary>
    [Test]
    public async Task SwapAliasAsync_replaces_a_persisted_map_on_the_logical_row()
    {
        var (logical, target) = await TwoTreesAsync(logicalShards: 4, targetShards: 3, persistLogicalMap: true);
        Assert.That((await Registry.GetEntryAsync(logical))!.ShardMap!.Slots, Is.EqualTo(Map(4).Slots), "precondition: the logical row persists its own map");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SwapAliasAsync(logical, target, Map(3), nextShardIndex: null, expectedPhysicalTreeId: logical);
        }

        var after = (await Registry.GetEntryAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.EqualTo(target));
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(Map(3).Slots), "the swap carries the new copy's map, not the row's old one");
        });
    }

    /// <summary>
    /// <see cref="A_warm_multi_get_after_a_swap_reads_the_new_copy_whole"/> for a
    /// logical row that persists its own map before the swap (finding F8).
    /// </summary>
    [Test]
    public async Task A_warm_multi_get_after_a_swap_reads_the_new_copy_whole_when_the_logical_row_persists_a_map()
    {
        var (logical, target) = await TwoTreesAsync(logicalShards: 4, targetShards: 3, persistLogicalMap: true);
        var lattice = Grains.GetGrain<ILattice>(logical);
        Assert.That((await lattice.GetManyAsync(Keys)).Values.Select(Text), Is.All.EqualTo(logical),
            "precondition: the logical tree serves its own copy");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SwapAliasAsync(logical, target, Map(3), null, expectedPhysicalTreeId: logical);
        }

        var read = await lattice.GetManyAsync(Keys);
        Assert.Multiple(() =>
        {
            Assert.That(read.Count, Is.EqualTo(KeyCount), "no key reads as absent");
            Assert.That(read.Values.Select(Text), Is.All.EqualTo(target), "every key comes from the new copy");
        });
    }
    /// <summary>
    /// A point read carries no map-version guard, so only a staleness signal on the
    /// replaced copy moves a warmed activation off it after an explicit alias.
    /// </summary>
    [Test]
    public async Task An_explicit_alias_moves_warm_point_reads_onto_the_target_and_back()
    {
        var (logical, first) = await TwoTreesAsync(logicalShards: 4, targetShards: 3);
        var second = await SeededTreeAsync($"{logical}-second", shards: 5);
        var lattice = Grains.GetGrain<ILattice>(logical);
        Assert.That(Text(await lattice.GetAsync(Keys[0])), Is.EqualTo(logical), "precondition: warmed on its own copy");

        await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, first);
        var onFirst = await ReadAllPointsAsync(lattice);

        await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, second);
        var onSecond = await ReadAllPointsAsync(lattice);

        // Back onto a copy the alias left earlier: its redirect for this logical
        // tree must be released, or every read would bounce until it timed out.
        await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, first);
        var backOnFirst = await ReadAllPointsAsync(lattice);

        Assert.Multiple(() =>
        {
            Assert.That(onFirst, Is.All.EqualTo(first));
            Assert.That(onSecond, Is.All.EqualTo(second));
            Assert.That(backOnFirst, Is.All.EqualTo(first));
        });
    }

    private static void AssertUnchanged(TreeRegistryEntry? before, TreeRegistryEntry? after) =>
        Assert.Multiple(() =>
        {
            Assert.That(after?.PhysicalTreeId, Is.EqualTo(before?.PhysicalTreeId));
            Assert.That(after?.ShardMap?.Version, Is.EqualTo(before?.ShardMap?.Version));
            Assert.That(after?.NextShardIndex, Is.EqualTo(before?.NextShardIndex));
        });

    private async Task<List<string?>> ReadAllPointsAsync(ILattice lattice)
    {
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var values = new List<string?>(KeyCount);
        foreach (var key in Keys)
            values.Add(Text(await lattice.GetAsync(key, budget.Token)));
        return values;
    }

    private async Task<(string Logical, string Target)> TwoTreesAsync(int logicalShards, int targetShards, bool persistLogicalMap = false)
    {
        var logical = await SeededTreeAsync($"swap-logical-{Guid.NewGuid():N}", logicalShards, persistLogicalMap);
        var target = await SeededTreeAsync($"{logical}-target", targetShards);
        return (logical, target);
    }

    /// <summary>Registers a tree on the default map for <paramref name="shards"/> (persisted on its row when <paramref name="persistMap"/> is set) and writes every key with the tree's own id as its value.</summary>
    private async Task<string> SeededTreeAsync(string treeId, int shards, bool persistMap = false)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = shards, ShardMap = persistMap ? Map(shards) : null });
        var lattice = Grains.GetGrain<ILattice>(treeId);
        foreach (var key in Keys)
            await lattice.SetAsync(key, System.Text.Encoding.UTF8.GetBytes(treeId));
        return treeId;
    }

    private static string? Text(byte[]? value) => value is null ? null : System.Text.Encoding.UTF8.GetString(value);

    private static ShardMap Map(int shards) =>
        ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, shards);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
