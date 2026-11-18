using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end regression for issue #6083: automatic shard healing of an aliased
/// tree (one restored with a shadow cutover, resized, or remediated) folded the
/// alias target's shards but committed the new routing onto the copy's own
/// registry row. Routing reads the logical row, so every folded slot kept routing
/// to the retired donor and each read or write into it failed after the
/// stale-routing budget.
/// <para>
/// Each test builds an over-split aliased tree directly in the registry: a
/// physical copy registered with four physical shards against a base of two,
/// and a logical tree aliased onto it. The fixture's silos switch off adaptive
/// splitting, the skew and backpressure clauses and the post-split cooldown, and
/// park the healing timer,
/// so a sweep is admitted only when a test drives one and the outcome depends on
/// nothing but the routing it asserts.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShardHealingAliasedTreeIntegrationTests
{
    private const int BaseShardCount = 2;
    private const int PhysicalShardCount = 4;
    private const int KeyCount = 96;

    private TestCluster _cluster = null!;

    private IGrainFactory Grains => _cluster.Client;

    private ILatticeRegistry Registry => Grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

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

    [Test]
    public async Task Healing_an_aliased_tree_commits_each_fold_onto_the_logical_routing_map()
    {
        var (logical, physical) = await CreateAliasedTreeAsync("heal-alias", derived: true);
        var tree = Grains.GetGrain<ILattice>(logical);
        var expected = await PopulateAsync(tree);

        var healer = Grains.GetGrain<IShardHealingOrchestratorGrain>(logical);
        for (var pass = 0; pass < 200 && await PhysicalShardCountAsync(logical) > BaseShardCount; pass++)
            await healer.RunHealingPassAsync();


        var logicalMap = await Registry.GetShardMapAsync(logical);
        Assert.That(logicalMap!.GetPhysicalShardIndices(), Has.Count.EqualTo(BaseShardCount),
            "Healing must fold the logical tree's own routing map down to its base shard count.");
        foreach (var retired in Enumerable.Range(0, PhysicalShardCount).Except(logicalMap.GetPhysicalShardIndices()))
        {
            Assert.That(await Grains.GetGrain<IShardRootGrain>($"{physical}/{retired}").IsRetiredAsync(), Is.True,
                $"Shard {retired} left the logical map, so its fold must have retired it on the alias target.");
        }

        await AssertReadableAndWritableAsync(tree, expected, "after healing folded the aliased tree");
    }

    [Test]
    public async Task A_fold_keyed_by_a_derived_alias_target_is_refused_before_it_starts()
    {
        var (logical, physical) = await CreateAliasedTreeAsync("heal-alias-fence", derived: true);
        await PopulateAsync(Grains.GetGrain<ILattice>(logical));
        var logicalBefore = (await Registry.GetShardMapAsync(logical))!.Slots;
        var physicalBefore = (await Registry.GetShardMapAsync(physical))!.Slots;

        var copyKeyed = Grains.GetGrain<ITreeShardConsolidationGrain>($"{physical}/3");
        Assert.That(async () => await copyKeyed.StartAsync(2), Throws.InvalidOperationException);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await copyKeyed.IsIdleAsync(), Is.True);
            Assert.That((await Registry.GetShardMapAsync(logical))!.Slots, Is.EqualTo(logicalBefore).AsCollection);
            Assert.That((await Registry.GetShardMapAsync(physical))!.Slots, Is.EqualTo(physicalBefore).AsCollection);
            Assert.That(await Grains.GetGrain<IShardRootGrain>($"{physical}/3").IsRetiredAsync(), Is.False);
        });
    }

    [Test]
    public async Task Routing_a_pre_fix_fold_left_on_the_alias_target_is_repaired_by_the_next_read()
    {
        var (logical, physical, tree, expected) = await CreateDamagedTreeAsync("heal-alias-read-repair");

        await AssertReadableAndWritableAsync(tree, expected, "after the damaged routing was repaired");

        Assert.That((await Registry.GetShardMapAsync(logical))!.GetPhysicalShardIndices(), Does.Not.Contain(3),
            "The repair must re-point the logical map off the retired donor.");
        Assert.That((await Registry.GetShardMapAsync(logical))!.Slots,
            Is.EqualTo((await Registry.GetShardMapAsync(physical))!.Slots).AsCollection);
    }

    [Test]
    public async Task Routing_a_pre_fix_fold_left_on_the_alias_target_is_repaired_by_a_healing_sweep()
    {
        var (logical, physical, tree, expected) = await CreateDamagedTreeAsync("heal-alias-sweep-repair");

        await Grains.GetGrain<IShardHealingOrchestratorGrain>(logical).RunHealingPassAsync();

        Assert.That((await Registry.GetShardMapAsync(logical))!.GetPhysicalShardIndices(), Does.Not.Contain(3),
            "A healing sweep must re-point the logical map off the retired donor without waiting for a read.");
        await AssertReadableAndWritableAsync(tree, expected, "after a healing sweep repaired the routing");
    }

    /// <summary>
    /// Reproduces the state a deployment running the pre-fix healer is left in:
    /// a fold keyed by the alias target's id, driven to completion, retires the
    /// donor and re-points only the target's own row. A bare alias records no
    /// owner on the target, so the commit fence admits that fold exactly as the
    /// pre-fix code did.
    /// </summary>
    private async Task<(string Logical, string Physical, ILattice Tree, Dictionary<string, byte[]> Expected)> CreateDamagedTreeAsync(string prefix)
    {
        var (logical, physical) = await CreateAliasedTreeAsync(prefix, derived: false);
        var tree = Grains.GetGrain<ILattice>(logical);
        var expected = await PopulateAsync(tree);

        var copyKeyed = Grains.GetGrain<ITreeShardConsolidationGrain>($"{physical}/3");
        await copyKeyed.StartAsync(2);
        for (var i = 0; i < 64 && !await copyKeyed.IsIdleAsync(); i++)
            await copyKeyed.RunConsolidationPassAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await copyKeyed.IsIdleAsync(), Is.True, "the copy-keyed fold must reach a terminal state");
            Assert.That(await Grains.GetGrain<IShardRootGrain>($"{physical}/3").IsRetiredAsync(), Is.True,
                "the copy-keyed fold must retire its donor");
            Assert.That((await Registry.GetShardMapAsync(physical))!.GetPhysicalShardIndices(), Does.Not.Contain(3),
                "the copy-keyed fold commits onto the copy's own row");
            Assert.That((await Registry.GetShardMapAsync(logical))!.GetPhysicalShardIndices(), Does.Contain(3),
                "the logical row still routes the folded slots to the retired donor: the damage #6083 describes");
        });

        return (logical, physical, tree, expected);
    }

    private async Task<(string Logical, string Physical)> CreateAliasedTreeAsync(string prefix, bool derived)
    {
        var logical = $"{prefix}-{Guid.NewGuid():N}";
        var physical = logical + "-copy";

        await Registry.RegisterAsync(physical, new TreeRegistryEntry
        {
            ShardCount = BaseShardCount,
            ShardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, PhysicalShardCount),
            NextShardIndex = PhysicalShardCount - 1,
            DerivedFrom = derived ? logical : null,
        });
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = BaseShardCount });
        await Registry.SetAliasAsync(logical, physical);

        Assert.That(await PhysicalShardCountAsync(logical), Is.EqualTo(PhysicalShardCount),
            "The alias must carry the copy's over-split map onto the logical row.");
        return (logical, physical);
    }

    private async Task<int> PhysicalShardCountAsync(string treeId)
        => (await Registry.GetShardMapAsync(treeId))!.GetPhysicalShardIndices().Count;

    private static async Task<Dictionary<string, byte[]>> PopulateAsync(ILattice tree)
    {
        var expected = new Dictionary<string, byte[]>(KeyCount);
        for (var i = 0; i < KeyCount; i++)
        {
            var key = $"k-{i:D4}";
            var value = Encoding.UTF8.GetBytes($"v-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }

        return expected;
    }

    /// <summary>
    /// Reads every key and overwrites it through the logical tree, each call
    /// bounded well inside the stale-routing budget, so a slot that still routes
    /// to a retired shard fails here rather than after a minute-long stall.
    /// </summary>
    private static async Task AssertReadableAndWritableAsync(ILattice tree, Dictionary<string, byte[]> expected, string when)
    {
        foreach (var (key, value) in expected)
        {
            using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            Assert.That(await tree.GetAsync(key, budget.Token), Is.EqualTo(value).AsCollection,
                $"Key '{key}' must be readable {when}.");

            var rewritten = Encoding.UTF8.GetBytes($"{Encoding.UTF8.GetString(value)}-rewritten");
            await tree.SetAsync(key, rewritten, budget.Token);
            Assert.That(await tree.GetAsync(key, budget.Token), Is.EqualTo(rewritten).AsCollection,
                $"Key '{key}' must be writable {when}.");
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o =>
            {
                o.AutoSplitEnabled = false;
                o.HotShardMinSkewRatio = 1_000_001d;
                o.HotShardConsolidationSkewRatio = 1_000_000d;
                o.ShardHealingBackpressureOpsPerSecond = 0;
                o.ShardHealingInterval = TimeSpan.FromHours(1);
                // A fold's donor reports itself splitting while its shadow window is
                // open, which would otherwise hold the next fold back for minutes.
                o.ShardHealingCooldown = TimeSpan.Zero;
            });
        }
    }
}
