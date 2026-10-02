using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4250: the shard-map carry a shadow-cutover restore and a schema
/// remediation cutover make across their alias swap, and the carry back on a revert.
/// The end-to-end loss is proven in the backup and schema suites; these pin the
/// registry effects and the resume behaviour each step depends on.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AliasCutoverShardMapsTests
{
    private const int LogicalShards = 4;
    private const int DestinationShards = 3;

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

    private static int[] DefaultSlots(int shards) =>
        ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, shards).Slots;

    [Test]
    public async Task PrepareCutoverAsync_carries_the_destination_map_and_records_the_replaced_one()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;

        var replaced = await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        var after = (await Registry.GetShardMapAsync(logical))!;
        var recorded = await Registry.GetEntryAsync(destination);
        Assert.Multiple(() =>
        {
            Assert.That(replaced!.Slots, Is.EqualTo(before.Slots));
            Assert.That(after.Slots, Is.EqualTo(DefaultSlots(DestinationShards)));
            Assert.That(after.Version, Is.GreaterThan(before.Version), "a cached router must see the change");
            Assert.That(recorded!.ReplacedShardMap!.Slots, Is.EqualTo(before.Slots));
        });
    }

    [Test]
    public async Task PrepareCutoverAsync_resumed_before_the_swap_keeps_the_recorded_map()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        var replaced = await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        Assert.That(replaced!.Slots, Is.EqualTo(before.Slots),
            "a resumed cutover reads the destination's map back off the logical entry, so it must keep the first capture");
    }

    [Test]
    public async Task PrepareCutoverAsync_resumed_after_the_swap_leaves_the_logical_map_alone()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);
        await SetAliasAsync(logical, destination);
        var carried = (await Registry.GetShardMapAsync(logical))!;

        var replaced = await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        var after = (await Registry.GetShardMapAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(replaced!.Slots, Is.EqualTo(before.Slots));
            Assert.That(after.Version, Is.EqualTo(carried.Version));
        });
    }

    [Test]
    public async Task PrepareCutoverAsync_of_an_aliased_tree_stamps_the_logical_map_onto_the_replaced_tree()
    {
        var (logical, destination) = await RegisterAsync();
        var physical = $"{logical}-physical";
        await Registry.RegisterAsync(physical, new TreeRegistryEntry { ShardCount = 2 });
        await SetAliasAsync(logical, physical);
        var before = (await Registry.GetShardMapAsync(logical))!;

        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        Assert.That((await Registry.GetShardMapAsync(physical))!.Slots, Is.EqualTo(before.Slots),
            "the replaced tree was routed by the logical map, so that is what describes its shards");
    }

    [Test]
    public async Task PrepareRevertAsync_carries_the_recorded_map_back_and_stamps_the_shadow()
    {
        var (logical, shadow) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, shadow);
        await SetAliasAsync(logical, shadow);

        await AliasCutoverShardMaps.PrepareRevertAsync(Grains, logical, shadow, previousPhysicalTreeId: logical);
        // A revert resumed after the map was carried back must not stamp the
        // restored map onto the shadow.
        await AliasCutoverShardMaps.PrepareRevertAsync(Grains, logical, shadow, previousPhysicalTreeId: logical);

        var logicalAfter = (await Registry.GetShardMapAsync(logical))!;
        var shadowAfter = (await Registry.GetShardMapAsync(shadow))!;
        Assert.Multiple(() =>
        {
            Assert.That(logicalAfter.Slots, Is.EqualTo(before.Slots));
            Assert.That(shadowAfter.Slots, Is.EqualTo(DefaultSlots(DestinationShards)));
        });
    }

    [Test]
    public async Task CompleteRevertAsync_clears_the_recorded_map()
    {
        var (logical, shadow) = await RegisterAsync();
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, shadow);

        await AliasCutoverShardMaps.CompleteRevertAsync(Grains, shadow);

        var entry = await Registry.GetEntryAsync(shadow);
        Assert.Multiple(() =>
        {
            Assert.That(entry!.ReplacedShardMap, Is.Null);
            Assert.That(entry.ReplacedNextShardIndex, Is.Null);
        });
    }

    [Test]
    public void Helpers_reject_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.PrepareCutoverAsync(Grains, null!, "d"));
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.PrepareRevertAsync(Grains, "l", null!, "p"));
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.CompleteRevertAsync(null!, "s"));
        });
    }

    /// <summary>
    /// Registers a logical tree re-pinned to <see cref="LogicalShards"/> through the
    /// empty-tree reshard, and a destination on its default map at
    /// <see cref="DestinationShards"/>.
    /// </summary>
    private async Task<(string Logical, string Destination)> RegisterAsync()
    {
        var logical = $"cutover-maps-{Guid.NewGuid():N}";
        var destination = $"{logical}-destination";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Grains.GetGrain<ILattice>(logical).ReshardAsync(LogicalShards);
        await Registry.RegisterAsync(destination, new TreeRegistryEntry
        {
            ShardCount = DestinationShards,
            DerivedFrom = logical,
        });
        return (logical, destination);
    }

    private async Task SetAliasAsync(string logical, string physical)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SetAliasAsync(logical, physical);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
