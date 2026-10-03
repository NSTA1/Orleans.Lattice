using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4250: the shard-map carry a shadow-cutover restore and a schema
/// remediation cutover make across their alias swap, and the carry back on a revert;
/// and (issue #4263) the carry an explicit alias set makes.
/// The end-to-end loss is proven in the backup and schema suites; these pin the
/// registry effects and the resume behaviour each step depends on, and (issue
/// #4336) that the alias and its map move in one registry write.
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
    public async Task PrepareCutoverAsync_records_the_replaced_map_and_leaves_the_logical_entry_alone()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetEntryAsync(logical))!;

        var replaced = await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        var after = (await Registry.GetEntryAsync(logical))!;
        var recorded = await Registry.GetEntryAsync(destination);
        Assert.Multiple(() =>
        {
            Assert.That(replaced!.Slots, Is.EqualTo(before.ShardMap!.Slots));
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(before.ShardMap.Slots),
                "the logical map moves only with the alias (#4336)");
            Assert.That(after.ShardMap.Version, Is.EqualTo(before.ShardMap.Version));
            Assert.That(after.PhysicalTreeId, Is.Null);
            Assert.That(after.AliasCutoverTarget, Is.Null);
            Assert.That(recorded!.ReplacedShardMap!.Slots, Is.EqualTo(before.ShardMap.Slots));
        });
    }

    [Test]
    public async Task PrepareCutoverAsync_resumed_before_the_swap_keeps_the_recorded_map()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        var replaced = await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        Assert.That(replaced!.Slots, Is.EqualTo(before.Slots));
    }

    [Test]
    public async Task SwapCutoverAsync_moves_the_alias_and_the_destination_map_in_one_write()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        var replaced = await AliasCutoverShardMaps.SwapCutoverAsync(Grains, logical, destination);

        var after = (await Registry.GetEntryAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.EqualTo(destination));
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(DefaultSlots(DestinationShards)));
            Assert.That(after.ShardMap.Version, Is.GreaterThan(before.Version), "a cached router must see the change");
            Assert.That(after.AliasCutoverTarget, Is.Null);
            Assert.That(replaced!.Slots, Is.EqualTo(before.Slots));
        });
    }

    [Test]
    public async Task SwapCutoverAsync_resumed_after_the_swap_leaves_the_logical_map_alone()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);
        await AliasCutoverShardMaps.SwapCutoverAsync(Grains, logical, destination);
        var carried = (await Registry.GetShardMapAsync(logical))!;

        var replacedOnPrepare = await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);
        var replacedOnSwap = await AliasCutoverShardMaps.SwapCutoverAsync(Grains, logical, destination);

        var after = (await Registry.GetShardMapAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(replacedOnPrepare!.Slots, Is.EqualTo(before.Slots));
            Assert.That(replacedOnSwap!.Slots, Is.EqualTo(before.Slots));
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
    public async Task RevertAsync_carries_the_recorded_map_back_with_the_alias_and_stamps_the_shadow()
    {
        var (logical, shadow) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, shadow);
        await AliasCutoverShardMaps.SwapCutoverAsync(Grains, logical, shadow);
        var cutOver = (await Registry.GetShardMapAsync(logical))!;

        await AliasCutoverShardMaps.RevertAsync(Grains, logical, shadow, previousPhysicalTreeId: logical);
        // A revert resumed after the swap changes nothing further.
        await AliasCutoverShardMaps.RevertAsync(Grains, logical, shadow, previousPhysicalTreeId: logical);

        var logicalAfter = (await Registry.GetEntryAsync(logical))!;
        var shadowAfter = (await Registry.GetShardMapAsync(shadow))!;
        Assert.Multiple(() =>
        {
            Assert.That(logicalAfter.PhysicalTreeId, Is.Null, "moved back onto its own shards");
            Assert.That(logicalAfter.ShardMap!.Slots, Is.EqualTo(before.Slots));
            Assert.That(logicalAfter.ShardMap.Version, Is.GreaterThan(cutOver.Version));
            Assert.That(logicalAfter.AliasCutoverTarget, Is.Null);
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

    /// <summary>
    /// Issue #4264 end to end: a split begun on the replaced tree and driven to
    /// its commit after the cutover must leave the carried map untouched. Its
    /// diff names a target shard of the replaced tree, which the destination's
    /// map does not have, so applying it would route the moved slots nowhere.
    /// </summary>
    [Test]
    public async Task A_split_in_flight_across_the_cutover_leaves_the_carried_map_untouched()
    {
        var (logical, destination) = await RegisterAsync();
        var split = Grains.GetGrain<ITreeShardSplitGrain>($"{logical}/0");
        await split.SplitAsync(sourceShardIndex: 0);

        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);
        await AliasCutoverShardMaps.SwapCutoverAsync(Grains, logical, destination);
        var carried = (await Registry.GetShardMapAsync(logical))!;

        await split.RunSplitPassAsync();

        var after = (await Registry.GetShardMapAsync(logical))!;
        var idle = await split.IsIdleAsync();
        Assert.Multiple(() =>
        {
            Assert.That(after.Slots, Is.EqualTo(carried.Slots),
                "the split's diff names the replaced tree's shards, not the destination's");
            Assert.That(after.Slots, Is.EqualTo(DefaultSlots(DestinationShards)));
            Assert.That(idle, Is.True, "the split is abandoned, not left in flight");
        });
    }

    /// <summary>
    /// A split that commits onto the replaced tree's map after the prepare recorded
    /// it, but before the swap, is part of that tree's final layout: the swap
    /// returns the map it actually replaced and re-records it for a revert.
    /// </summary>
    [Test]
    public async Task A_split_committing_between_the_prepare_and_the_swap_is_recorded_as_the_replaced_layout()
    {
        var (logical, destination) = await RegisterAsync();
        var before = (await Registry.GetShardMapAsync(logical))!;
        var split = Grains.GetGrain<ITreeShardSplitGrain>($"{logical}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await AliasCutoverShardMaps.PrepareCutoverAsync(Grains, logical, destination);

        await split.RunSplitPassAsync();
        var afterSplit = (await Registry.GetShardMapAsync(logical))!;
        var replaced = await AliasCutoverShardMaps.SwapCutoverAsync(Grains, logical, destination);

        var recorded = await Registry.GetEntryAsync(destination);
        Assert.Multiple(() =>
        {
            Assert.That(afterSplit.Slots, Is.Not.EqualTo(before.Slots), "precondition: the split committed");
            Assert.That(replaced!.Slots, Is.EqualTo(afterSplit.Slots));
            Assert.That(recorded!.ReplacedShardMap!.Slots, Is.EqualTo(afterSplit.Slots));
        });
    }

    [Test]
    public async Task CarryAcrossExplicitAliasAsync_moves_the_alias_with_the_target_map_and_stamps_the_replaced_tree()
    {
        var (logical, target) = await RegisterAsync();
        var physical = $"{logical}-physical";
        await Registry.RegisterAsync(physical, new TreeRegistryEntry { ShardCount = 2 });
        await SetAliasAsync(logical, physical);
        var before = (await Registry.GetShardMapAsync(logical))!;

        await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, target);

        var after = (await Registry.GetEntryAsync(logical))!;
        var replaced = (await Registry.GetShardMapAsync(physical))!;
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.EqualTo(target));
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(DefaultSlots(DestinationShards)),
                "a target with no persisted map is routed by the default map for its own pin");
            Assert.That(after.ShardMap.Version, Is.GreaterThan(before.Version), "a cached router must see the change");
            Assert.That(replaced.Slots, Is.EqualTo(before.Slots),
                "the replaced tree was routed by the logical map, so that is what describes its shards");
        });
    }

    [Test]
    public async Task CarryAcrossExplicitAliasAsync_re_set_of_the_current_alias_leaves_the_logical_map_alone()
    {
        var (logical, target) = await RegisterAsync();
        await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, target);
        var before = (await Registry.GetShardMapAsync(logical))!;

        await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, target);

        var after = (await Registry.GetEntryAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.EqualTo(target));
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(before.Slots), "the logical map already describes the target");
            Assert.That(after.ShardMap.Version, Is.EqualTo(before.Version));
        });
    }

    [Test]
    public async Task CarryAcrossExplicitAliasAsync_refused_swap_changes_nothing()
    {
        var (logical, target) = await RegisterAsync();
        var elsewhere = $"{logical}-elsewhere";
        await Registry.RegisterAsync(elsewhere, new TreeRegistryEntry { ShardCount = 2, DerivedFrom = target });
        await SetAliasAsync(target, elsewhere);
        var before = (await Registry.GetEntryAsync(logical))!;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, target),
            "a target that is itself aliased is refused");

        var after = (await Registry.GetEntryAsync(logical))!;
        Assert.Multiple(() =>
        {
            Assert.That(after.PhysicalTreeId, Is.Null);
            Assert.That(after.ShardMap!.Slots, Is.EqualTo(before.ShardMap!.Slots));
            Assert.That(after.ShardMap.Version, Is.EqualTo(before.ShardMap.Version));
        });
    }

    [Test]
    public void Helpers_reject_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.PrepareCutoverAsync(Grains, null!, "d"));
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.SwapCutoverAsync(Grains, "l", null!));
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.RevertAsync(Grains, "l", "s", null!));
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.CompleteRevertAsync(null!, "s"));
            Assert.ThrowsAsync<ArgumentNullException>(() => AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, "l", null!));
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
