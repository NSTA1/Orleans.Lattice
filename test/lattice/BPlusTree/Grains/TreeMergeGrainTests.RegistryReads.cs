using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Initiating a merge reads the source's alias and shard map from one registry
/// entry: <c>ResolveAsync</c> and <c>GetShardMapAsync</c> are projections of
/// that entry, so asking for them separately paid extra serial registry turns.
/// </summary>
public partial class TreeMergeGrainTests
{
    [Test]
    public async Task MergeAsync_reuses_one_source_entry_for_existence_options_and_intent()
    {
        var (grain, state, reminders, factory, _) = CreateGrain();
        ((IGrainBase)grain).GrainContext.ActivationServices.Returns(
            new ServiceCollection().AddSingleton(Substitute.For<ITimerRegistry>()).BuildServiceProvider());
        SetupKeepalive(reminders);
        var registry = StubSourceEntry(factory, "source-copy", new ShardMap { Slots = [0, 4], Version = 2 });
        registry.ResolveAsync(TargetTreeId).Returns("target-copy");
        registry.ClearReceivedCalls();

        await grain.MergeAsync(SourceTreeId);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo("source-copy"));
            Assert.That(state.State.TargetPhysicalTreeId, Is.EqualTo("target-copy"));
            Assert.That(state.State.SourceShardCount, Is.EqualTo(ShardCount));
            Assert.That(state.State.SourcePhysicalShards, Is.EqualTo(new[] { 0, 4 }));
            Assert.That(registry.ReceivedCalls().Count(), Is.EqualTo(2));
        });
        await registry.Received(1).GetEntryAsync(SourceTreeId);
        await registry.DidNotReceive().ExistsAsync(Arg.Any<string>());
        await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
    }

    [Test]
    public void MergeAsync_missing_source_does_not_seed_a_registry_row()
    {
        var (grain, _, _, factory, _) = CreateGrain();
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(SourceTreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));
        registry.ClearReceivedCalls();

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.MergeAsync(SourceTreeId));

        registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
        Assert.That(registry.ReceivedCalls().Count(), Is.EqualTo(1));
    }

    [Test]
    public async Task MergeAsync_incomplete_source_keeps_lazy_seeding_and_fresh_topology()
    {
        var (grain, state, reminders, factory, _) = CreateGrain();
        ((IGrainBase)grain).GrainContext.ActivationServices.Returns(
            new ServiceCollection().AddSingleton(Substitute.For<ITimerRegistry>()).BuildServiceProvider());
        SetupKeepalive(reminders);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = new TreeRegistryEntry { PhysicalTreeId = "source-copy", ShardMap = new ShardMap { Slots = [0, 4] } };
        registry.GetEntryAsync(SourceTreeId).Returns(Task.FromResult<TreeRegistryEntry?>(entry));
        registry.ClearReceivedCalls();

        await grain.MergeAsync(SourceTreeId);

        await registry.Received(1).RegisterAsync(SourceTreeId, entry);
        await registry.Received(4).GetEntryAsync(SourceTreeId);
        Assert.That(state.State.SourcePhysicalShards, Is.EqualTo(new[] { 0, 4 }));
    }

    [Test]
    public async Task InitiateMergeState_reads_the_source_topology_from_one_registry_entry()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var registry = StubSourceEntry(grainFactory, "source-physical", new ShardMap { Slots = [0, 0, 4, 4], Version = 2 });
        registry.ResolveAsync(TargetTreeId).Returns("target-physical");
        registry.ClearReceivedCalls();

        await grain.InitiateMergeStateAsync(SourceTreeId, ShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo("source-physical"));
            Assert.That(state.State.TargetPhysicalTreeId, Is.EqualTo("target-physical"));
            Assert.That(state.State.SourcePhysicalShards, Is.EqualTo(new[] { 0, 4 }));
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.ResolveAsync)
                && (string)c.GetArguments()[0]! == SourceTreeId), Is.Zero);
            Assert.That(registry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)), Is.Zero);
            // One source entry read plus the target alias.
            Assert.That(registry.ReceivedCalls().Count(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task MergeShardAsync_reads_target_entry_once_for_options_and_shard_map()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 0);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.SourcePhysicalTreeId = SourceTreeId;
        state.State.TargetPhysicalTreeId = TargetTreeId;
        state.State.SourcePhysicalShards = [0];
        state.State.NextShardIndex = 0;

        registry.ClearReceivedCalls();

        await grain.RunMergePassAsync();

        // Shard drain reads target registry entry once for options + shard map,
        // without calling GetShardMapAsync or ResolveAsync.
        Assert.Multiple(() =>
        {
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)), Is.Zero);
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.ResolveAsync)
                && (string)c.GetArguments()[0]! == TargetTreeId), Is.Zero);
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetEntryAsync)
                && (string)c.GetArguments()[0]! == TargetTreeId), Is.GreaterThanOrEqualTo(1));
        });
    }
}
