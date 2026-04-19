using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for audit bug #1: <see cref="TombstoneCompactionGrain"/> must
/// (a) resolve the tree alias through the registry, so compaction targets the physical
/// tree id actually storing the shards, and (b) iterate the distinct physical shard
/// indices recorded in the current <see cref="ShardMap"/> rather than
/// <c>0..options.ShardCount</c>. Otherwise compaction silently skips shards created
/// by adaptive splits (slots above the initial count) and targets the wrong grain
/// key for aliased trees.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task Compaction_resolves_alias_and_targets_physical_tree_id()
    {
        const string physicalTreeId = "physical-tree";
        var (grain, state, _, grainFactory, _) = CreateGrain();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(physicalTreeId);
        registry.GetShardMapAsync(TreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, 2)));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .Returns(registry);

        var physicalShardRoot = Substitute.For<IShardRootGrain>();
        physicalShardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/0")
            .Returns(physicalShardRoot);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        _ = physicalShardRoot.Received().GetLeftmostLeafIdAsync();

        // And the grain must never have asked for the logical-keyed shard root.
        grainFactory.DidNotReceive().GetGrain<IShardRootGrain>($"{TreeId}/0");
    }

    [Test]
    public async Task Compaction_iterates_physical_shards_from_ShardMap_not_option_count()
    {
        // Simulate a tree that has been adaptively split so physical shard
        // indices are {0, 5} — not contiguous and not bounded by options.ShardCount.
        var customMap = new ShardMap
        {
            Slots = [0, 0, 0, 0, 5, 5, 5, 5],
            Version = 3
        };

        var (grain, state, _, grainFactory, _) = CreateGrain();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(Task.FromResult<ShardMap?>(customMap));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .Returns(registry);

        var shard0 = Substitute.For<IShardRootGrain>();
        shard0.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        var shard5 = Substitute.For<IShardRootGrain>();
        shard5.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shard0);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/5").Returns(shard5);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // Two physical shards — must tick twice to cover {0, 5} before completing.
        await grain.ProcessNextShardAsync();
        await grain.ProcessNextShardAsync();
        await grain.ProcessNextShardAsync(); // triggers completion

        await shard0.Received().GetLeftmostLeafIdAsync();
        await shard5.Received().GetLeftmostLeafIdAsync();
        Assert.That(state.State.InProgress, Is.False,
            "Pass must complete after covering every physical shard in the map.");
    }

    [Test]
    public async Task ReceiveReminder_reregisters_when_period_drifts_from_options()
    {
        // FX-002: when a stale reminder period survives an options change
        // (or an Orleans upgrade), the next tick must re-register with
        // the currently configured period.
        var options = new LatticeOptions
        {
            ShardCount = ShardCount,
            TombstoneGracePeriod = TimeSpan.FromHours(12),
        };
        var (grain, _, reminderRegistry, grainFactory, _) = CreateGrain(options);

        // Stub registry so the reminder callback doesn't null-ref.
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(Task.FromResult<ShardMap?>(
            ShardMap.CreateDefault(4, ShardCount)));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        for (int i = 0; i < ShardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shard);
        }

        // Fire the periodic compaction reminder with a stale period.
        var stalePeriod = TimeSpan.FromHours(1);
        var staleStatus = new TickStatus(DateTime.UtcNow, stalePeriod, DateTime.UtcNow);
        // ReceiveReminder triggers StartCompactionAsync after the drift check, which
        // requires a real grain-timer service provider; we only care that the
        // drift-detection branch calls RegisterOrUpdateReminder before that point.
        try { await grain.ReceiveReminder("tombstone-compaction", staleStatus); }
        catch (InvalidOperationException) { /* expected: no grain context timer service */ }

        // Grain must have re-registered with the configured 12h period.
        await reminderRegistry.Received().RegisterOrUpdateReminder(
            callingGrainId: Arg.Any<GrainId>(),
            reminderName: "tombstone-compaction",
            dueTime: TimeSpan.FromHours(12),
            period: TimeSpan.FromHours(12));
    }

    [Test]
    public async Task ReceiveReminder_does_not_reregister_when_period_matches()
    {
        var options = new LatticeOptions
        {
            ShardCount = ShardCount,
            TombstoneGracePeriod = TimeSpan.FromHours(2),
        };
        var (grain, _, reminderRegistry, grainFactory, _) = CreateGrain(options);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(Task.FromResult<ShardMap?>(
            ShardMap.CreateDefault(4, ShardCount)));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        for (int i = 0; i < ShardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shard);
        }

        // Matching period → no re-register.
        var status = new TickStatus(DateTime.UtcNow, TimeSpan.FromHours(2), DateTime.UtcNow);
        reminderRegistry.ClearReceivedCalls();
        try { await grain.ReceiveReminder("tombstone-compaction", status); }
        catch (InvalidOperationException) { /* expected: StartCompactionAsync needs grain timer svc */ }

        await reminderRegistry.DidNotReceive().RegisterOrUpdateReminder(
            callingGrainId: Arg.Any<GrainId>(),
            reminderName: "tombstone-compaction",
            dueTime: Arg.Any<TimeSpan>(),
            period: Arg.Any<TimeSpan>());
    }
}
