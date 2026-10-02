using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Regression coverage for which shards <see cref="SagaWriteFenceGrain"/>
/// fences. The fence must cover every shard the tree's live routing reaches -
/// an adaptive split's target above the pinned shard count, and the aliased
/// physical copy after a resize or an earlier restore - not
/// <c>{tree}/0..ShardCount-1</c>, and every lift must release exactly the set
/// the engage fenced even once the cutover's alias swap has moved routing.
/// </summary>
public partial class SagaWriteFenceGrainTests
{
    private sealed class RoutedHarness
    {
        public required SagaWriteFenceGrain Grain { get; init; }
        public required FakePersistentState<SagaWriteFenceState> State { get; init; }
        public required IShardCountProvider ShardCounts { get; init; }
        public required ConcurrentDictionary<string, IShardRootGrain> Shards { get; init; }

        public IShardRootGrain Shard(string key) =>
            Shards.GetOrAdd(key, static _ => Substitute.For<IShardRootGrain>());

        public void RouteTo(params string[] keys) =>
            ShardCounts.GetShardRootKeysAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<string>>(keys));
    }

    private static RoutedHarness CreateRoutedGrain(FakePersistentState<SagaWriteFenceState>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-write-fence", SagaId));
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        // The pinned count stays 2 throughout: the defect was trusting it.
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardIndicesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<int>>(Enumerable.Range(0, ShardCount).ToArray()));

        var shards = new ConcurrentDictionary<string, IShardRootGrain>(StringComparer.Ordinal);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>())
            .Returns(call => shards.GetOrAdd(call.ArgAt<string>(0), static _ => Substitute.For<IShardRootGrain>()));
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationShipperGrain>());
        factory.GetGrain<ITreeReceiveFenceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<ITreeReceiveFenceGrain>());

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.CurrentValue.Returns(new LatticeOptions());

        state ??= new FakePersistentState<SagaWriteFenceState>();
        var grain = new SagaWriteFenceGrain(
            context, reminders, NullLogger<SagaWriteFenceGrain>.Instance,
            state, shardCounts, new FakeReplicationTopology(["peer-a"]),
            factory, new FakeSagaCompletionSource(), options);

        return new RoutedHarness { Grain = grain, State = state, ShardCounts = shardCounts, Shards = shards };
    }

    [Test]
    public async Task Engage_fences_the_routed_physical_shards_not_the_pinned_logical_range()
    {
        var h = CreateRoutedGrain();
        // A resized tree (physical id differs from the logical one) whose
        // adaptive split moved slots to shard 4, above the pinned count of 2.
        h.RouteTo("orders-resized-1/0", "orders-resized-1/1", "orders-resized-1/4");

        await h.Grain.EngageAsync(Request("orders"));

        foreach (var key in new[] { "orders-resized-1/0", "orders-resized-1/1", "orders-resized-1/4" })
        {
            await h.Shard(key).Received(1).EngageWriteFenceAsync(SagaId, Arg.Any<long>());
        }

        // The retired copy under the logical id is not what serves writers.
        await h.Shard("orders/0").DidNotReceive().EngageWriteFenceAsync(Arg.Any<string>(), Arg.Any<long>());
        await h.Shard("orders/1").DidNotReceive().EngageWriteFenceAsync(Arg.Any<string>(), Arg.Any<long>());
        Assert.That(h.State.State.FencedShardKeys,
            Is.EqualTo(new[] { "orders-resized-1/0", "orders-resized-1/1", "orders-resized-1/4" }));
    }

    [Test]
    public async Task Unblock_lifts_the_engaged_shards_after_the_cutover_moved_routing()
    {
        var h = CreateRoutedGrain();
        h.RouteTo("orders/0", "orders/1");
        await h.Grain.EngageAsync(Request("orders"));

        // The cutover's alias swap now routes the tree to the restored shadow.
        h.RouteTo("orders-shadow-9/0", "orders-shadow-9/1");
        await h.Grain.UnblockWritesAsync();

        await h.Shard("orders/0").Received(1).LiftWriteFenceAsync(SagaId);
        await h.Shard("orders/1").Received(1).LiftWriteFenceAsync(SagaId);
        await h.Shard("orders-shadow-9/0").DidNotReceive().LiftWriteFenceAsync(Arg.Any<string>());
        await h.Shard("orders-shadow-9/1").DidNotReceive().LiftWriteFenceAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Re_engage_of_an_active_fence_keeps_the_shards_it_already_fenced()
    {
        var h = CreateRoutedGrain();
        h.RouteTo("orders/0", "orders/1");
        await h.Grain.EngageAsync(Request("orders"));

        // Routing moves between two engages of the same saga: the first set is
        // still fenced and must still be lifted.
        h.RouteTo("orders/0", "orders/1", "orders/3");
        await h.Grain.EngageAsync(Request("orders"));

        Assert.That(h.State.State.FencedShardKeys,
            Is.EquivalentTo(new[] { "orders/0", "orders/1", "orders/3" }));

        await h.Grain.LiftAsync();

        foreach (var key in new[] { "orders/0", "orders/1", "orders/3" })
        {
            await h.Shard(key).Received(1).LiftWriteFenceAsync(SagaId);
        }
    }

    [Test]
    public async Task Engage_after_a_lift_fences_only_the_current_routing()
    {
        var h = CreateRoutedGrain();
        h.RouteTo("orders/0", "orders/1");
        await h.Grain.EngageAsync(Request("orders"));
        await h.Grain.LiftAsync();

        h.RouteTo("orders-shadow-9/0");
        await h.Grain.EngageAsync(Request("orders"));

        Assert.That(h.State.State.FencedShardKeys, Is.EqualTo(new[] { "orders-shadow-9/0" }));
    }

    [Test]
    public async Task Engage_that_cannot_resolve_routing_changes_no_fence_state()
    {
        var h = CreateRoutedGrain();
        h.ShardCounts.GetShardRootKeysAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<IReadOnlyList<string>>>(_ => throw new TimeoutException("routing unavailable"));

        Assert.That(() => h.Grain.EngageAsync(Request("orders")), Throws.InstanceOf<TimeoutException>());

        var snap = await h.Grain.GetSnapshotAsync();
        Assert.Multiple(() =>
        {
            Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.None));
            Assert.That(h.State.State.FencedShardKeys, Is.Empty);
            Assert.That(h.Shards, Is.Empty, "no shard may be fenced when the fenced set is unknown");
        });
    }

    [Test]
    public async Task Lift_of_state_without_a_recorded_shard_set_falls_back_to_current_routing()
    {
        // State persisted before the engaged set was recorded: engaged, past its
        // deadline, and carrying no FencedShardKeys.
        var legacy = new FakePersistentState<SagaWriteFenceState>();
        legacy.State.SagaId = SagaId;
        legacy.State.Trees = ["orders"];
        legacy.State.Phase = SagaWriteFencePhase.Engaged;
        legacy.State.FenceDeadlineTicks = DateTime.UtcNow.AddSeconds(-1).Ticks;
        legacy.State.EngagedAtTicks = DateTime.UtcNow.AddSeconds(-10).Ticks;

        var h = CreateRoutedGrain(legacy);
        h.RouteTo("orders/0", "orders/5");

        var snap = await h.Grain.PollResumeAsync();

        Assert.That(snap.WritesUnblocked, Is.True);
        await h.Shard("orders/0").Received(1).LiftWriteFenceAsync(SagaId);
        await h.Shard("orders/5").Received(1).LiftWriteFenceAsync(SagaId);
    }
}
