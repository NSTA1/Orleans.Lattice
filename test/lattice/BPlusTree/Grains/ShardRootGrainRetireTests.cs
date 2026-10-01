using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for <see cref="ShardRootGrain.RetireAsync"/>: the release of a
/// consolidation donor's storage once it has been retired from the routing map.
/// <para>
/// Two obligations meet here. The storage must actually go - every leaf and
/// internal node cleared, which is what retires the leaves' WAL materialiser
/// pins - and the shard must stay a routing tombstone, because a caller holding
/// an older shard map can still reach it: its moved-away fence must survive,
/// and it must never grow a new root and start serving.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainRetireTests
{
    private const string TreeId = "retire-tree";
    private const int VirtualShardCount = 16;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static Harness CreateHarness(bool fenced = true)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/1"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = GrainId.Create("leaf", "leaf-0");
        state.State.RootIsLeaf = true;
        if (fenced)
        {
            state.State.MovedAwaySlots[1] = 0;
            state.State.MovedAwaySlots[3] = 0;
            state.State.MovedAwayVirtualShardCount = VirtualShardCount;
        }

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 2, factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, Leaf = leaf, State = state };
    }

    [Test]
    public async Task Retire_clears_the_leaves_and_keeps_only_the_routing_fence()
    {
        var h = CreateHarness();
        h.State.State.PendingLeafClears.Add(GrainId.Create("leaf", "owed"));

        await h.Grain.RetireAsync();

        await h.Leaf.Received().ClearGrainStateAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.IsRetired, Is.True);
            Assert.That(h.State.State.RootNodeId, Is.Null, "the shard must not keep routing to cleared nodes");
            Assert.That(h.State.State.PendingLeafClears, Is.Empty);
            Assert.That(h.State.State.MovedAwaySlots.Keys, Is.EquivalentTo(new[] { 1, 3 }),
                "the fence is what redirects a caller still holding an older shard map");
            Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount));
        });
    }

    [Test]
    public async Task IsRetired_reports_the_persisted_flag()
    {
        var h = CreateHarness();
        Assert.That(await h.Grain.IsRetiredAsync(), Is.False);

        await h.Grain.RetireAsync();

        Assert.That(await h.Grain.IsRetiredAsync(), Is.True);
    }

    [Test]
    public async Task Retire_is_idempotent()
    {
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        Assert.DoesNotThrowAsync(() => h.Grain.RetireAsync());
        Assert.That(h.State.State.IsRetired, Is.True);
    }

    [Test]
    public void Retire_refuses_a_shard_without_a_moved_away_fence()
    {
        var h = CreateHarness(fenced: false);

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RetireAsync());
        Assert.That(h.State.State.IsRetired, Is.False);
        Assert.That(h.State.State.RootNodeId, Is.Not.Null, "a refused retirement must change nothing");
    }

    [Test]
    public async Task Retire_refuses_a_shard_with_a_migration_in_progress()
    {
        var h = CreateHarness();
        h.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.Drain,
            ShadowTargetShardIndex = 0,
            MovedSlots = [5],
            VirtualShardCount = VirtualShardCount,
        };

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RetireAsync());
        Assert.That(h.State.State.IsRetired, Is.False);
        await h.Leaf.DidNotReceive().ClearGrainStateAsync();
    }

    [Test]
    public void A_write_failure_before_the_walk_leaves_the_shard_unretired()
    {
        var h = CreateHarness();
        h.State.ThrowOnWrite = new TimeoutException();

        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.RetireAsync());
        Assert.That(h.State.State.IsRetired, Is.False);
    }

    [Test]
    public async Task A_retired_shard_refuses_routed_operations_as_stale_routing_and_grows_no_root()
    {
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.SetAsync("k", [1]));
        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.GetAsync("k"));
        Assert.That(h.State.State.RootNodeId, Is.Null);
    }

    [Test]
    public async Task A_retired_shard_answers_range_reads_with_empty_pages()
    {
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        var keys = await h.Grain.GetSortedKeysBatchAsync(null, null, 10);
        var entries = await h.Grain.GetSortedEntriesBatchAsync(null, null, 10);
        var count = await h.Grain.CountBoundedAsync(null, null);
        var any = await h.Grain.AnyBoundedAsync(null);
        var moved = await h.Grain.CountWithMovedAwayBoundedAsync(null);

        Assert.Multiple(() =>
        {
            Assert.That(keys.Keys, Is.Empty);
            Assert.That(keys.HasMore, Is.False);
            Assert.That(entries.Entries, Is.Empty);
            Assert.That(count.Count, Is.Zero);
            Assert.That(count.ResumeFromInclusive, Is.Null);
            Assert.That(any.Found, Is.False);
            Assert.That(moved.Count, Is.Zero);
        });
    }

    [Test]
    public async Task A_retired_shard_hands_no_leaf_to_a_walker()
    {
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        Assert.That(await h.Grain.GetLeftmostLeafIdAsync(), Is.Null);
        Assert.That(await h.Grain.GetLeafIdForKeyAsync("k"), Is.Null);
    }

    [Test]
    public async Task A_retired_shard_answers_a_transaction_terminal_with_nothing_to_persist()
    {
        // Its leaves are gone, so it holds no prepared state; refusing would
        // fail the whole terminal broadcast on every retry instead.
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        var record = await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);

        Assert.That(record, Is.Null);
        Assert.That(h.State.State.RootNodeId, Is.Null, "a terminal must not grow a root on a retired shard");
    }

    [Test]
    public async Task A_retired_shard_refuses_bulk_loads_so_it_never_regrows_storage()
    {
        var h = CreateHarness();
        await h.Grain.RetireAsync();
        List<KeyValuePair<string, byte[]>> entries = [new("a", [1])];

        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.BulkAppendAsync("op-1", entries));
        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.BulkLoadAsync("op-2", entries));
        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.BulkLoadRawAsync("op-3", []));
        Assert.That(h.State.State.RootNodeId, Is.Null);
    }

    [Test]
    public async Task A_retired_shard_refuses_to_become_a_migration_source_as_a_refusal_not_a_stale_route()
    {
        // The split and consolidation coordinators unwind the intent they
        // persisted only on InvalidOperationException. A stale-routing fault
        // here would leave a split coordinator in progress against a shard it
        // can never open, retrying forever.
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        Assert.ThrowsAsync<InvalidOperationException>(
            () => h.Grain.BeginSplitAsync(2, [5], VirtualShardCount));
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.SplitInProgress, Is.Null);
            Assert.That(h.State.State.RootNodeId, Is.Null);
        });
    }

    [Test]
    public async Task GetMigrationTargetShardIndex_reports_the_in_flight_target_and_null_otherwise()
    {
        var h = CreateHarness();
        Assert.That(await h.Grain.GetMigrationTargetShardIndexAsync(), Is.Null);

        h.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.Drain,
            ShadowTargetShardIndex = 7,
            MovedSlots = [5],
            VirtualShardCount = VirtualShardCount,
        };

        Assert.That(await h.Grain.GetMigrationTargetShardIndexAsync(), Is.EqualTo(7));
    }

    [Test]
    public async Task Revive_returns_a_retired_shard_to_service_and_lifts_the_fence_on_the_slots_it_owns_again()
    {
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        await h.Grain.ReviveAsync([1, 3], VirtualShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.IsRetired, Is.False);
            Assert.That(h.State.State.MovedAwaySlots, Is.Empty,
                "a revived shard is routed a fresh identity map, so an old fence would refuse slots it now owns");
            Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.Null);
        });
    }

    [Test]
    public async Task Revive_keeps_the_fence_on_slots_the_new_map_routes_elsewhere()
    {
        // Slot 3 now belongs to another shard: a caller whose map predates the
        // one being published must still be redirected, not served empty.
        var h = CreateHarness();
        await h.Grain.RetireAsync();

        await h.Grain.ReviveAsync([1], VirtualShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.IsRetired, Is.False);
            Assert.That(h.State.State.MovedAwaySlots.Keys, Is.EquivalentTo(new[] { 3 }));
            Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount));
        });
    }

    [Test]
    public void Revive_validates_its_arguments()
    {
        var h = CreateHarness();

        Assert.ThrowsAsync<ArgumentNullException>(() => h.Grain.ReviveAsync(null!, VirtualShardCount));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => h.Grain.ReviveAsync([1], 0));
    }

    [Test]
    public async Task Revive_is_a_no_op_on_a_shard_that_is_not_retired()
    {
        var h = CreateHarness();
        var writesBefore = h.State.WriteCount;

        await h.Grain.ReviveAsync([1, 3], VirtualShardCount);

        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore));
        Assert.That(h.State.State.MovedAwaySlots.Keys, Is.EquivalentTo(new[] { 1, 3 }),
            "a live shard's fence is never touched by revive");
        Assert.That(h.State.State.RootNodeId, Is.Not.Null);
    }

    [Test]
    public async Task Revive_finishes_an_interrupted_retirement_before_returning_the_shard_to_service()
    {
        // Retired flag persisted, then the walk died: the topology is still referenced.
        var h = CreateHarness();
        h.State.State.IsRetired = true;

        await h.Grain.ReviveAsync([], VirtualShardCount);

        await h.Leaf.Received().ClearGrainStateAsync();
        Assert.That(h.State.State.RootNodeId, Is.Null);
        Assert.That(h.State.State.IsRetired, Is.False);
    }
}
