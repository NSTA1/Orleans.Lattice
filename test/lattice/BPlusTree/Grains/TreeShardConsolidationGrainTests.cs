using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for <see cref="TreeShardConsolidationGrain"/>, the coordinator that
/// folds one physical donor shard back onto an adjacent survivor and retires
/// it from the routing map.
/// <para>
/// Consolidation is the inverse of an adaptive split and the only way to
/// repair a tree that a runaway split shattered. These tests pin the phase
/// machine, the ordering invariants that keep every key reachable through the
/// fold, the bounded/resumable drain, cancellation, and idempotence.
/// </para>
/// </summary>
[TestFixture]
public partial class TreeShardConsolidationGrainTests
{
    private const string TreeId = "consolidation-test-tree";
    private const int VirtualShardCount = 16;

    /// <summary>
    /// Ordered log of the cross-grain calls the coordinator makes, so ordering
    /// invariants are asserted on the actual call sequence rather than inferred.
    /// </summary>
    private sealed class CallLog
    {
        public List<string> Entries { get; } = [];
        public void Record(string step) => Entries.Add(step);
        public int IndexOf(string step) => Entries.IndexOf(step);
    }

    private sealed class Harness
    {
        public required TreeShardConsolidationGrain Grain { get; init; }
        public required FakePersistentState<TreeShardConsolidationState> State { get; init; }
        public required ILatticeRegistry Registry { get; init; }
        public required IShardRootGrain Donor { get; init; }
        public required IShardRootGrain Survivor { get; init; }
        public required CallLog Log { get; init; }
        public required FakeTimeProvider Clock { get; init; }

        /// <summary>
        /// Live view of the map the fake registry currently holds. Read through
        /// an accessor rather than a snapshot so an assertion always observes
        /// what the coordinator actually persisted.
        /// </summary>
        public required Func<ShardMap?> PersistedMapAccessor { get; init; }

        /// <summary>Replaces the registry's map, modelling a concurrent topology change.</summary>
        public required Action<ShardMap?> SetPersistedMap { get; init; }

        public ShardMap? PersistedMap
        {
            get => PersistedMapAccessor();
            set => SetPersistedMap(value);
        }
    }

    /// <summary>
    /// Deterministic clock: every read advances by a fixed step so progress
    /// timestamps are strictly increasing without any dependence on the wall
    /// clock or on how long a test happens to take.
    /// </summary>
    private sealed class FakeTimeProvider : TimeProvider
    {
        private long _ticks = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        public override DateTimeOffset GetUtcNow()
        {
            _ticks += TimeSpan.TicksPerSecond;
            return new DateTimeOffset(_ticks, TimeSpan.Zero);
        }
    }

    /// <summary>
    /// Builds a coordinator over a two-shard identity map where the donor owns
    /// the odd virtual slots. <paramref name="leafEntries"/> seeds the donor's
    /// leaf chain: one dictionary per leaf, walked in order.
    /// </summary>
    private static Harness CreateGrain(
        int donorShardIndex = 1,
        int survivorShardIndex = 0,
        ShardMap? existingMap = null,
        FakePersistentState<TreeShardConsolidationState>? existingState = null,
        IReadOnlyList<Dictionary<string, LwwValue<byte[]>>>? leafEntries = null,
        LatticeOptions? options = null)
    {
        var log = new CallLog();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("consolidation", $"{TreeId}/{donorShardIndex}"));

        // The coordinator base class arms a phase timer through the
        // activation's service provider, so a test that drives StartAsync
        // end-to-end needs a timer registry wired in.
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 2 }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);

        ShardMap? persistedMap = existingMap ?? ShardMap.CreateDefault(VirtualShardCount, 2);
        registry.GetShardMapAsync(TreeId).Returns(_ => Task.FromResult<ShardMap?>(persistedMap));
        registry.SetShardMapAsync(TreeId, Arg.Any<ShardMap>()).Returns(ci =>
        {
            persistedMap = (ShardMap)ci[1];
            log.Record("registry.SetShardMap");
            return Task.CompletedTask;
        });

        var donor = Substitute.For<IShardRootGrain>();
        var survivor = Substitute.For<IShardRootGrain>();

        donor.IsSplittingAsync().Returns(Task.FromResult(false));
        survivor.IsSplittingAsync().Returns(Task.FromResult(false));
        donor.BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(_ => { log.Record("donor.BeginSplit"); return Task.CompletedTask; });
        donor.MarkLeavesMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(_ => { log.Record("donor.MarkLeavesMovedAway"); return Task.FromResult(1); });
        donor.EnterRejectPhaseAsync()
            .Returns(_ => { log.Record("donor.EnterReject"); return Task.CompletedTask; });
        donor.CompleteSplitAsync()
            .Returns(_ => { log.Record("donor.CompleteSplit"); return Task.CompletedTask; });
        donor.AbortSplitAsync()
            .Returns(_ => { log.Record("donor.AbortSplit"); return Task.CompletedTask; });
        survivor.ReclaimSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(ci => { log.Record("survivor.ReclaimSlots"); return Task.FromResult(((int[])ci[0]).Length); });
        survivor.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>())
            .Returns(_ => { log.Record("survivor.MergeMany"); return Task.CompletedTask; });

        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return idx == donorShardIndex ? donor : survivor;
        });

        WireLeafChain(grainFactory, donor, leafEntries, log);

        var clock = new FakeTimeProvider();
        var state = existingState ?? new FakePersistentState<TreeShardConsolidationState>();
        var grain = new TreeShardConsolidationGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeShardConsolidationGrain>(), state)
        {
            Clock = clock,
        };

        return new Harness
        {
            Grain = grain,
            State = state,
            Registry = registry,
            Donor = donor,
            Survivor = survivor,
            Log = log,
            Clock = clock,
            PersistedMapAccessor = () => persistedMap,
            SetPersistedMap = m => persistedMap = m,
        };
    }

    /// <summary>
    /// Wires a donor leaf chain of <c>leafEntries.Count</c> leaves, each
    /// returning its own delta and pointing at the next. A null or empty list
    /// gives the donor no leaves at all.
    /// </summary>
    private static void WireLeafChain(
        IGrainFactory grainFactory,
        IShardRootGrain donor,
        IReadOnlyList<Dictionary<string, LwwValue<byte[]>>>? leafEntries,
        CallLog log)
    {
        if (leafEntries is null || leafEntries.Count == 0)
        {
            donor.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            return;
        }

        var leafIds = new GrainId[leafEntries.Count];
        for (var i = 0; i < leafEntries.Count; i++)
            leafIds[i] = GrainId.Create("leaf", $"donor-leaf-{i}");

        donor.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafIds[0]));

        var leaves = new IBPlusLeafGrain[leafEntries.Count];
        for (var i = 0; i < leafEntries.Count; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.GetDeltaSinceForSlotsAsync(Arg.Any<VersionVector>(), Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(_ =>
                {
                    log.Record($"leaf{index}.GetDelta");
                    return Task.FromResult(new StateDelta
                    {
                        Entries = leafEntries[index],
                        Version = new VersionVector(),
                    });
                });
            leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(
                index + 1 < leafIds.Length ? leafIds[index + 1] : null));
            leaves[i] = leaf;
        }

        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(ci =>
        {
            var id = (GrainId)ci[0];
            for (var i = 0; i < leafIds.Length; i++)
                if (leafIds[i] == id) return leaves[i];
            return leaves[0];
        });
    }

    private static Dictionary<string, LwwValue<byte[]>> Entries(params string[] keys)
    {
        var result = new Dictionary<string, LwwValue<byte[]>>(keys.Length);
        var wall = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        for (var i = 0; i < keys.Length; i++)
        {
            var hlc = new HybridLogicalClock { WallClockTicks = wall, Counter = i };
            result[keys[i]] = LwwValue<byte[]>.Create([(byte)i], hlc);
        }
        return result;
    }

    /// <summary>Seeds a coordinator state that is already mid-fold at <paramref name="phase"/>.</summary>
    private static FakePersistentState<TreeShardConsolidationState> InFlightState(
        ShardConsolidationPhase phase, int donor = 1, int survivor = 0, ShardMap? map = null)
        => new()
        {
            State = new TreeShardConsolidationState
            {
                InProgress = true,
                Phase = phase,
                OperationId = "op-1",
                DonorShardIndex = donor,
                SurvivorShardIndex = survivor,
                DonorSlots = [1, 3, 5, 7, 9, 11, 13, 15],
                OriginalShardMap = map ?? ShardMap.CreateDefault(VirtualShardCount, 2),
            },
        };

    // --- StartAsync validation ---

    [Test]
    public void StartAsync_throws_on_negative_survivor_index()
    {
        var h = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => h.Grain.StartAsync(-1));
    }

    [Test]
    public void StartAsync_throws_when_survivor_equals_donor()
    {
        var h = CreateGrain(donorShardIndex: 1);

        Assert.ThrowsAsync<ArgumentException>(() => h.Grain.StartAsync(1));
    }

    [Test]
    public async Task StartAsync_is_idempotent_for_an_in_flight_fold_to_the_same_survivor()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Drain));

        await h.Grain.StartAsync(0);

        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain),
            "A duplicate start must not restart or disturb the in-flight fold.");
        Assert.That(h.Log.Entries, Is.Empty);
    }

    [Test]
    public void StartAsync_refuses_to_re_aim_an_in_flight_fold()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Drain, survivor: 0));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(4));
        Assert.That(ex!.Message, Does.Contain("already in progress"));
    }

    [Test]
    public async Task StartAsync_is_a_no_op_when_the_donor_already_owns_no_slot()
    {
        // Every slot already routes to shard 0, so donor 1 is already retired.
        var map = new ShardMap { Slots = new int[VirtualShardCount] };
        var h = CreateGrain(donorShardIndex: 1, existingMap: map);

        await h.Grain.StartAsync(0);

        Assert.That(h.State.State.InProgress, Is.False,
            "Consolidating an already-consolidated pair must be a clean no-op, not a fault.");
        Assert.That(await h.Grain.IsIdleAsync(), Is.True);
        await h.Donor.DidNotReceive().BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public void StartAsync_refuses_a_non_adjacent_survivor()
    {
        // Three shards; donor 2 and survivor 0 are separated by shard 1.
        var slots = new int[VirtualShardCount];
        for (var i = 0; i < slots.Length; i++) slots[i] = i % 3;
        var h = CreateGrain(donorShardIndex: 2, existingMap: new ShardMap { Slots = slots });

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(0));
        Assert.That(ex!.Message, Does.Contain("not adjacent"));
    }

    [Test]
    public void StartAsync_refuses_while_an_adaptive_split_runs_on_the_donor()
    {
        var h = CreateGrain();
        h.Donor.IsSplittingAsync().Returns(Task.FromResult(true));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(0));
        Assert.That(ex!.Message, Does.Contain("adaptive split"));
    }

    [Test]
    public void StartAsync_refuses_while_an_adaptive_split_runs_on_the_survivor()
    {
        var h = CreateGrain();
        h.Survivor.IsSplittingAsync().Returns(Task.FromResult(true));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(0));
        Assert.That(ex!.Message, Does.Contain("survivor"));
    }

    // --- Intent and shadow-write window ---

    [Test]
    public async Task StartAsync_persists_intent_and_opens_the_donor_shadow_write_window()
    {
        var h = CreateGrain(donorShardIndex: 1, survivorShardIndex: 0);

        await h.Grain.StartAsync(0);

        Assert.That(h.State.State.InProgress, Is.True);
        Assert.That(h.State.State.DonorShardIndex, Is.EqualTo(1));
        Assert.That(h.State.State.SurvivorShardIndex, Is.EqualTo(0));
        Assert.That(h.State.State.OperationId, Is.Not.Null.And.Not.Empty);
        Assert.That(h.State.State.OriginalShardMap, Is.Not.Null);
        Assert.That(h.State.State.DonorSlots, Is.EqualTo(new[] { 1, 3, 5, 7, 9, 11, 13, 15 }).AsCollection,
            "The default 16/2 identity map routes every odd virtual slot to shard 1.");
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain));

        await h.Donor.Received(1).BeginSplitAsync(0, Arg.Any<int[]>(), VirtualShardCount);
    }

    [Test]
    public async Task StartAsync_does_not_touch_the_routing_map()
    {
        var h = CreateGrain();
        var mapBefore = h.PersistedMap!.Slots;

        await h.Grain.StartAsync(0);

        Assert.That(h.PersistedMap!.Slots, Is.EqualTo(mapBefore).AsCollection,
            "Opening the shadow window must leave routing untouched so the tree stays fully online.");
        await h.Registry.DidNotReceive().SetShardMapAsync(Arg.Any<string>(), Arg.Any<ShardMap>());
    }

    [Test]
    public async Task ReopenShadowWrite_re_issues_the_window_after_a_crash_and_advances_to_drain()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.BeginShadowWrite));

        await h.Grain.ReopenShadowWriteAsync();

        await h.Donor.Received(1).BeginSplitAsync(0, Arg.Any<int[]>(), VirtualShardCount);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain));
    }

    // --- Drain ---

    [Test]
    public async Task Drain_forwards_every_donor_entry_to_the_survivor_and_advances_to_swap()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a", "b"), Entries("c")]);

        var complete = await h.Grain.DrainAsync();

        Assert.That(complete, Is.True);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Swap));
        Assert.That(h.State.State.EntriesDrained, Is.EqualTo(3));
        Assert.That(h.State.State.LeavesScanned, Is.EqualTo(2));
        await h.Survivor.Received().MergeManyAsync(
            Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), isCrossShardMigration: true);
    }

    [Test]
    public async Task Drain_completes_immediately_for_a_donor_with_no_leaves()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Drain));

        var complete = await h.Grain.DrainAsync();

        Assert.That(complete, Is.True);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Swap));
        await h.Survivor.DidNotReceive().MergeManyAsync(
            Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
    }

    [Test]
    public async Task Drain_is_bounded_by_the_configured_leaves_per_pass()
    {
        var options = new LatticeOptions { ConsolidationDrainLeavesPerPass = 2 };
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a"), Entries("b"), Entries("c"), Entries("d"), Entries("e")],
            options: options);

        var firstPass = await h.Grain.DrainAsync();

        Assert.That(firstPass, Is.False, "A bounded pass must yield rather than sweep the whole donor.");
        Assert.That(h.State.State.LeavesScanned, Is.EqualTo(2));
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain),
            "The fold stays in Drain until the sweep is exhausted.");
        Assert.That(h.State.State.DrainCursorLeafId, Is.Not.Null,
            "A yielded pass must persist where to resume so an interruption never restarts the sweep.");
    }

    [Test]
    public async Task Drain_resumes_from_the_persisted_cursor_across_passes()
    {
        var options = new LatticeOptions { ConsolidationDrainLeavesPerPass = 2 };
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a"), Entries("b"), Entries("c"), Entries("d"), Entries("e")],
            options: options);

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();
        var third = await h.Grain.DrainAsync();

        Assert.That(third, Is.True);
        Assert.That(h.State.State.LeavesScanned, Is.EqualTo(5),
            "Every leaf must be visited exactly once across the resumed passes.");
        Assert.That(h.State.State.EntriesDrained, Is.EqualTo(5));
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Swap));
    }

    [Test]
    public async Task Drain_flushes_in_batches_bounded_by_the_configured_batch_size()
    {
        var options = new LatticeOptions { ConsolidationDrainBatchSize = 2 };
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a", "b", "c", "d", "e")],
            options: options);

        await h.Grain.DrainAsync();

        var flushes = h.Log.Entries.Count(e => e == "survivor.MergeMany");
        Assert.That(flushes, Is.EqualTo(3),
            "Five entries at a batch size of two must flush as 2 + 2 + 1.");
        Assert.That(h.State.State.EntriesDrained, Is.EqualTo(5));
    }

    [Test]
    public async Task Drain_falls_back_to_the_default_batch_size_when_configured_non_positive()
    {
        var options = new LatticeOptions { ConsolidationDrainBatchSize = 0 };
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a", "b", "c")],
            options: options);

        await h.Grain.DrainAsync();

        Assert.That(h.State.State.EntriesDrained, Is.EqualTo(3));
        Assert.That(h.Log.Entries.Count(e => e == "survivor.MergeMany"), Is.EqualTo(1));
    }

    [Test]
    public async Task Drain_preserves_tombstones_and_causality_metadata_verbatim()
    {
        // The drain moves whole LwwValue records, not values, so tombstone
        // flags, expiry ticks and HLC causality ride along untouched. That is
        // what makes a fold invisible to CRDT convergence.
        var hlc = new HybridLogicalClock { WallClockTicks = 12345, Counter = 7 };
        var tombstone = LwwValue<byte[]>.Tombstone(hlc);
        var expiring = LwwValue<byte[]>.Create([9], hlc) with { ExpiresAtTicks = 999_999 };
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["dead"] = tombstone,
            ["ttl"] = expiring,
        };

        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [entries]);

        Dictionary<string, LwwValue<byte[]>>? forwarded = null;
        h.Survivor.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>())
            .Returns(ci =>
            {
                forwarded = new Dictionary<string, LwwValue<byte[]>>((Dictionary<string, LwwValue<byte[]>>)ci[0]);
                return Task.CompletedTask;
            });

        await h.Grain.DrainAsync();

        Assert.That(forwarded, Is.Not.Null);
        Assert.That(forwarded!["dead"].IsTombstone, Is.True);
        Assert.That(forwarded["dead"].Timestamp, Is.EqualTo(hlc));
        Assert.That(forwarded["ttl"].ExpiresAtTicks, Is.EqualTo(999_999));
        Assert.That(forwarded["ttl"].Timestamp, Is.EqualTo(hlc));
    }

    // --- Swap: the ordering invariants ---

    [Test]
    public async Task Swap_freezes_the_donor_before_the_routing_map_flips()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Swap),
            leafEntries: [Entries("a")]);

        await h.Grain.SwapAsync();

        Assert.That(h.Log.IndexOf("donor.MarkLeavesMovedAway"), Is.LessThan(h.Log.IndexOf("donor.EnterReject")));
        Assert.That(h.Log.IndexOf("donor.EnterReject"), Is.LessThan(h.Log.IndexOf("registry.SetShardMap")),
            "Flipping first would let a stale-routing reader serve a value the survivor has superseded.");
    }

    [Test]
    public async Task Swap_drains_after_the_freeze_so_the_survivor_copy_is_authoritative()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Swap),
            leafEntries: [Entries("a")]);

        await h.Grain.SwapAsync();

        Assert.That(h.Log.IndexOf("donor.EnterReject"), Is.LessThan(h.Log.IndexOf("survivor.MergeMany")),
            "The hot-path shadow-forward is best-effort under LWW; only a post-freeze sweep "
            + "turns 'eventually equal' into 'equal now'.");
    }

    [Test]
    public async Task Swap_reclaims_on_the_survivor_after_the_final_drain_and_before_the_flip()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Swap),
            leafEntries: [Entries("a")]);

        await h.Grain.SwapAsync();

        Assert.That(h.Log.IndexOf("survivor.MergeMany"), Is.LessThan(h.Log.IndexOf("survivor.ReclaimSlots")),
            "Unsealing before the copy is authoritative would expose a partially drained survivor.");
        Assert.That(h.Log.IndexOf("survivor.ReclaimSlots"), Is.LessThan(h.Log.IndexOf("registry.SetShardMap")),
            "Flipping onto a still-sealed survivor makes every folded key permanently unreachable.");
    }

    [Test]
    public async Task Swap_repoints_every_donor_slot_onto_the_survivor()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Swap));

        await h.Grain.SwapAsync();

        Assert.That(h.PersistedMap, Is.Not.Null);
        for (var slot = 0; slot < VirtualShardCount; slot++)
        {
            Assert.That(h.PersistedMap!.Slots[slot], Is.EqualTo(0),
                $"Virtual slot {slot} must route to the survivor after the fold.");
        }
        Assert.That(h.PersistedMap!.GetPhysicalShardIndices(), Has.Count.EqualTo(1),
            "The whole point of a fold is that the tree's physical shard count comes down.");
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Reject));
    }

    [Test]
    public async Task Swap_composes_with_a_concurrent_map_change_rather_than_clobbering_it()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Swap));

        // A concurrent split moved slot 0 to a brand-new shard 4 after this
        // fold was planned. The fold must apply only its own slot diff.
        var concurrent = (int[])h.PersistedMap!.Slots.Clone();
        concurrent[0] = 4;
        h.PersistedMap = new ShardMap { Slots = concurrent };

        await h.Grain.SwapAsync();

        Assert.That(h.PersistedMap!.Slots[0], Is.EqualTo(4),
            "A fold must not clobber a slot it does not own.");
        Assert.That(h.PersistedMap!.Slots[1], Is.EqualTo(0));
    }

    [Test]
    public async Task Swap_never_leaves_a_virtual_slot_unrouted()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Swap));

        await h.Grain.SwapAsync();

        Assert.That(h.PersistedMap!.Slots.Length, Is.EqualTo(VirtualShardCount));
        Assert.That(h.PersistedMap!.Slots, Has.All.GreaterThanOrEqualTo(0),
            "No key may be unreachable at any instant, so every virtual slot always routes somewhere.");
    }

    // --- Reject and finalise ---

    [Test]
    public async Task EnterReject_re_asserts_the_freeze_and_advances_to_complete()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Reject));

        await h.Grain.EnterRejectAsync();

        await h.Donor.Received(1).EnterRejectPhaseAsync();
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Complete));
    }

    [Test]
    public async Task Finalise_retires_the_donor_and_clears_the_coordinator()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Complete),
            leafEntries: [Entries("a")]);

        await h.Grain.FinaliseAsync();

        await h.Donor.Received(1).CompleteSplitAsync();
        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.State.State.Complete, Is.True);
        Assert.That(h.State.State.Cancelled, Is.False);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.None));
        Assert.That(await h.Grain.IsIdleAsync(), Is.True);
    }

    [Test]
    public async Task Finalise_runs_a_last_drain_before_retiring_the_donor()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Complete),
            leafEntries: [Entries("late-tombstone")]);

        await h.Grain.FinaliseAsync();

        Assert.That(h.Log.IndexOf("survivor.MergeMany"), Is.LessThan(h.Log.IndexOf("donor.CompleteSplit")),
            "A delete landing during the freeze window must reach the survivor before the donor retires.");
    }

    // --- End-to-end phase progression ---

    [Test]
    public async Task RunConsolidationPass_drives_a_fold_from_start_to_completion()
    {
        var h = CreateGrain(leafEntries: [Entries("a", "b")]);

        await h.Grain.StartAsync(0);
        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Complete, Is.True);
        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.PersistedMap!.GetPhysicalShardIndices(), Has.Count.EqualTo(1));
        await h.Donor.Received(1).CompleteSplitAsync();
    }

    [Test]
    public async Task RunConsolidationPass_is_a_no_op_when_nothing_is_in_flight()
    {
        var h = CreateGrain();

        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.Log.Entries, Is.Empty);
    }

    [Test]
    public async Task RunConsolidationPass_yields_while_a_bounded_drain_is_still_sweeping()
    {
        var options = new LatticeOptions { ConsolidationDrainLeavesPerPass = 1 };
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a"), Entries("b"), Entries("c"), Entries("d"),
                          Entries("e"), Entries("f"), Entries("g"), Entries("h"),
                          Entries("i"), Entries("j")],
            options: options);

        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain),
            "A synchronous driver must still yield between bounded passes on a large donor.");
        Assert.That(h.State.State.InProgress, Is.True);
    }

    // --- Progress reporting ---

    [Test]
    public async Task GetProgress_reports_idle_before_any_fold_has_run()
    {
        var h = CreateGrain();

        var progress = await h.Grain.GetProgressAsync();

        Assert.That(progress.InProgress, Is.False);
        Assert.That(progress.Complete, Is.False);
        Assert.That(progress.Cancelled, Is.False);
        Assert.That(progress.Phase, Is.EqualTo(ShardConsolidationPhase.None));
        Assert.That(progress.OperationId, Is.Null);
    }

    [Test]
    public async Task GetProgress_reports_the_in_flight_fold_and_its_counters()
    {
        var h = CreateGrain(leafEntries: [Entries("a", "b"), Entries("c")]);
        await h.Grain.StartAsync(0);
        await h.Grain.DrainAsync();

        var progress = await h.Grain.GetProgressAsync();

        Assert.That(progress.InProgress, Is.True);
        Assert.That(progress.DonorShardIndex, Is.EqualTo(1));
        Assert.That(progress.SurvivorShardIndex, Is.EqualTo(0));
        Assert.That(progress.SlotsToFold, Is.EqualTo(8));
        Assert.That(progress.EntriesDrained, Is.EqualTo(3));
        Assert.That(progress.LeavesScanned, Is.EqualTo(2));
        Assert.That(progress.OperationId, Is.Not.Null.And.Not.Empty);
        Assert.That(progress.UpdatedAtTicks, Is.GreaterThan(0));
    }

    [Test]
    public async Task GetProgress_advances_its_timestamp_as_the_fold_makes_progress()
    {
        var h = CreateGrain(leafEntries: [Entries("a")]);
        await h.Grain.StartAsync(0);
        var first = await h.Grain.GetProgressAsync();

        await h.Grain.DrainAsync();
        var second = await h.Grain.GetProgressAsync();

        Assert.That(second.UpdatedAtTicks, Is.GreaterThan(first.UpdatedAtTicks),
            "A driver watching for a stalled fold needs the timestamp to move on real progress.");
        Assert.That(second.StartedAtTicks, Is.EqualTo(first.StartedAtTicks));
    }

    [Test]
    public async Task GetProgress_reports_completion_after_the_fold_lands()
    {
        var h = CreateGrain(leafEntries: [Entries("a")]);
        await h.Grain.StartAsync(0);
        await h.Grain.RunConsolidationPassAsync();

        var progress = await h.Grain.GetProgressAsync();

        Assert.That(progress.InProgress, Is.False);
        Assert.That(progress.Complete, Is.True);
        Assert.That(progress.Cancelled, Is.False);
        Assert.That(progress.Phase, Is.EqualTo(ShardConsolidationPhase.None));
    }

    // --- Cancellation ---

    [Test]
    public async Task Cancel_is_refused_when_nothing_is_in_flight()
    {
        var h = CreateGrain();

        Assert.That(await h.Grain.CancelAsync(), Is.False);
    }

    [Test]
    public Task Cancel_is_accepted_during_the_shadow_write_phase()
        => AssertCancelAcceptedAsync(ShardConsolidationPhase.BeginShadowWrite);

    [Test]
    public Task Cancel_is_accepted_during_the_drain_phase()
        => AssertCancelAcceptedAsync(ShardConsolidationPhase.Drain);

    private static async Task AssertCancelAcceptedAsync(ShardConsolidationPhase phase)
    {
        var h = CreateGrain(existingState: InFlightState(phase));

        Assert.That(await h.Grain.CancelAsync(), Is.True);
        Assert.That(h.State.State.CancelRequested, Is.True);
    }

    [Test]
    public Task Cancel_is_not_honoured_during_the_swap_phase()
        => AssertCancelRecordedButRefusedAsync(ShardConsolidationPhase.Swap);

    [Test]
    public Task Cancel_is_not_honoured_during_the_reject_phase()
        => AssertCancelRecordedButRefusedAsync(ShardConsolidationPhase.Reject);

    [Test]
    public Task Cancel_is_not_honoured_during_the_complete_phase()
        => AssertCancelRecordedButRefusedAsync(ShardConsolidationPhase.Complete);

    private static async Task AssertCancelRecordedButRefusedAsync(ShardConsolidationPhase phase)
    {
        var h = CreateGrain(existingState: InFlightState(phase));

        Assert.That(await h.Grain.CancelAsync(), Is.False,
            "Abandoning after the flip would strand the donor mid-retirement.");
        Assert.That(h.State.State.CancelRequested, Is.True,
            "The request is still recorded so a poll shows it was received.");
    }

    [Test]
    public async Task Cancel_during_drain_abandons_the_fold_and_restores_the_donor()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a")]);

        await h.Grain.CancelAsync();
        await h.Grain.RunConsolidationPassAsync();

        await h.Donor.Received(1).AbortSplitAsync();
        await h.Donor.DidNotReceive().CompleteSplitAsync();
        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.State.State.Cancelled, Is.True);
        Assert.That(h.State.State.Complete, Is.False);
        await h.Registry.DidNotReceive().SetShardMapAsync(Arg.Any<string>(), Arg.Any<ShardMap>());
    }

    [Test]
    public async Task Cancel_leaves_the_routing_map_exactly_as_it_was()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Drain));
        var before = (int[])h.PersistedMap!.Slots.Clone();

        await h.Grain.CancelAsync();
        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.PersistedMap!.Slots, Is.EqualTo(before).AsCollection);
    }

    [Test]
    public async Task Cancel_past_the_swap_still_lets_the_fold_run_to_completion()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Reject),
            leafEntries: [Entries("a")]);

        await h.Grain.CancelAsync();
        await h.Grain.RunConsolidationPassAsync();

        await h.Donor.Received(1).CompleteSplitAsync();
        await h.Donor.DidNotReceive().AbortSplitAsync();
        Assert.That(h.State.State.Complete, Is.True,
            "A fold past the flip must finish rather than strand the donor half-retired.");
    }

    [Test]
    public async Task Cancel_is_idempotent()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Drain));

        await h.Grain.CancelAsync();
        var writesAfterFirst = h.State.WriteCount;
        var second = await h.Grain.CancelAsync();

        Assert.That(second, Is.True);
        Assert.That(h.State.WriteCount, Is.EqualTo(writesAfterFirst),
            "A repeated cancel must not pay a second storage write.");
    }

    // --- Idempotence of a completed fold ---

    [Test]
    public async Task A_completed_fold_can_be_re_requested_as_a_no_op()
    {
        var h = CreateGrain(leafEntries: [Entries("a")]);
        await h.Grain.StartAsync(0);
        await h.Grain.RunConsolidationPassAsync();
        var mapAfterFold = (int[])h.PersistedMap!.Slots.Clone();

        // The map now routes everything to the survivor, so the pair is
        // already consolidated and a re-request must do nothing at all.
        await h.Grain.StartAsync(0);

        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.PersistedMap!.Slots, Is.EqualTo(mapAfterFold).AsCollection);
        await h.Donor.Received(1).CompleteSplitAsync();
    }
}
