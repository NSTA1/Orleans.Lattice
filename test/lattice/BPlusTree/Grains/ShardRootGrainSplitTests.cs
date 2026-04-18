using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the F-011 adaptive-split surface on <see cref="ShardRootGrain"/>:
/// lifecycle methods, reject-phase routing, and shadow-write forwarding.
/// </summary>
public class ShardRootGrainSplitTests
{
    private const string TreeId = "test-tree";
    private const int VirtualShardCount = 16;

    /// <summary>
    /// Picks a virtual slot owned by physical shard 0 under a default 16-slot identity map
    /// (slots 0,2,4,...) by exhaustive search for a key that hashes there.
    /// </summary>
    private static string KeyForVirtualSlot(int targetSlot)
    {
        for (int i = 0; i < 100_000; i++)
        {
            var k = $"k{i}";
            if (ShardMap.GetVirtualSlot(k, VirtualShardCount) == targetSlot)
                return k;
        }
        throw new InvalidOperationException($"Could not find a key for virtual slot {targetSlot}.");
    }

    private sealed class GrainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required IShardRootGrain ShadowTarget { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required IGrainFactory Factory { get; init; }
    }

    private static GrainHarness CreateHarness(
        string shardKey = TreeId + "/0",
        FakePersistentState<ShardRootState>? state = null,
        byte[]? readbackValue = null,
        HybridLogicalClock readbackVersion = default,
        bool getOrSetReturnsExisting = false,
        bool setIfVersionSucceeds = true)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", shardKey));

        state ??= new FakePersistentState<ShardRootState>();
        state.State.RootNodeId ??= GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var leaf = Substitute.For<IBPlusLeafGrain>();
        var versionedReadback = readbackValue is null
            ? new VersionedValue { Value = null, Version = HybridLogicalClock.Zero }
            : new VersionedValue { Value = readbackValue, Version = readbackVersion };
        leaf.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        leaf.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(false));
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.DeleteAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        leaf.GetWithVersionAsync(Arg.Any<string>()).Returns(Task.FromResult(versionedReadback));
        leaf.GetOrSetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult(
            getOrSetReturnsExisting
                ? new GetOrSetResult { ExistingValue = [9, 9], Split = null }
                : new GetOrSetResult { ExistingValue = null, Split = null }));
        leaf.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>())
            .Returns(Task.FromResult(new CasResult { Success = setIfVersionSucceeds, Split = null }));
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        cache.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(false));
        cache.GetManyAsync(Arg.Any<List<string>>()).Returns(Task.FromResult(new Dictionary<string, byte[]>()));
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var shadowTarget = Substitute.For<IShardRootGrain>();
        shadowTarget.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>()).Returns(Task.CompletedTask);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        var grain = new ShardRootGrain(context, state, factory, optionsMonitor);
        return new GrainHarness { Grain = grain, Leaf = leaf, ShadowTarget = shadowTarget, State = state, Factory = factory };
    }

    private static int[] AllSlotsBelongingToShardZero()
    {
        // Default identity map: shard i owns slots where slot % physicalShardCount == i.
        // We use a 2-shard logical layout, so shard 0 owns even slots.
        var list = new List<int>();
        for (int i = 0; i < VirtualShardCount; i++)
            if (i % 2 == 0) list.Add(i);
        return [.. list];
    }

    // ============================================================================
    // BeginSplitAsync — input validation + idempotency
    // ============================================================================

    [Test]
    public void BeginSplitAsync_throws_when_movedSlots_is_null()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginSplitAsync(1, null!, VirtualShardCount),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void BeginSplitAsync_throws_when_movedSlots_is_empty()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginSplitAsync(1, [], VirtualShardCount),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BeginSplitAsync_throws_when_target_equals_self()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginSplitAsync(0, [0, 2], VirtualShardCount),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BeginSplitAsync_throws_when_virtualShardCount_is_zero_or_negative()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginSplitAsync(1, [0], 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That(async () => await h.Grain.BeginSplitAsync(1, [0], -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void BeginSplitAsync_throws_when_slot_out_of_range()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginSplitAsync(1, [VirtualShardCount], VirtualShardCount),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That(async () => await h.Grain.BeginSplitAsync(1, [-1], VirtualShardCount),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task BeginSplitAsync_persists_state_with_BeginShadowWrite_phase()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(1, [4, 2, 0], VirtualShardCount);

        var sip = h.State.State.SplitInProgress;
        Assert.That(sip, Is.Not.Null);
        Assert.That(sip!.Phase, Is.EqualTo(ShardSplitPhase.BeginShadowWrite));
        Assert.That(sip.ShadowTargetShardIndex, Is.EqualTo(1));
        Assert.That(sip.VirtualShardCount, Is.EqualTo(VirtualShardCount));
        // Slots are stored sorted for binary-search hot-path.
        Assert.That(sip.MovedSlots, Is.EqualTo(new[] { 0, 2, 4 }));
    }

    [Test]
    public async Task BeginSplitAsync_is_idempotent_for_matching_params()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(1, [0, 2], VirtualShardCount);
        var writesAfterFirst = h.State.WriteCount;

        await h.Grain.BeginSplitAsync(1, [2, 0], VirtualShardCount);
        Assert.That(h.State.WriteCount, Is.EqualTo(writesAfterFirst), "Idempotent re-entry must not write state.");
    }

    [Test]
    public async Task BeginSplitAsync_overwrites_when_phase_advanced_to_Drain()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(1, [0, 2], VirtualShardCount);
        // Manually advance to Drain to simulate coordinator progress.
        h.State.State.SplitInProgress = h.State.State.SplitInProgress! with { Phase = ShardSplitPhase.Drain };

        // A second BeginSplitAsync with matching params is still idempotent in Drain phase.
        var writes = h.State.WriteCount;
        await h.Grain.BeginSplitAsync(1, [0, 2], VirtualShardCount);
        Assert.That(h.State.WriteCount, Is.EqualTo(writes));
    }

    // ============================================================================
    // EnterRejectPhaseAsync / CompleteSplitAsync / IsSplittingAsync
    // ============================================================================

    [Test]
    public async Task EnterRejectPhaseAsync_transitions_to_Reject()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(1, [0], VirtualShardCount);
        await h.Grain.EnterRejectPhaseAsync();

        Assert.That(h.State.State.SplitInProgress!.Phase, Is.EqualTo(ShardSplitPhase.Reject));
    }

    [Test]
    public async Task EnterRejectPhaseAsync_is_idempotent_when_already_in_Reject()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(1, [0], VirtualShardCount);
        await h.Grain.EnterRejectPhaseAsync();
        var writes = h.State.WriteCount;

        await h.Grain.EnterRejectPhaseAsync();
        Assert.That(h.State.WriteCount, Is.EqualTo(writes));
    }

    [Test]
    public async Task EnterRejectPhaseAsync_is_noop_when_no_split_active()
    {
        var h = CreateHarness();
        await h.Grain.EnterRejectPhaseAsync();
        Assert.That(h.State.State.SplitInProgress, Is.Null);
    }

    [Test]
    public async Task CompleteSplitAsync_clears_state()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(1, [0], VirtualShardCount);
        await h.Grain.CompleteSplitAsync();

        Assert.That(h.State.State.SplitInProgress, Is.Null);
    }

    [Test]
    public async Task CompleteSplitAsync_is_noop_when_no_split_active()
    {
        var h = CreateHarness();
        await h.Grain.CompleteSplitAsync();
        Assert.That(h.State.State.SplitInProgress, Is.Null);
    }

    // ============================================================================
    // F-011 MovedAwaySlots — permanent rejection after Complete (stale-cache safety)
    // ============================================================================

    [Test]
    public async Task CompleteSplitAsync_promotes_moved_slots_into_MovedAwaySlots()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(targetShardIndex: 1, movedSlots: [2, 4, 6], VirtualShardCount);
        await h.Grain.CompleteSplitAsync();

        Assert.That(h.State.State.SplitInProgress, Is.Null);
        Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount));
        Assert.That(h.State.State.MovedAwaySlots, Is.EquivalentTo(new Dictionary<int, int>
        {
            [2] = 1,
            [4] = 1,
            [6] = 1,
        }));
    }

    [Test]
    public async Task CompleteSplitAsync_accumulates_MovedAwaySlots_across_multiple_splits()
    {
        var h = CreateHarness();
        await h.Grain.BeginSplitAsync(targetShardIndex: 1, movedSlots: [2, 4], VirtualShardCount);
        await h.Grain.CompleteSplitAsync();
        await h.Grain.BeginSplitAsync(targetShardIndex: 3, movedSlots: [6, 8], VirtualShardCount);
        await h.Grain.CompleteSplitAsync();

        Assert.That(h.State.State.MovedAwaySlots, Is.EquivalentTo(new Dictionary<int, int>
        {
            [2] = 1,
            [4] = 1,
            [6] = 3,
            [8] = 3,
        }));
    }

    [Test]
    public async Task GetAsync_throws_StaleShardRouting_for_key_in_MovedAwaySlots_after_complete()
    {
        var h = CreateHarness();
        var movedSlot = ShardMap.GetVirtualSlot("x", VirtualShardCount);
        await h.Grain.BeginSplitAsync(targetShardIndex: 7, movedSlots: [movedSlot], VirtualShardCount);
        await h.Grain.CompleteSplitAsync();

        // Sanity — split state cleared but slot is permanently rejected.
        Assert.That(h.State.State.SplitInProgress, Is.Null);
        var ex = Assert.ThrowsAsync<StaleShardRoutingException>(async () => await h.Grain.GetAsync("x"));
        Assert.That(ex!.TargetShardIndex, Is.EqualTo(7));
        Assert.That(ex.VirtualSlot, Is.EqualTo(movedSlot));
    }

    [Test]
    public async Task SetAsync_throws_StaleShardRouting_for_key_in_MovedAwaySlots_after_complete()
    {
        var h = CreateHarness();
        var movedSlot = ShardMap.GetVirtualSlot("y", VirtualShardCount);
        await h.Grain.BeginSplitAsync(targetShardIndex: 9, movedSlots: [movedSlot], VirtualShardCount);
        await h.Grain.CompleteSplitAsync();

        var ex = Assert.ThrowsAsync<StaleShardRoutingException>(async () => await h.Grain.SetAsync("y", [1, 2, 3]));
        Assert.That(ex!.TargetShardIndex, Is.EqualTo(9));
    }

    [Test]
    public async Task GetAsync_succeeds_for_key_NOT_in_MovedAwaySlots_after_complete()
    {
        var h = CreateHarness();
        // Move slot A; query a key whose slot is different.
        var slotA = ShardMap.GetVirtualSlot("a", VirtualShardCount);
        // Find a key whose slot != slotA.
        string? otherKey = null;
        for (int i = 0; i < 1000; i++)
        {
            var candidate = $"other-{i}";
            if (ShardMap.GetVirtualSlot(candidate, VirtualShardCount) != slotA)
            {
                otherKey = candidate;
                break;
            }
        }
        Assert.That(otherKey, Is.Not.Null);

        await h.Grain.BeginSplitAsync(targetShardIndex: 1, movedSlots: [slotA], VirtualShardCount);
        await h.Grain.CompleteSplitAsync();

        // Must not throw — the key's slot is not moved-away.
        Assert.DoesNotThrowAsync(async () => await h.Grain.GetAsync(otherKey!));
    }

    [Test]
    public async Task IsSlotMovedAway_returns_true_during_Swap_phase_for_moved_slot()
    {
        var h = CreateHarness();
        var slot = ShardMap.GetVirtualSlot("k", VirtualShardCount);
        await h.Grain.BeginSplitAsync(targetShardIndex: 1, movedSlots: [slot], VirtualShardCount);

        // Force phase to Swap.
        var sip = h.State.State.SplitInProgress!;
        h.State.State.SplitInProgress = sip with { Phase = ShardSplitPhase.Swap };

        Assert.That(h.Grain.IsSlotMovedAway("k"), Is.True);
    }

    [Test]
    public async Task IsSlotMovedAway_returns_false_during_BeginShadowWrite_phase()
    {
        var h = CreateHarness();
        var slot = ShardMap.GetVirtualSlot("k", VirtualShardCount);
        await h.Grain.BeginSplitAsync(targetShardIndex: 1, movedSlots: [slot], VirtualShardCount);

        // Phase is BeginShadowWrite — S still authoritative; scans must include it.
        Assert.That(h.State.State.SplitInProgress!.Phase, Is.EqualTo(ShardSplitPhase.BeginShadowWrite));
        Assert.That(h.Grain.IsSlotMovedAway("k"), Is.False);
    }

    [Test]
    public void IsSlotMovedAway_returns_false_when_no_split_ever_happened()
    {
        var h = CreateHarness();
        Assert.That(h.Grain.IsSlotMovedAway("anything"), Is.False);
    }
}
