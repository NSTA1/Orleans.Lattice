using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the strongly-consistent scan surface added to
/// <see cref="ShardRootGrain"/>: <see cref="IShardRootGrain.CountWithMovedAwayAsync"/>,
/// <see cref="IShardRootGrain.CountForSlotsAsync"/>, the slot-filtered scan
/// methods, and the <c>MovedAwaySlots</c> reporting on the existing scan
/// methods.
/// </summary>
[TestFixture]
public class ShardRootGrainConsistentScanTests
{
    private const string TreeId = "scan-tree";
    private const string ShardKey = TreeId + "/0";
    private const int VirtualShardCount = 16;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static Harness CreateHarness(
        IReadOnlyList<string>? leafKeys = null,
        IReadOnlyList<KeyValuePair<string, byte[]>>? leafEntries = null,
        ShardSplitInProgress? splitInProgress = null,
        Dictionary<int, int>? movedAwaySlots = null,
        int? movedAwayVirtualShardCount = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "scan-leaf");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;
        state.State.SplitInProgress = splitInProgress;
        if (movedAwaySlots is not null)
            foreach (var (k, v) in movedAwaySlots) state.State.MovedAwaySlots[k] = v;
        if (movedAwayVirtualShardCount is not null)
            state.State.MovedAwayVirtualShardCount = movedAwayVirtualShardCount;

        var keys = leafKeys ?? leafEntries?.Select(e => e.Key).ToList() ?? new List<string>();
        var entries = leafEntries ?? keys.Select(k => new KeyValuePair<string, byte[]>(k, [1])).ToList();

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(keys.ToList()));
        leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(entries.ToList()));
        leaf.CountAsync().Returns(Task.FromResult(keys.Count));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        leaf.GetPrevSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            shardCount: 1,
            factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver, Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance),
            Leaf = leaf,
            State = state,
        };
    }

    /// <summary>Builds a small list of keys with deterministic virtual slots.</summary>
    private static List<string> KeysWithSlots(int count)
    {
        // We only need a stable batch — the actual slots vary by hash but are
        // deterministic for the tests below.
        return Enumerable.Range(0, count).Select(i => $"k{i:D4}").ToList();
    }

    // ============================================================================
    // CountWithMovedAwayAsync
    // ============================================================================

    [Test]
    public async Task CountWithMovedAwayAsync_returns_zero_count_and_null_slots_when_leaf_empty()
    {
        var h = CreateHarness(leafKeys: []);

        var result = await h.Grain.CountWithMovedAwayAsync();

        Assert.That(result.Count, Is.Zero);
        Assert.That(result.MovedAwaySlots, Is.Null);
    }

    [Test]
    public async Task CountWithMovedAwayAsync_returns_full_count_and_null_slots_when_no_split_active()
    {
        var keys = KeysWithSlots(50);
        var h = CreateHarness(leafKeys: keys);

        var result = await h.Grain.CountWithMovedAwayAsync();

        Assert.That(result.Count, Is.EqualTo(50));
        Assert.That(result.MovedAwaySlots, Is.Null,
            "No split active → no slot reporting → MovedAwaySlots must be null.");
    }

    [Test]
    public async Task CountWithMovedAwayAsync_filters_and_reports_slots_during_active_split_swap_phase()
    {
        var keys = KeysWithSlots(20);
        // Pick the slot of the first key as the moved slot so we are guaranteed
        // at least one filtered key.
        var movedSlot = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);

        var h = CreateHarness(
            leafKeys: keys,
            splitInProgress: new ShardSplitInProgress
            {
                Phase = ShardSplitPhase.Swap,
                ShadowTargetShardIndex = 1,
                MovedSlots = [movedSlot],
                VirtualShardCount = VirtualShardCount,
            });

        var result = await h.Grain.CountWithMovedAwayAsync();

        var expectedFiltered = keys.Count(k => ShardMap.GetVirtualSlot(k, VirtualShardCount) == movedSlot);
        Assert.That(result.Count, Is.EqualTo(20 - expectedFiltered));
        Assert.That(result.MovedAwaySlots, Is.EqualTo(new[] { movedSlot }));
    }

    [Test]
    public async Task CountWithMovedAwayAsync_does_not_filter_during_BeginShadowWrite_phase()
    {
        var keys = KeysWithSlots(20);
        var movedSlot = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);

        var h = CreateHarness(
            leafKeys: keys,
            splitInProgress: new ShardSplitInProgress
            {
                Phase = ShardSplitPhase.BeginShadowWrite,
                ShadowTargetShardIndex = 1,
                MovedSlots = [movedSlot],
                VirtualShardCount = VirtualShardCount,
            });

        var result = await h.Grain.CountWithMovedAwayAsync();

        // BeginShadowWrite/Drain phases keep authoritative ownership; no filtering.
        Assert.That(result.Count, Is.EqualTo(20));
        Assert.That(result.MovedAwaySlots, Is.Null);
    }

    [Test]
    public async Task CountWithMovedAwayAsync_filters_and_reports_permanent_moved_slots()
    {
        var keys = KeysWithSlots(20);
        var movedSlot = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);

        var h = CreateHarness(
            leafKeys: keys,
            movedAwaySlots: new() { [movedSlot] = 0 },
            movedAwayVirtualShardCount: VirtualShardCount);

        var result = await h.Grain.CountWithMovedAwayAsync();

        var expectedFiltered = keys.Count(k => ShardMap.GetVirtualSlot(k, VirtualShardCount) == movedSlot);
        Assert.That(result.Count, Is.EqualTo(20 - expectedFiltered));
        Assert.That(result.MovedAwaySlots, Is.EqualTo(new[] { movedSlot }));
    }

    // ============================================================================
    // CountForSlotsAsync
    // ============================================================================

    [Test]
    public void CountForSlotsAsync_throws_when_sortedSlots_null()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.CountForSlotsAsync(null!, VirtualShardCount),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void CountForSlotsAsync_throws_when_virtualShardCount_non_positive()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.CountForSlotsAsync([0], 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That(async () => await h.Grain.CountForSlotsAsync([0], -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task CountForSlotsAsync_returns_zero_when_leaf_empty()
    {
        var h = CreateHarness(leafKeys: []);

        var result = await h.Grain.CountForSlotsAsync([0, 1, 2], VirtualShardCount);

        Assert.That(result, Is.Zero);
    }

    [Test]
    public async Task CountForSlotsAsync_returns_zero_when_sortedSlots_empty()
    {
        var keys = KeysWithSlots(50);
        var h = CreateHarness(leafKeys: keys);

        var result = await h.Grain.CountForSlotsAsync([], VirtualShardCount);

        Assert.That(result, Is.Zero);
    }

    [Test]
    public async Task CountForSlotsAsync_counts_only_keys_in_requested_slots()
    {
        var keys = KeysWithSlots(50);
        var slot0 = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);
        var slot1 = ShardMap.GetVirtualSlot(keys[1], VirtualShardCount);

        var sorted = new[] { slot0, slot1 }.Distinct().OrderBy(x => x).ToArray();
        var h = CreateHarness(leafKeys: keys);

        var result = await h.Grain.CountForSlotsAsync(sorted, VirtualShardCount);

        var expected = keys.Count(k =>
        {
            var s = ShardMap.GetVirtualSlot(k, VirtualShardCount);
            return Array.BinarySearch(sorted, s) >= 0;
        });
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public async Task CountForSlotsAsync_does_not_apply_own_movedAwaySlots_filter()
    {
        // The slot-filtered method is for the orchestrator's pass-2; the caller
        // has chosen these slots based on the latest map and is responsible
        // for routing. Therefore the shard must not silently drop entries even
        // if its own MovedAwaySlots claims those slots have moved away.
        var keys = KeysWithSlots(20);
        var slot0 = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);

        var h = CreateHarness(
            leafKeys: keys,
            movedAwaySlots: new() { [slot0] = 0 },
            movedAwayVirtualShardCount: VirtualShardCount);

        var result = await h.Grain.CountForSlotsAsync([slot0], VirtualShardCount);

        var expected = keys.Count(k => ShardMap.GetVirtualSlot(k, VirtualShardCount) == slot0);
        Assert.That(result, Is.EqualTo(expected),
            "Slot-filtered count must include keys even when slot is in own MovedAwaySlots set.");
    }

    // ============================================================================
    // GetSortedKeysBatchAsync — MovedAwaySlots reporting
    // ============================================================================

    [Test]
    public async Task GetSortedKeysBatchAsync_reports_filtered_slots_in_MovedAwaySlots()
    {
        var keys = KeysWithSlots(10);
        var movedSlot = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);

        var h = CreateHarness(
            leafKeys: keys,
            splitInProgress: new ShardSplitInProgress
            {
                Phase = ShardSplitPhase.Swap,
                ShadowTargetShardIndex = 1,
                MovedSlots = [movedSlot],
                VirtualShardCount = VirtualShardCount,
            });

        var page = await h.Grain.GetSortedKeysBatchAsync(null, null, 100, null);

        Assert.That(page.MovedAwaySlots, Is.EqualTo(new[] { movedSlot }));
        Assert.That(page.Keys, Has.None.Matches<string>(
            k => ShardMap.GetVirtualSlot(k, VirtualShardCount) == movedSlot));
    }

    [Test]
    public async Task GetSortedKeysBatchAsync_returns_null_MovedAwaySlots_when_no_filtering()
    {
        var keys = KeysWithSlots(10);
        var h = CreateHarness(leafKeys: keys);

        var page = await h.Grain.GetSortedKeysBatchAsync(null, null, 100, null);

        Assert.That(page.MovedAwaySlots, Is.Null);
        Assert.That(page.Keys, Has.Count.EqualTo(10));
    }

    [Test]
    public async Task GetSortedEntriesBatchAsync_reports_filtered_slots_in_MovedAwaySlots()
    {
        var entries = KeysWithSlots(10).Select(k => new KeyValuePair<string, byte[]>(k, [1])).ToList();
        var movedSlot = ShardMap.GetVirtualSlot(entries[0].Key, VirtualShardCount);

        var h = CreateHarness(
            leafEntries: entries,
            splitInProgress: new ShardSplitInProgress
            {
                Phase = ShardSplitPhase.Swap,
                ShadowTargetShardIndex = 1,
                MovedSlots = [movedSlot],
                VirtualShardCount = VirtualShardCount,
            });

        var page = await h.Grain.GetSortedEntriesBatchAsync(null, null, 100, null);

        Assert.That(page.MovedAwaySlots, Is.EqualTo(new[] { movedSlot }));
        Assert.That(page.Entries, Has.None.Matches<KeyValuePair<string, byte[]>>(
            kv => ShardMap.GetVirtualSlot(kv.Key, VirtualShardCount) == movedSlot));
    }

    // ============================================================================
    // GetSortedKeysBatchForSlotsAsync / GetSortedEntriesBatchForSlotsAsync
    // ============================================================================

    [Test]
    public void GetSortedKeysBatchForSlotsAsync_throws_when_sortedSlots_null()
    {
        var h = CreateHarness();
        Assert.That(
            async () => await h.Grain.GetSortedKeysBatchForSlotsAsync(null, null, 10, null, null!, VirtualShardCount),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetSortedKeysBatchForSlotsAsync_throws_when_virtualShardCount_non_positive()
    {
        var h = CreateHarness();
        Assert.That(
            async () => await h.Grain.GetSortedKeysBatchForSlotsAsync(null, null, 10, null, [0], 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GetSortedKeysBatchForSlotsAsync_returns_empty_when_sortedSlots_empty()
    {
        var keys = KeysWithSlots(20);
        var h = CreateHarness(leafKeys: keys);

        var page = await h.Grain.GetSortedKeysBatchForSlotsAsync(null, null, 100, null, [], VirtualShardCount);

        Assert.That(page.Keys, Is.Empty);
        Assert.That(page.HasMore, Is.False);
        Assert.That(page.MovedAwaySlots, Is.Null);
    }

    [Test]
    public async Task GetSortedKeysBatchForSlotsAsync_returns_only_keys_in_requested_slots()
    {
        var keys = KeysWithSlots(50);
        var slot = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);
        var sorted = new[] { slot };

        var h = CreateHarness(leafKeys: keys);

        var page = await h.Grain.GetSortedKeysBatchForSlotsAsync(null, null, 100, null, sorted, VirtualShardCount);

        Assert.That(page.Keys, Is.All.Matches<string>(
            k => ShardMap.GetVirtualSlot(k, VirtualShardCount) == slot));
        Assert.That(page.MovedAwaySlots, Is.Null,
            "Slot-filtered scan does not produce MovedAwaySlots reporting.");
    }

    [Test]
    public async Task GetSortedKeysBatchForSlotsAsync_does_not_filter_own_movedAwaySlots()
    {
        var keys = KeysWithSlots(20);
        var slot = ShardMap.GetVirtualSlot(keys[0], VirtualShardCount);

        var h = CreateHarness(
            leafKeys: keys,
            movedAwaySlots: new() { [slot] = 0 },
            movedAwayVirtualShardCount: VirtualShardCount);

        var page = await h.Grain.GetSortedKeysBatchForSlotsAsync(null, null, 100, null, [slot], VirtualShardCount);

        var expected = keys.Where(k => ShardMap.GetVirtualSlot(k, VirtualShardCount) == slot).ToList();
        Assert.That(page.Keys, Is.EquivalentTo(expected),
            "Slot-filtered scan must surface entries even from own MovedAwaySlots set.");
    }

    [Test]
    public async Task GetSortedEntriesBatchForSlotsAsync_returns_only_entries_in_requested_slots()
    {
        var entries = KeysWithSlots(50).Select(k => new KeyValuePair<string, byte[]>(k, [1])).ToList();
        var slot = ShardMap.GetVirtualSlot(entries[0].Key, VirtualShardCount);
        var sorted = new[] { slot };

        var h = CreateHarness(leafEntries: entries);

        var page = await h.Grain.GetSortedEntriesBatchForSlotsAsync(null, null, 100, null, sorted, VirtualShardCount);

        Assert.That(page.Entries, Is.All.Matches<KeyValuePair<string, byte[]>>(
            kv => ShardMap.GetVirtualSlot(kv.Key, VirtualShardCount) == slot));
    }

    [Test]
    public void GetSortedEntriesBatchForSlotsAsync_throws_when_sortedSlots_null()
    {
        var h = CreateHarness();
        Assert.That(
            () => h.Grain.GetSortedEntriesBatchForSlotsAsync(null, null, 100, null, null!, VirtualShardCount),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetSortedEntriesBatchForSlotsAsync_throws_when_virtualShardCount_non_positive()
    {
        var h = CreateHarness();
        Assert.That(
            () => h.Grain.GetSortedEntriesBatchForSlotsAsync(null, null, 100, null, [0], 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GetSortedEntriesBatchForSlotsAsync_returns_empty_when_sortedSlots_empty()
    {
        var entries = KeysWithSlots(10).Select(k => new KeyValuePair<string, byte[]>(k, [1])).ToList();
        var h = CreateHarness(leafEntries: entries);

        var page = await h.Grain.GetSortedEntriesBatchForSlotsAsync(null, null, 100, null, [], VirtualShardCount);

        Assert.That(page.Entries, Is.Empty);
    }
}

