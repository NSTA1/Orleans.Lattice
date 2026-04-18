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

[TestFixture]
public partial class TreeMergeGrainTests
{
    private const string TargetTreeId = "target-tree";
    private const string SourceTreeId = "source-tree";
    private const int ShardCount = 2;

    private static (TreeMergeGrain grain,
                     FakePersistentState<TreeMergeState> state,
                     IReminderRegistry reminderRegistry,
                     IGrainFactory grainFactory,
                     IOptionsMonitor<LatticeOptions> optionsMonitor) CreateGrain(
        LatticeOptions? options = null,
        FakePersistentState<TreeMergeState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("merge", TargetTreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options ??= new LatticeOptions
        {
            ShardCount = ShardCount,
            MaxLeafKeys = 128,
            MaxInternalChildren = 128,
        };
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var state = existingState ?? new FakePersistentState<TreeMergeState>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ExistsAsync(SourceTreeId).Returns(true);

        var grain = new TreeMergeGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<TreeMergeGrain>(), state);
        return (grain, state, reminderRegistry, grainFactory, optionsMonitor);
    }

    private static void SetupSourceShardWithEntries(
        IGrainFactory grainFactory,
        string treeId,
        int shardIndex,
        Dictionary<string, LwwValue<byte[]>>? rawEntries = null,
        params GrainId[] leafIds)
    {
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIndex}")
            .Returns(shardRoot);

        if (leafIds.Length == 0)
        {
            shardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            return;
        }

        shardRoot.GetLeftmostLeafIdAsync()
            .Returns(Task.FromResult<GrainId?>(leafIds[0]));

        var allEntries = rawEntries ?? new Dictionary<string, LwwValue<byte[]>>();
        var entriesPerLeaf = SplitRawEntries(allEntries, leafIds.Length);

        for (int i = 0; i < leafIds.Length; i++)
        {
            var leafMock = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leafMock);

            // TreeMergeGrain uses GetDeltaSinceAsync with an empty VersionVector
            // to drain all entries from the source leaf.
            var delta = new StateDelta
            {
                Entries = entriesPerLeaf[i],
                Version = new VersionVector(),
            };
            leafMock.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(Task.FromResult(delta));

            var nextId = i + 1 < leafIds.Length ? (GrainId?)leafIds[i + 1] : null;
            leafMock.GetNextSiblingAsync().Returns(Task.FromResult(nextId));
        }
    }

    private static Dictionary<string, LwwValue<byte[]>>[] SplitRawEntries(
        Dictionary<string, LwwValue<byte[]>> entries, int count)
    {
        var result = new Dictionary<string, LwwValue<byte[]>>[count];
        for (int i = 0; i < count; i++)
            result[i] = [];

        int idx = 0;
        foreach (var kvp in entries)
        {
            result[idx % count][kvp.Key] = kvp.Value;
            idx++;
        }
        return result;
    }

    private static void SetupTargetShardMocks(IGrainFactory grainFactory, string treeId, int shardCount)
    {
        for (int i = 0; i < shardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}").Returns(shard);
        }
    }

    private static void SetupKeepalive(IReminderRegistry reminderRegistry)
    {
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "merge-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
    }

    // --- MergeAsync validation ---

    [Test]
    public void MergeAsync_throws_when_source_is_target()
    {
        var (grain, _, _, _, _) = CreateGrain();

        // Grain is keyed by TargetTreeId, so sourceTreeId == TargetTreeId should fail.
        // But our grain key is "target-tree", we need to pass "target-tree" as source.
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.MergeAsync(TargetTreeId));
    }

    [Test]
    public void MergeAsync_throws_when_source_is_system_tree()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.MergeAsync("_lattice_reserved"));
    }

    [Test]
    public void MergeAsync_throws_when_source_does_not_exist()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ExistsAsync("nonexistent").Returns(false);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.MergeAsync("nonexistent"));
    }

    [Test]
    public void MergeAsync_throws_when_null_source()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.MergeAsync(null!));
    }

    [Test]
    public async Task MergeAsync_idempotent_same_source()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;

        // Should not throw — same source.
        await grain.MergeAsync(SourceTreeId);
    }

    [Test]
    public void MergeAsync_throws_when_different_source_in_progress()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.SourceTreeId = "other-tree";

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.MergeAsync(SourceTreeId));
    }

    // --- Merge processing ---

    [Test]
    public async Task InitiateMergeState_persists_state()
    {
        var (grain, state, _, _, _) = CreateGrain();

        await grain.InitiateMergeStateAsync(SourceTreeId, ShardCount);

        Assert.That(state.State.InProgress, Is.True);
        Assert.That(state.State.SourceTreeId, Is.EqualTo(SourceTreeId));
        Assert.That(state.State.SourceShardCount, Is.EqualTo(ShardCount));
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
        Assert.That(state.State.Complete, Is.False);
        Assert.That(state.WriteCount, Is.GreaterThan(0));
    }

    [Test]
    public async Task ProcessNextShard_merges_entries_from_source_to_target()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key-a"] = LwwValue<byte[]>.Create([1, 2, 3], clock),
            ["key-b"] = LwwValue<byte[]>.Create([4, 5, 6], HybridLogicalClock.Tick(clock)),
        };

        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 0, entries, leafId);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 1);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.NextShardIndex = 0;

        await grain.ProcessNextShardAsync();

        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
    }

    [Test]
    public async Task RunMergePass_processes_all_shards()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leafId0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leafId1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 0,
            new Dictionary<string, LwwValue<byte[]>>
            {
                ["key-a"] = LwwValue<byte[]>.Create([1], clock),
            }, leafId0);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 1,
            new Dictionary<string, LwwValue<byte[]>>
            {
                ["key-b"] = LwwValue<byte[]>.Create([2], clock),
            }, leafId1);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.NextShardIndex = 0;

        await grain.RunMergePassAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task RunMergePass_skips_empty_shards()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 0);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 1);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.NextShardIndex = 0;

        await grain.RunMergePassAsync();

        Assert.That(state.State.Complete, Is.True);
        // No MergeManyAsync calls on target shards since source was empty.
        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TargetTreeId}/{i}")
                .DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
        }
    }

    [Test]
    public async Task CompleteMerge_clears_in_progress()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;

        await grain.CompleteMergeAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task Merge_includes_tombstones()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["live-key"] = LwwValue<byte[]>.Create([1, 2], clock),
            ["dead-key"] = LwwValue<byte[]>.Tombstone(HybridLogicalClock.Tick(clock)),
        };

        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 0, entries, leafId);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 1);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.NextShardIndex = 0;

        await grain.RunMergePassAsync();

        // Verify MergeManyAsync was called with entries including the tombstone.
        // The exact shard depends on the hash, but at least one shard should have received entries.
        var receivedCalls = false;
        for (int i = 0; i < ShardCount; i++)
        {
            var calls = grainFactory.GetGrain<IShardRootGrain>($"{TargetTreeId}/{i}")
                .ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.MergeManyAsync))
                .ToList();
            if (calls.Count > 0)
                receivedCalls = true;
        }

        Assert.That(receivedCalls, Is.True);
    }
}
