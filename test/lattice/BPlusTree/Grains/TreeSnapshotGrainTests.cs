using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class TreeSnapshotGrainTests
{
    private const string SourceTreeId = "source-tree";
    private const string DestTreeId = "dest-tree";
    private const int ShardCount = 2;

    private static (TreeSnapshotGrain grain,
                     FakePersistentState<TreeSnapshotState> state,
                     IReminderRegistry reminderRegistry,
                     IGrainFactory grainFactory,
                     IOptionsMonitor<LatticeOptions> optionsMonitor) CreateGrain(
        LatticeOptions? options = null,
        FakePersistentState<TreeSnapshotState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("snapshot", SourceTreeId));
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
        var state = existingState ?? new FakePersistentState<TreeSnapshotState>();

        // Setup default mocks.
        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        grainFactory.GetGrain<ITombstoneCompactionGrain>(Arg.Any<string>()).Returns(compaction);

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ExistsAsync(Arg.Any<string>()).Returns(false);

        var grain = new TreeSnapshotGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<TreeSnapshotGrain>(), state);
        return (grain, state, reminderRegistry, grainFactory, optionsMonitor);
    }

    private static void SetupShardForSnapshot(
        IGrainFactory grainFactory,
        string treeId,
        int shardIndex,
        Dictionary<string, byte[]>? liveEntries = null,
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

        var allEntries = liveEntries ?? new Dictionary<string, byte[]>();
        var entriesPerLeaf = SplitEntries(allEntries, leafIds.Length);

        for (int i = 0; i < leafIds.Length; i++)
        {
            var leafMock = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leafMock);
            leafMock.GetLiveEntriesAsync().Returns(Task.FromResult(entriesPerLeaf[i]));

            var nextId = i + 1 < leafIds.Length ? (GrainId?)leafIds[i + 1] : null;
            leafMock.GetNextSiblingAsync().Returns(Task.FromResult(nextId));
        }
    }

    private static Dictionary<string, byte[]>[] SplitEntries(
        Dictionary<string, byte[]> entries, int count)
    {
        var result = new Dictionary<string, byte[]>[count];
        for (int i = 0; i < count; i++)
            result[i] = new Dictionary<string, byte[]>();

        int idx = 0;
        foreach (var kvp in entries)
        {
            result[idx % count][kvp.Key] = kvp.Value;
            idx++;
        }
        return result;
    }

    private static void SetupShardMocks(IGrainFactory grainFactory, string treeId)
    {
        for (int i = 0; i < ShardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}").Returns(shard);
        }
    }

    private static void SetupKeepalive(IReminderRegistry reminderRegistry)
    {
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "snapshot-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
    }

    // --- SnapshotAsync validation ---

    [Test]
    public void SnapshotAsync_throws_when_destination_is_source()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SnapshotAsync(SourceTreeId, SnapshotMode.Offline));
    }

    [Test]
    public void SnapshotAsync_throws_when_destination_is_system_tree()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SnapshotAsync("_lattice_reserved", SnapshotMode.Offline));
    }

    [Test]
    public void SnapshotAsync_throws_when_shard_counts_differ()
    {
        var (grain, _, _, grainFactory, optionsMonitor) = CreateGrain();
        optionsMonitor.Get(DestTreeId).Returns(new LatticeOptions { ShardCount = 4 });

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline));
    }

    [Test]
    public void SnapshotAsync_throws_when_destination_exists()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ExistsAsync(DestTreeId).Returns(true);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline));
    }

    [Test]
    public async Task SnapshotAsync_idempotent_with_same_parameters()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;

        await grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline);
    }

    [Test]
    public void SnapshotAsync_throws_when_in_progress_with_different_parameters()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.DestinationTreeId = "other-tree";
        state.State.Mode = SnapshotMode.Offline;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline));
    }

    [Test]
    public void SnapshotAsync_throws_on_null_destination()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.SnapshotAsync(null!, SnapshotMode.Offline));
    }

    [Test]
    public void SnapshotAsync_throws_on_invalid_maxLeafKeys()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline, maxLeafKeys: 1));
    }

    [Test]
    public void SnapshotAsync_throws_on_invalid_maxInternalChildren()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline, maxInternalChildren: 2));
    }

    // --- Initiate ---

    [Test]
    public async Task Offline_snapshot_marks_source_shards_deleted()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, DestTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Offline, ShardCount);
        await grain.LockSourceShardsAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).MarkDeletedAsync();
        }
    }

    [Test]
    public async Task Online_snapshot_does_not_mark_source_shards_deleted()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, DestTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount);

        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .DidNotReceive().MarkDeletedAsync();
        }
    }

    [Test]
    public async Task Initiate_persists_snapshot_intent()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount,
            maxLeafKeys: 256, maxInternalChildren: 64);

        Assert.That(state.State.InProgress, Is.True);
        Assert.That(state.State.DestinationTreeId, Is.EqualTo(DestTreeId));
        Assert.That(state.State.Mode, Is.EqualTo(SnapshotMode.Online));
        Assert.That(state.State.ShardCount, Is.EqualTo(ShardCount));
        Assert.That(state.State.OperationId, Is.Not.Null.And.Not.Empty);
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
        Assert.That(state.State.MaxLeafKeys, Is.EqualTo(256));
        Assert.That(state.State.MaxInternalChildren, Is.EqualTo(64));
    }

    [Test]
    public async Task Initiate_registers_destination_in_registry()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount, 256, 64);

        await registry.Received(1).RegisterAsync(DestTreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.MaxLeafKeys == 256 && e.MaxInternalChildren == 64));
    }

    // --- Lock phase ---

    [Test]
    public async Task Offline_initiate_sets_lock_phase()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Offline, ShardCount);

        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Lock));
    }

    [Test]
    public async Task LockSourceShards_advances_to_copy()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.ShardCount = ShardCount;
        state.State.Mode = SnapshotMode.Offline;

        await grain.LockSourceShardsAsync();

        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).MarkDeletedAsync();
        }
    }

    [Test]
    public async Task Initiate_does_not_mark_shards_for_offline()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Offline, ShardCount);

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .DidNotReceive().MarkDeletedAsync();
        }
    }

    [Test]
    public async Task Idempotency_rejects_different_sizing()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.MaxLeafKeys = 256;
        state.State.MaxInternalChildren = 64;

        // Same dest+mode but different sizing should throw.
        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline, maxLeafKeys: 512));
    }

    [Test]
    public async Task Idempotency_accepts_same_sizing()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.MaxLeafKeys = 256;
        state.State.MaxInternalChildren = 64;

        // Same params including sizing — should not throw.
        await grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline, maxLeafKeys: 256, maxInternalChildren: 64);
    }

    // --- Copy phase ---

    [Test]
    public async Task Copy_drains_source_and_bulk_loads_destination()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var entries = new Dictionary<string, byte[]>
        {
            ["key-a"] = [1, 2, 3],
            ["key-b"] = [4, 5, 6]
        };
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0, entries, leafId);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "test-op";

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/0")
            .Received(1).BulkLoadAsync("test-op-snapshot-0", Arg.Is<List<KeyValuePair<string, byte[]>>>(
                e => e.Count == 2));
    }

    [Test]
    public async Task Copy_skips_bulk_load_for_empty_shard()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        SetupShardForSnapshot(grainFactory, SourceTreeId, 0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "test-op";

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/0")
            .DidNotReceive().BulkLoadAsync(Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Online vs Offline phase flow ---

    [Test]
    public async Task Online_copy_advances_directly_to_next_shard()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "test-op";

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
    }

    [Test]
    public async Task Offline_copy_transitions_to_unmark()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "test-op";

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Unmark));
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
    }

    [Test]
    public async Task Unmark_restores_source_shard_and_advances()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Unmark;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "test-op";

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0")
            .Received(1).UnmarkDeletedAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
    }

    // --- Retry ---

    [Test]
    public async Task ProcessNextPhase_retries_on_first_failure()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0").Returns(shardRoot);
        shardRoot.GetLeftmostLeafIdAsync().Throws(new InvalidOperationException("transient"));

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "test-op";

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.ShardRetries, Is.EqualTo(1));
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
    }

    [Test]
    public void ProcessNextPhase_throws_after_exhausting_retries()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0").Returns(shardRoot);
        shardRoot.GetLeftmostLeafIdAsync().Throws(new InvalidOperationException("persistent"));

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.ShardRetries = 1;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "test-op";

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ProcessNextPhaseAsync());
    }

    // --- Completion ---

    [Test]
    public async Task CompleteSnapshot_resets_state_and_sets_up_compaction()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        state.State.InProgress = true;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "test-op";

        await grain.CompleteSnapshotAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);

        await grainFactory.GetGrain<ITombstoneCompactionGrain>(DestTreeId)
            .Received(1).EnsureReminderAsync();
    }

    [Test]
    public async Task CompleteSnapshot_unregisters_keepalive()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        var mockReminder = Substitute.For<IGrainReminder>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "snapshot-keepalive")
            .Returns(Task.FromResult(mockReminder));

        state.State.DestinationTreeId = DestTreeId;

        await grain.CompleteSnapshotAsync();

        await reminderRegistry.Received(1)
            .UnregisterReminder(Arg.Any<GrainId>(), mockReminder);
    }

    // --- Full pass ---

    [Test]
    public async Task Full_offline_pass_copies_all_shards()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0,
            new Dictionary<string, byte[]> { ["a"] = [1] }, leaf0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1,
            new Dictionary<string, byte[]> { ["b"] = [2] }, leaf1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "full-pass";

        // Lock phase: marks source shards deleted, advances to Copy.
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));

        // Shard 0: Copy → Unmark
        await grain.ProcessNextPhaseAsync(); // Copy shard 0
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Unmark));
        await grain.ProcessNextPhaseAsync(); // Unmark shard 0
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));

        // Shard 1: Copy → Unmark
        await grain.ProcessNextPhaseAsync(); // Copy shard 1
        await grain.ProcessNextPhaseAsync(); // Unmark shard 1
        Assert.That(state.State.NextShardIndex, Is.EqualTo(2));

        // Completion
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task Full_online_pass_copies_all_shards_without_unmark()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0,
            new Dictionary<string, byte[]> { ["a"] = [1] }, leaf0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "full-pass";

        // Shard 0: Copy (advances directly)
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));

        // Shard 1: Copy (advances directly)
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(2));

        // Completion
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }
}
