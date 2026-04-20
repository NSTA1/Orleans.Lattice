using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeSnapshotGrainTests
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

            // snapshot TTL preservation: snapshot now drains raw LwwValue
            // entries via GetLiveRawEntriesAsync. Stub the raw method so tests
            // exercise the new snapshot path.
            var rawPerLeaf = new List<LwwEntry>(entriesPerLeaf[i].Count);
            foreach (var kvp in entriesPerLeaf[i])
            {
                var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
                rawPerLeaf.Add(new LwwEntry(kvp.Key, LwwValue<byte[]>.Create(kvp.Value, hlc)));
            }
            leafMock.GetLiveRawEntriesAsync().Returns(Task.FromResult(rawPerLeaf));
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

}
