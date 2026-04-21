using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeSnapshotGrainTests
{
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

        // Idempotent call must leave the in-progress marker and sizing unchanged.
        Assert.That(state.State.InProgress, Is.True);
        Assert.That(state.State.MaxLeafKeys, Is.EqualTo(256));
        Assert.That(state.State.MaxInternalChildren, Is.EqualTo(64));
    }

    // --- Copy phase ---

    [Test]
    public async Task Copy_drains_source_and_merges_destination_in_online_mode()
    {
        // Online drain must use MergeManyAsync (not BulkLoadRawAsync) so
        // concurrent shadow-forward writes that have already populated the
        // destination shard converge with the drain batch via LWW rather
        // than violating BulkLoadRawAsync's empty-shard precondition.
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

        var destShard = grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/0");
        await destShard.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 2
                && d.ContainsKey("key-a") && d.ContainsKey("key-b")));
        await destShard.DidNotReceive().BulkLoadRawAsync(
            Arg.Any<string>(), Arg.Any<List<LwwEntry>>());
    }

    [Test]
    public async Task Copy_drains_source_and_bulk_loads_destination_in_offline_mode()
    {
        // Offline drain: source is locked, destination is guaranteed empty,
        // so the efficient bottom-up BulkLoadRawAsync path is used.
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
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "test-op";

        await grain.ProcessNextPhaseAsync();

        var destShard = grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/0");
        await destShard.Received(1).BulkLoadRawAsync("test-op-snapshot-0",
            Arg.Is<List<LwwEntry>>(e => e.Count == 2));
        await destShard.DidNotReceive().MergeManyAsync(
            Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
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
}
