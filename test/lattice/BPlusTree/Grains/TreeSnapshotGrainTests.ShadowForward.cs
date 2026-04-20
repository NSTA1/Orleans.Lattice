using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the online-snapshot shadow-forwarding drive path added to
/// <see cref="TreeSnapshotGrain"/>. Covers:
/// <list type="bullet">
/// <item><description>Initial phase for <see cref="SnapshotMode.Online"/> is <see cref="SnapshotPhase.ShadowBegin"/>.</description></item>
/// <item><description><c>BeginShadowForwardAllShardsAsync</c> invokes <see cref="IShardRootGrain.BeginShadowForwardAsync"/> on every source shard with the snapshot's operationId and destination tree.</description></item>
/// <item><description><c>DrainAllShardsOnlineAsync</c> copies every shard then calls <see cref="IShardRootGrain.MarkDrainedAsync"/> on each.</description></item>
/// <item><description>Per-tick Online Copy path calls <see cref="IShardRootGrain.MarkDrainedAsync"/> after each <c>CopyShardAsync</c>.</description></item>
/// <item><description><c>RunSnapshotPassAsync</c> drives the full Online flow from <see cref="SnapshotPhase.ShadowBegin"/> through <see cref="SnapshotPhase.Copy"/> to completion.</description></item>
/// <item><description>Concurrency cap from <see cref="LatticeOptions.MaxConcurrentDrains"/> is respected.</description></item>
/// </list>
/// </summary>
public partial class TreeSnapshotGrainTests
{
    [Test]
    public async Task Online_initiate_sets_shadow_begin_phase()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount);

        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.ShadowBegin));
    }

    [Test]
    public async Task BeginShadowForwardAllShards_calls_each_shard_with_destination_and_opid()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "op-1";
        state.State.Mode = SnapshotMode.Online;

        await grain.BeginShadowForwardAllShardsAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).BeginShadowForwardAsync(DestTreeId, "op-1", SourceTreeId);
        }
    }

    [Test]
    public async Task BeginShadowForwardAllShards_advances_phase_to_copy_and_resets_index()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "op-1";
        state.State.Mode = SnapshotMode.Online;
        state.State.NextShardIndex = 99; // garbage — should reset

        await grain.BeginShadowForwardAllShardsAsync();

        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
        Assert.That(state.State.ShardRetries, Is.EqualTo(0));
    }

    [Test]
    public void BeginShadowForwardAllShards_throws_when_operationId_missing()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = null;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.BeginShadowForwardAllShardsAsync());
    }

    [Test]
    public void BeginShadowForwardAllShards_throws_when_destination_missing()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = null;
        state.State.OperationId = "op-1";

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.BeginShadowForwardAllShardsAsync());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_dispatches_ShadowBegin()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "op-1";
        state.State.Mode = SnapshotMode.Online;

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).BeginShadowForwardAsync(DestTreeId, "op-1", SourceTreeId);
        }
    }

    [Test]
    public async Task Online_per_tick_copy_marks_shard_drained()
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
        state.State.OperationId = "op-drain";

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0")
            .Received(1).MarkDrainedAsync("op-drain");
        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/1")
            .DidNotReceive().MarkDrainedAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Offline_per_tick_copy_does_not_mark_drained()
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
        state.State.OperationId = "op-offline";

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0")
            .DidNotReceive().MarkDrainedAsync(Arg.Any<string>());
    }

    [Test]
    public async Task DrainAllShardsOnline_copies_and_marks_each_shard_drained()
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
        state.State.OperationId = "op-drain-all";

        await grain.DrainAllShardsOnlineAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).MarkDrainedAsync("op-drain-all");
        }
        Assert.That(state.State.NextShardIndex, Is.EqualTo(ShardCount));
    }

    [Test]
    public async Task DrainAllShardsOnline_respects_MaxConcurrentDrains_cap()
    {
        // 8 shards, cap=2 → never more than 2 drains in flight.
        const int shardCount = 8;
        var options = new LatticeOptions
        {
            ShardCount = shardCount,
            MaxConcurrentDrains = 2,
        };
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain(options);
        SetupKeepalive(reminderRegistry);

        // Instrument each source shard to block until signalled so we can
        // observe peak concurrency.
        var inFlight = 0;
        var peak = 0;
        var gate = new TaskCompletionSource();
        var seen = 0;

        for (int i = 0; i < shardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}").Returns(shard);
            shard.GetLeftmostLeafIdAsync().Returns(async _ =>
            {
                var now = Interlocked.Increment(ref inFlight);
                while (true)
                {
                    var current = peak;
                    if (now <= current) break;
                    if (Interlocked.CompareExchange(ref peak, now, current) == current) break;
                }
                if (Interlocked.Increment(ref seen) >= 2) gate.TrySetResult();
                await gate.Task;
                Interlocked.Decrement(ref inFlight);
                return (Orleans.Runtime.GrainId?)null;
            });
        }

        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = shardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "op-cap";

        await grain.DrainAllShardsOnlineAsync();

        Assert.That(peak, Is.EqualTo(2), "Peak concurrent drains must not exceed MaxConcurrentDrains");
    }

    [Test]
    public async Task RunSnapshotPassAsync_drives_online_flow_from_ShadowBegin_to_complete()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "op-e2e";

        await grain.RunSnapshotPassAsync();

        // BeginShadowForward on every shard.
        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).BeginShadowForwardAsync(DestTreeId, "op-e2e", SourceTreeId);
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).MarkDrainedAsync("op-e2e");
        }
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task RunSnapshotPassAsync_offline_path_unchanged()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "op-off";

        await grain.RunSnapshotPassAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).MarkDeletedAsync();
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).UnmarkDeletedAsync();
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .DidNotReceive().BeginShadowForwardAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .DidNotReceive().MarkDrainedAsync(Arg.Any<string>());
        }
        Assert.That(state.State.Complete, Is.True);
    }
}
