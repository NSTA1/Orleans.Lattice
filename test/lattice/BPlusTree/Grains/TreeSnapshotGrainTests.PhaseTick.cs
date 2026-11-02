using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #5455: the snapshot coordinator's phase timer
/// advanced one step per two-second tick, so an offline snapshot of a 96-shard
/// tree - a lock, then a copy and an unlock per shard - held its source locked
/// for over six minutes. A tick now takes as many steps as fit in a bounded slice.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private const int PacedShardCount = 96;

    /// <summary>
    /// Seeds an offline snapshot at its Lock phase over
    /// <paramref name="shardCount"/> source shards of one entry each, with a
    /// substitute destination shard per index.
    /// </summary>
    private static (TreeSnapshotGrain Grain, FakePersistentState<TreeSnapshotState> State, IGrainFactory Factory)
        CreatePacedOfflineSnapshot(int shardCount)
    {
        var (grain, state, reminderRegistry, factory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        for (var shard = 0; shard < shardCount; shard++)
        {
            SetupShardForSnapshot(factory, SourceTreeId, shard,
                new Dictionary<string, byte[]> { [$"key-{shard:D3}"] = [(byte)shard] },
                GrainId.Create("leaf", $"paced-src-{shard}"));
            var dest = Substitute.For<IShardRootGrain>();
            factory.GetGrain<IShardRootGrain>($"{DestTreeId}/{shard}").Returns(dest);
        }

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = shardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "paced";
        return (grain, state, factory);
    }

    [Test]
    public async Task One_phase_tick_carries_an_offline_snapshot_of_many_shards_to_completion()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(PacedShardCount);

        await grain.ProcessPhaseTickAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False,
                "one tick must take every step that fits in its slice, not one step per two-second tick");
            Assert.That(state.State.Complete, Is.True);
        });
        for (var shard = 0; shard < PacedShardCount; shard++)
        {
            var source = factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{shard}");
            await source.Received(1).MarkDeletedAsync();
            await source.Received(1).UnmarkDeletedAsync();
            await factory.GetGrain<IShardRootGrain>($"{DestTreeId}/{shard}")
                .Received(1).BulkLoadRawAsync($"paced-snapshot-{shard}", Arg.Any<List<LwwEntry>>());
        }
    }

    [Test]
    public async Task A_single_phase_step_still_advances_one_step()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(PacedShardCount);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(state.State.NextShardIndex, Is.Zero);
        });
        await factory.GetGrain<IShardRootGrain>($"{DestTreeId}/0")
            .DidNotReceive().BulkLoadRawAsync(Arg.Any<string>(), Arg.Any<List<LwwEntry>>());
    }

    [Test]
    public async Task A_step_that_spends_a_retry_ends_the_tick_so_the_retry_waits_for_the_next_one()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(4);
        var failing = factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/1");
        failing.UnmarkDeletedAsync().ThrowsAsync(new InvalidOperationException("shard unavailable"));

        await grain.ProcessPhaseTickAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Unmark));
            Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
            Assert.That(state.State.ShardRetries, Is.EqualTo(1));
        });
        await failing.Received(1).UnmarkDeletedAsync();
        await factory.GetGrain<IShardRootGrain>($"{DestTreeId}/2")
            .DidNotReceive().BulkLoadRawAsync(Arg.Any<string>(), Arg.Any<List<LwwEntry>>());
    }

    [Test]
    public async Task A_phase_tick_on_an_idle_snapshot_writes_nothing()
    {
        var (grain, state, _, _, _) = CreateGrain();

        await grain.ProcessPhaseTickAsync();

        Assert.That(state.WriteCount, Is.Zero);
    }

    [TestCase(0, 5_000)]
    [TestCase(2_000, 2_000)]
    [TestCase(5_000, 5_000)]
    [TestCase(10_000, 5_000)]
    [TestCase(60_000, 5_000)]
    public void PhaseTickSliceDuration_is_the_slice_bound_capped_at_the_tick_cap(int configuredMs, int expectedMs)
    {
        var options = new LatticeOptions { BackgroundDrainMaxDuration = TimeSpan.FromMilliseconds(configuredMs) };

        Assert.That(TreeSnapshotGrain.PhaseTickSliceDuration(options), Is.EqualTo(TimeSpan.FromMilliseconds(expectedMs)));
    }

    [Test]
    public void A_tick_and_a_queued_slice_together_stay_inside_the_default_response_timeout()
    {
        Assert.That(TreeSnapshotGrain.MaxPhaseTickSliceDuration + TreeSnapshotGrain.MaxSnapshotSliceDuration,
            Is.LessThanOrEqualTo(TimeSpan.FromSeconds(15)));
    }
}
