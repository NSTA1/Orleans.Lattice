using System.Diagnostics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 3904: the resize drove its snapshot through the
/// run-to-completion <c>RunSnapshotPassAsync</c>, which held the snapshot's
/// non-reentrant turn until every shard was copied. On a large or contended tree
/// that outlived the caller's thirty-second response timeout on every pass and
/// starved the <c>snapshot-keepalive</c> reminder queued behind it.
/// <c>RunSnapshotSliceAsync</c> bounds the call in wall clock, banks each shard's
/// progress, and returns the turn.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private sealed record SliceRig(
        TreeSnapshotGrain Grain,
        FakePersistentState<TreeSnapshotState> State,
        IGrainFactory Factory,
        IShardRootGrain[] SourceShards,
        List<string>[] MergedKeys);

    /// <summary>
    /// Seeds an online snapshot already in its Copy phase over
    /// <paramref name="shardCount"/> source shards of <paramref name="leavesPerShard"/>
    /// leaves each, every leaf holding one entry and taking
    /// <paramref name="leafReadDelay"/> to read, and records every key each
    /// destination shard accepts.
    /// </summary>
    private static SliceRig CreateSlicingSnapshot(
        int shardCount, int leavesPerShard, TimeSpan leafReadDelay, LatticeOptions options,
        TreeSnapshotState? seed = null)
    {
        var existing = new FakePersistentState<TreeSnapshotState>
        {
            State = seed ?? new TreeSnapshotState
            {
                InProgress = true,
                Phase = SnapshotPhase.Copy,
                NextShardIndex = 0,
                DestinationTreeId = DestTreeId,
                Mode = SnapshotMode.Online,
                OperationId = "op-slice",
                ShardCount = shardCount,
            },
        };

        var (grain, state, _, factory, _) = CreateGrain(options, existing);

        var sources = new IShardRootGrain[shardCount];
        var merged = new List<string>[shardCount];
        for (var shard = 0; shard < shardCount; shard++)
        {
            var leafIds = new GrainId[leavesPerShard];
            var entries = new Dictionary<string, byte[]>(leavesPerShard);
            for (var i = 0; i < leavesPerShard; i++)
            {
                leafIds[i] = GrainId.Create("leaf", $"slice-src-{shard}-leaf-{i}");
                entries[$"s{shard}-entry-{i:D3}"] = [(byte)i];
            }

            SetupShardForSnapshot(factory, SourceTreeId, shard, entries, leafIds);
            sources[shard] = factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{shard}");

            if (leafReadDelay > TimeSpan.Zero)
            {
                foreach (var leafId in leafIds)
                {
                    var leaf = factory.GetGrain<IBPlusLeafGrain>(leafId);
                    var raw = leaf.GetLiveRawEntriesAsync().Result;
                    leaf.GetLiveRawEntriesAsync().Returns(_ => DelayedAsync(raw, leafReadDelay));
                }
            }

            var keys = new List<string>();
            merged[shard] = keys;
            var dest = Substitute.For<IShardRootGrain>();
            factory.GetGrain<IShardRootGrain>($"{DestTreeId}/{shard}").Returns(dest);
            dest.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
                .Returns(ci =>
                {
                    lock (keys) keys.AddRange(((Dictionary<string, LwwValue<byte[]>>)ci[0]).Keys);
                    return Task.FromResult<SplitResult?>(null);
                });
        }

        return new SliceRig(grain, state, factory, sources, merged);
    }

    private static async Task<List<LwwEntry>> DelayedAsync(List<LwwEntry> entries, TimeSpan delay)
    {
        await Task.Delay(delay);
        return entries;
    }

    /// <summary>
    /// The regression itself. Fifty leaves at 100 ms each is five seconds of
    /// drain; the unbounded pass would hold the turn for all of it. A slice bound
    /// to 300 ms must return long before that, with the copy only partly done and
    /// its position banked, so the next slice resumes rather than restarts.
    /// </summary>
    [Test]
    public async Task A_slice_returns_within_its_wall_clock_bound_and_banks_the_partial_copy()
    {
        var options = new LatticeOptions
        {
            BackgroundDrainLeavesPerPass = 1000,
            BackgroundDrainMaxDuration = TimeSpan.FromMilliseconds(300),
        };
        var rig = CreateSlicingSnapshot(1, 50, TimeSpan.FromMilliseconds(100), options);

        var clock = Stopwatch.StartNew();
        var finished = await rig.Grain.RunSnapshotSliceAsync();
        clock.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(finished, Is.False, "a slice that stopped part-way must report work remaining");
            Assert.That(clock.Elapsed, Is.LessThan(TimeSpan.FromSeconds(2.5)),
                "the slice must return near its 300 ms bound, not after the whole five-second drain");
            Assert.That(rig.MergedKeys[0].Count, Is.InRange(1, 50 - 1),
                "the slice copies what fits in its bound and no more");
            Assert.That(rig.State.State.InProgress, Is.True);
            Assert.That(rig.State.State.NextShardIndex, Is.EqualTo(0));
            Assert.That(rig.State.State.CopyCursorKey, Is.EqualTo(SnapshotLeafResumeKey(rig.MergedKeys[0].Count)),
                "the head shard's resume key must be persisted exactly where the copy stopped");
            Assert.That(rig.State.WriteCount, Is.GreaterThan(0), "the slice's progress must be written, not held in memory");
        });
        await rig.SourceShards[0].DidNotReceive().MarkDrainedAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Repeated_slices_converge_copying_every_entry_exactly_once()
    {
        var options = new LatticeOptions
        {
            BackgroundDrainLeavesPerPass = 1000,
            BackgroundDrainMaxDuration = TimeSpan.FromMilliseconds(100),
        };
        var rig = CreateSlicingSnapshot(1, 20, TimeSpan.FromMilliseconds(30), options);

        var slices = 0;
        while (!await rig.Grain.RunSnapshotSliceAsync())
        {
            Assert.That(++slices, Is.LessThan(100), "the slices must converge");
        }

        Assert.Multiple(() =>
        {
            Assert.That(slices, Is.GreaterThan(0), "the rig must actually need more than one slice");
            Assert.That(rig.MergedKeys[0], Is.Unique, "a resumed slice must not re-copy a leaf it already banked");
            Assert.That(rig.MergedKeys[0], Has.Count.EqualTo(20));
            Assert.That(rig.State.State.InProgress, Is.False);
            Assert.That(rig.State.State.Complete, Is.True);
        });
        await rig.SourceShards[0].Received(1).MarkDrainedAsync("op-slice");
    }

    /// <summary>
    /// The slice keeps the concurrent drain <see cref="LatticeOptions.MaxConcurrentDrains"/>
    /// governs, so a shard that is not the head can be part-copied when the slice
    /// ends. Its resume key must be banked separately from the head's, or the
    /// next slice would copy it again from the start.
    /// </summary>
    [Test]
    public async Task A_slice_drains_shards_concurrently_and_banks_each_shards_own_cursor()
    {
        var options = new LatticeOptions
        {
            BackgroundDrainLeavesPerPass = 1000,
            BackgroundDrainMaxDuration = TimeSpan.FromMilliseconds(200),
            MaxConcurrentDrains = 2,
        };
        var rig = CreateSlicingSnapshot(2, 30, TimeSpan.FromMilliseconds(50), options);

        Assert.That(await rig.Grain.RunSnapshotSliceAsync(), Is.False);

        Assert.Multiple(() =>
        {
            Assert.That(rig.MergedKeys[0].Count, Is.InRange(1, 30 - 1));
            Assert.That(rig.MergedKeys[1].Count, Is.InRange(1, 30 - 1),
                "the second shard must copy alongside the first, not wait behind it");
            Assert.That(rig.State.State.CopyCursorKey, Is.EqualTo(SnapshotLeafResumeKey(rig.MergedKeys[0].Count)));
            Assert.That(rig.State.State.DrainCursors, Is.Not.Null);
            Assert.That(rig.State.State.DrainCursors![1], Is.EqualTo(SnapshotLeafResumeKey(rig.MergedKeys[1].Count)),
                "the non-head shard's progress must be banked under its own position");
        });

        var slices = 1;
        while (!await rig.Grain.RunSnapshotSliceAsync())
        {
            Assert.That(++slices, Is.LessThan(100), "the slices must converge");
        }

        Assert.Multiple(() =>
        {
            for (var shard = 0; shard < 2; shard++)
            {
                Assert.That(rig.MergedKeys[shard], Is.Unique);
                Assert.That(rig.MergedKeys[shard], Has.Count.EqualTo(30));
            }
            Assert.That(rig.State.State.Complete, Is.True);
            Assert.That(rig.State.State.DrainCursors, Is.Null);
            Assert.That(rig.State.State.DrainedPositions, Is.Null);
        });
    }

    /// <summary>
    /// A later shard can finish inside a slice while the head is still copying.
    /// It is recorded as drained, and when the head finishes the head skips it
    /// and adopts the next shard's banked cursor instead of copying it again.
    /// The sequential timer path honours that record too.
    /// </summary>
    [Test]
    public async Task The_head_advances_past_a_shard_a_slice_already_drained_and_adopts_the_next_cursor()
    {
        var seed = new TreeSnapshotState
        {
            InProgress = true,
            Phase = SnapshotPhase.Copy,
            NextShardIndex = 0,
            DestinationTreeId = DestTreeId,
            Mode = SnapshotMode.Online,
            OperationId = "op-slice",
            ShardCount = 3,
            DrainedPositions = [1],
            DrainCursors = new Dictionary<int, string> { [2] = SnapshotLeafResumeKey(1) },
        };
        var rig = CreateSlicingSnapshot(3, 1, TimeSpan.Zero, new LatticeOptions(), seed);

        await rig.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rig.State.State.NextShardIndex, Is.EqualTo(2),
                "the head must skip the shard a slice already drained");
            Assert.That(rig.State.State.CopyCursorKey, Is.EqualTo(SnapshotLeafResumeKey(1)),
                "the new head must resume from its banked cursor, not from its leftmost leaf");
            Assert.That(rig.State.State.DrainCursors, Is.Null);
            Assert.That(rig.State.State.DrainedPositions, Is.Null);
            Assert.That(rig.MergedKeys[1], Is.Empty, "the already-drained shard must not be copied again");
        });
        await rig.SourceShards[0].Received(1).MarkDrainedAsync("op-slice");
        await rig.SourceShards[1].DidNotReceive().MarkDrainedAsync(Arg.Any<string>());
    }

    [Test]
    public async Task A_slice_skips_a_drained_shard_when_it_launches_work()
    {
        var seed = new TreeSnapshotState
        {
            InProgress = true,
            Phase = SnapshotPhase.Copy,
            NextShardIndex = 0,
            DestinationTreeId = DestTreeId,
            Mode = SnapshotMode.Online,
            OperationId = "op-slice",
            ShardCount = 3,
            DrainedPositions = [1],
        };
        var rig = CreateSlicingSnapshot(3, 2, TimeSpan.Zero, new LatticeOptions(), seed);

        Assert.That(await rig.Grain.RunSnapshotSliceAsync(), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(rig.MergedKeys[0], Has.Count.EqualTo(2));
            Assert.That(rig.MergedKeys[1], Is.Empty, "a shard recorded as drained must not be copied again");
            Assert.That(rig.MergedKeys[2], Has.Count.EqualTo(2));
            Assert.That(rig.State.State.Complete, Is.True);
        });
    }

    /// <summary>
    /// A shard that faults does not throw away the progress the other shards
    /// made in the same slice: that progress is written first, then the fault
    /// surfaces to the caller, which retries on its next tick.
    /// </summary>
    [Test]
    public async Task A_faulting_shard_still_lets_the_slice_bank_every_other_shards_progress()
    {
        var options = new LatticeOptions { MaxConcurrentDrains = 2 };
        var rig = CreateSlicingSnapshot(2, 2, TimeSpan.Zero, options);
        var failingLeaf = rig.Factory.GetGrain<IBPlusLeafGrain>(GrainId.Create("leaf", "slice-src-1-leaf-0"));
        failingLeaf.GetLiveRawEntriesAsync()
            .Returns(Task.FromException<List<LwwEntry>>(new InvalidOperationException("leaf unavailable")));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await rig.Grain.RunSnapshotSliceAsync());

        Assert.Multiple(() =>
        {
            Assert.That(rig.MergedKeys[0], Has.Count.EqualTo(2));
            Assert.That(rig.State.State.NextShardIndex, Is.EqualTo(1),
                "the head shard finished before the fault surfaced, so its advance must be persisted");
            Assert.That(rig.State.State.InProgress, Is.True);
        });
        await rig.SourceShards[0].Received(1).MarkDrainedAsync("op-slice");
    }

    [Test]
    public async Task A_slice_on_an_idle_snapshot_reports_finished_and_writes_nothing()
    {
        var (grain, state, _, _, _) = CreateGrain();

        Assert.That(await grain.RunSnapshotSliceAsync(), Is.True);
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task A_slice_takes_the_phase_steps_ahead_of_the_copy()
    {
        var seed = new TreeSnapshotState
        {
            InProgress = true,
            Phase = SnapshotPhase.ShadowBegin,
            DestinationTreeId = DestTreeId,
            Mode = SnapshotMode.Online,
            OperationId = "op-slice",
            ShardCount = 1,
        };
        var rig = CreateSlicingSnapshot(1, 2, TimeSpan.Zero, new LatticeOptions(), seed);

        Assert.That(await rig.Grain.RunSnapshotSliceAsync(), Is.True);

        await rig.SourceShards[0].Received(1).BeginShadowForwardAsync(DestTreeId, "op-slice", SourceTreeId);
        Assert.That(rig.MergedKeys[0], Has.Count.EqualTo(2));
    }

    [TestCase(0, 10_000)]
    [TestCase(-5, 10_000)]
    [TestCase(2_000, 2_000)]
    [TestCase(10_000, 10_000)]
    [TestCase(60_000, 10_000)]
    public void SliceDuration_is_the_background_drain_bound_capped_below_the_response_timeout(
        int configuredMs, int expectedMs)
    {
        var options = new LatticeOptions { BackgroundDrainMaxDuration = TimeSpan.FromMilliseconds(configuredMs) };

        Assert.That(TreeSnapshotGrain.SliceDuration(options), Is.EqualTo(TimeSpan.FromMilliseconds(expectedMs)));
    }

    [Test]
    public void The_slice_cap_leaves_headroom_under_the_default_response_timeout()
    {
        Assert.That(TreeSnapshotGrain.MaxSnapshotSliceDuration, Is.LessThanOrEqualTo(TimeSpan.FromSeconds(15)),
            "a slice must return well inside Orleans' default thirty-second response timeout");
    }
}
