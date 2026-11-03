using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for a leaf's live checkpoint advance (issues #5570, #5572, #5593; epic
/// #5605, member 4; <c>BPlusLeafGrain.LiveCheckpoint.cs</c>). A leaf advances its
/// projection checkpoint over the appends it issued itself, without reading the WAL,
/// only when the design model's <c>Armed</c> predicate holds
/// (<c>spec/wal/design/WalLiveAdvance.tla</c>). Each guard is pinned here on a real
/// <see cref="LeafAppendEpochFence"/> driven through <see cref="LiveAdvanceRig"/>: the
/// claim raised and read to its head, no open or unread hand-off, every reported
/// unstamped offset read, every own append applied, no saga pending, no other leaf
/// that may own the keys, and a storage provider known to enforce ETags. Whenever a
/// guard fails the leaf advances by replay, as before.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static Task SetStampedAsync(BPlusLeafGrain grain, string key, string value) =>
        grain.SetStampedAsync(key, Utf8(value), 0, LiveAdvanceRig.Routing);

    private static async Task<(BPlusLeafGrain Grain, LiveAdvanceRig Rig)> ArmedLeafWithWritesAsync(params string[] keys)
    {
        var rig = new LiveAdvanceRig();
        var grain = CreateGrain(liveAdvance: rig);
        foreach (var key in keys)
        {
            await SetStampedAsync(grain, key, "v");
        }

        return (grain, rig);
    }

    [Test]
    public async Task A_live_written_leaf_of_a_new_tree_advances_its_checkpoint_without_a_replay()
    {
        // #5570 / #5572: the claim head of an empty partition is 0, so a leaf at -1 has
        // read everything before its claim and arms with no replay.
        var (grain, _) = await ArmedLeafWithWritesAsync("a", "b", "c");

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L),
            "precondition: a live write does not itself move the checkpoint.");

        var advanced = await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(2L),
                "the checkpoint reaches the highest offset the leaf's own applied appends returned, with no WAL read.");
        });
    }

    [Test]
    public async Task A_leaf_that_has_not_read_to_its_claim_head_advances_only_by_replay()
    {
        // #5593: offsets 0..4 were appended before this activation's claim - by a
        // duplicate activation of the leaf, or by any other author. The claim head is
        // 5, so passing them unread would lose a duplicate's acknowledged writes.
        var rig = new LiveAdvanceRig(firstOffset: 5) { ClaimHead = 5 };
        var grain = CreateGrain(liveAdvance: rig);
        await SetStampedAsync(grain, "a", "1");
        await SetStampedAsync(grain, "b", "2");

        var advancedEarly = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.That(advancedEarly, Is.False, "rp >= claimHead - 1 does not hold");
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));

        // A replay has read through offset 4 (here, the checkpoint hint seam).
        await grain.SetCheckpointOffsetHintsAsync([4]);

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(6L),
            "once the read position reaches the claim head, the advance covers the leaf's own appends.");
    }

    [Test]
    public async Task A_reported_unstamped_append_holds_the_advance_until_a_replay_reads_past_it()
    {
        // The design owner's UnstampedNotReported detector: an armed leaf live-advances,
        // a foreign unstamped append is then reported to it, and it falls back to replay.
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True, "precondition: the leaf is armed");
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(0L));

        var foreign = rig.Writer.ForeignAppend();
        rig.ReportedWatermark = foreign;
        await SetStampedAsync(grain, "b", "2");

        var advanced = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(foreign, Is.EqualTo(1L));
            Assert.That(advanced, Is.False, "rp >= seenU does not hold: offset 1 is unstamped and unread");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(0L));
        });

        await grain.SetCheckpointOffsetHintsAsync([1]);

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(2L));
    }

    [Test]
    public async Task A_write_through_an_existing_leaf_method_reports_itself_and_holds_the_advance()
    {
        // S3: an existing method carries no routing generation, so its append is
        // unstamped; the activation reports it to itself.
        var rig = new LiveAdvanceRig();
        var grain = CreateGrain(liveAdvance: rig);
        await SetStampedAsync(grain, "a", "1");
        await grain.SetAsync("b", Utf8("2"));

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));
    }

    [Test]
    public async Task An_open_leaf_marker_holds_the_advance()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        await rig.SetLeafMarkerAsync(m => HandOffMarkerCore.Open(m, "split:donor", null, null));

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False, "~MarkerOpen does not hold");
    }

    [Test]
    public async Task An_open_shard_marker_token_holds_only_the_leaves_it_overlaps()
    {
        var rig = new LiveAdvanceRig();
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "m";
        state.State.HighKeyExclusive = "t";
        var grain = CreateGrain(state, liveAdvance: rig);
        await SetStampedAsync(grain, "n", "1");

        HandOffMarkerCore.Open(rig.ShardMarker, "rd:elsewhere", "a", "c");
        var heldByDisjoint = !await grain.TryAdvanceLiveCheckpointAsync();

        await SetStampedAsync(grain, "o", "2");
        HandOffMarkerCore.Open(rig.ShardMarker, "bulk:op-0", "p", null);
        var heldByOverlap = !await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(heldByDisjoint, Is.False, "a range delete over [a, c) does not reach [m, t)");
            Assert.That(heldByOverlap, Is.True, "a bulk switch over [p, null) overlaps [m, t)");
        });
    }

    [Test]
    public async Task A_close_head_above_the_checkpoint_holds_the_advance_until_a_replay_reaches_it()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a", "b", "c");
        rig.ShardMarker.Closes.Add(new HandOffClose { Token = "migrate:x", Heads = [2], Sequence = 1 });

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False,
            "rp >= MarkerClosedAt - 1 does not hold: offset 1 may be the migration's");

        await grain.SetCheckpointOffsetHintsAsync([1]);

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(2L));
    }

    [Test]
    public async Task An_uninitialised_leaf_marker_holds_until_the_leaf_adopts_its_shards_flag()
    {
        var rig = new LiveAdvanceRig();
        rig.ShardMarker = new ShardHandOffState { Initialised = false };
        var grain = CreateGrain(liveAdvance: rig);
        await SetStampedAsync(grain, "a", "1");

        var heldUninitialised = !await grain.TryAdvanceLiveCheckpointAsync();
        var leafBefore = (await rig.LeafMarkerAsync()).Initialised;

        rig.ShardMarker = new ShardHandOffState { Initialised = true };
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();
        var leafAfter = (await rig.LeafMarkerAsync()).Initialised;

        Assert.Multiple(() =>
        {
            Assert.That(heldUninitialised, Is.True, "mInit does not hold: absence never means closed");
            Assert.That(leafBefore, Is.False, "the leaf does not adopt an unset shard flag");
            Assert.That(advanced, Is.True);
            Assert.That(leafAfter, Is.True, "the leaf adopted its shard's flag durably (rule 2)");
        });
    }

    [Test]
    public async Task A_leaf_with_a_split_in_flight_does_not_adopt_its_shards_flag()
    {
        var rig = new LiveAdvanceRig();
        rig.ShardMarker = new ShardHandOffState { Initialised = true };
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, liveAdvance: rig);
        await SetStampedAsync(grain, "a", "1");
        state.State.SplitInFlight = true;

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);
        Assert.That((await rig.LeafMarkerAsync()).Initialised, Is.False);
    }

    [TestCase(false, false, TestName = "A_leaf_descent_does_not_reach_holds_the_advance")]
    [TestCase(true, true, TestName = "A_leaf_on_a_shard_repairing_orphans_holds_the_advance")]
    public async Task A_suspected_orphan_holds_the_advance(bool reachable, bool repairing)
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        rig.Reachable = reachable;
        rig.OrphanRepairInProgress = repairing;

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);
    }

    [TestCase((int)GrainStorageFencingVerdict.Unfenced)]
    [TestCase((int)GrainStorageFencingVerdict.Inconclusive)]
    public async Task A_storage_provider_not_found_fenced_holds_the_advance(int verdict)
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        rig.FencingCheck.LastResult = new GrainStorageFencingProbeResult((GrainStorageFencingVerdict)verdict, "test");

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False,
            "the claim's conditional writes prove nothing without enforced ETags");
    }

    [Test]
    public async Task A_disabled_fencing_probe_holds_the_advance()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        rig.FencingCheck.LastResult = null;

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);
    }

    [Test]
    public async Task A_slot_less_leaf_holds_the_advance()
    {
        var rig = new LiveAdvanceRig();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, liveAdvance: rig);
        await SetStampedAsync(grain, "a", "1");
        state.State.ShardIndex = null;

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False,
            "a slot-less legacy leaf owns every shard's entries in replay");
    }

    [Test]
    public async Task A_rangeless_leaf_with_siblings_holds_the_advance()
    {
        var rig = new LiveAdvanceRig();
        var state = new FakePersistentState<LeafNodeState>();
        state.State.NextSibling = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var grain = CreateGrain(state, liveAdvance: rig);
        await SetStampedAsync(grain, "a", "1");

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False,
            "a legacy bulk leaf carries no ownership range");
    }

    [Test]
    public async Task A_stale_latched_partition_never_live_advances()
    {
        // A partition whose WAL fell past its checkpoint with no snapshot covering the
        // gap is latched stale (#3477): only an apply or an operator reset may move it.
        var decision = FallOffLogDecision.TailReplay;
        var rig = new LiveAdvanceRig(firstOffset: 10);
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            treeId: UniqueCoverageRepairTreeId("live-advance-latched"),
            detectorDecision: () => decision,
            liveAdvance: rig);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        decision = FallOffLogDecision.SnapshotThenWal;
        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
        Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True, "precondition: latched");

        await SetStampedAsync(grain, "a", "1");

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(4L),
            "the leaf's own append at 10 is not evidence that it holds (4, 10)");
    }

    [Test]
    public async Task A_second_advance_while_one_is_deciding_returns_without_reading()
    {
        // The tick and the starvation drive run on interleaving turns; an advance decides
        // on a reused snapshot across awaits, so a second one must not overlap it.
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        var view = new TaskCompletionSource<LeafLiveAdvanceView>();
        rig.ShardRoot.GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>()).Returns(view.Task);
        rig.ShardRoot.ClearReceivedCalls();

        var first = grain.TryAdvanceLiveCheckpointAsync();
        var second = grain.TryAdvanceLiveCheckpointAsync();
        await WaitUntilAsync(() => rig.ShardRoot.ReceivedCalls().Any());
        var secondDeclinedAtOnce = second.IsCompleted && !second.Result;
        view.SetResult(LeafLiveAdvanceView.For(rig.ShardMarker, null, null, reachable: true, orphanRepairInProgress: false));
        await Task.WhenAll(first, second);

        var firstAdvanced = await first;
        Assert.Multiple(() =>
        {
            Assert.That(secondDeclinedAtOnce, Is.True, "the second advance returns without waiting on the first's read");
            Assert.That(firstAdvanced, Is.True);
            Assert.That(rig.ShardRoot.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.GetLiveAdvanceViewAsync)), Is.EqualTo(1), "only the first advance read the shard root's view");
        });
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (var i = 0; i < 200 && !condition(); i++)
        {
            await Task.Delay(10);
        }

        Assert.That(condition(), Is.True, "the condition was not reached in time");
    }

    [Test]
    public async Task A_closed_gate_view_holds_the_advance()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        rig.FenceGate.IsOpen = false;
        rig.ShardRoot.ClearReceivedCalls();

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);
        await rig.ShardRoot.DidNotReceive().GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>());
    }

    [Test]
    public async Task A_hand_off_close_bump_holds_the_advance_until_a_replay_reaches_the_new_claim_head()
    {
        // N3: a leaf initiator's own close re-claims under the raised epoch, so its
        // claim head moves and it re-arms only after reading to it.
        var (grain, rig) = await ArmedLeafWithWritesAsync("a", "b");
        rig.ClaimHead = 2;
        await grain.AppendFenceForTesting!.BumpAndReclaimAsync(_ => rig.Shard, rig.Partitions, CancellationToken.None);
        rig.Writer.ForeignAppend();
        rig.ClaimHead = 3;
        await SetStampedAsync(grain, "c", "3");

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);

        await grain.SetCheckpointOffsetHintsAsync([2]);
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L));
    }

    [Test]
    public async Task A_live_advance_waits_until_every_issued_append_is_applied()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");

        // The second write's append has been issued (offset 1) but has not returned,
        // so it is not applied yet.
        var hold = new TaskCompletionSource();
        rig.Writer.HoldNext = hold;
        var pending = SetStampedAsync(grain, "b", "2");

        Assert.That(grain.LiveAppendsUnappliedForTest, Is.EqualTo(1), "precondition: one own append is in flight.");
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False,
            "an advance while an own append is unapplied would claim an offset the leaf may never hold.");

        hold.SetResult();
        await pending;

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(1L),
            "once every issued append is applied, the advance covers all of them.");
    }

    [Test]
    public async Task A_failed_append_turns_the_live_advance_off_for_the_activation()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync();
        rig.Writer.ThrowNext = new TimeoutException("the append's outcome is unknown");
        Assert.ThrowsAsync<TimeoutException>(async () => await SetStampedAsync(grain, "a", "1"));
        await SetStampedAsync(grain, "b", "2");

        var unapplied = grain.LiveAppendsUnappliedForTest;
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(unapplied, Is.EqualTo(1), "the failed append is never released, so the count cannot return to zero.");
            Assert.That(advanced, Is.False,
                "an append that may have landed without being applied must hold the advance off; the leaf's replay remains the remedy.");
        });
    }

    [Test]
    public async Task A_pending_saga_prepare_holds_the_live_advance()
    {
        var (grain, _) = await ArmedLeafWithWritesAsync("a");
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await SetStampedAsync(grain, "b", "2");
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        var pendingCount = grain.PendingTransactionCount;
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(pendingCount, Is.EqualTo(1), "precondition: a prepare is pending.");
            Assert.That(advanced, Is.False,
                "a saga's terminal lands on the partition its shard index names, so the advance holds on every partition while a prepare is pending.");
        });
    }

    [Test]
    public async Task Each_partition_advances_to_its_own_highest_offset()
    {
        var rig = new LiveAdvanceRig(partitions: 2);
        var grain = CreateGrain(liveAdvance: rig);
        await SetStampedAsync(grain, KeyOnPartition(0, 2, "p0-"), "1");
        await SetStampedAsync(grain, KeyOnPartition(1, 2, "p1-"), "1");
        await SetStampedAsync(grain, KeyOnPartition(1, 2, "p1b-"), "2");

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(0L));
            Assert.That(grain.GetCurrentCheckpointForPartition(1), Is.EqualTo(1001L),
                "partition 1's offsets start at 1000 in the rig: each partition advances over its own appends only");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var treeId = UniqueCoverageRepairTreeId("live-advance-tick");
            var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
            coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
            coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
            coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

            var published = new List<MaterialiserPinReport>();
            var reporter = Substitute.For<ILeafCursorReporter>();
            reporter.FlushDurableMaterialiserFrontierAsync(
                    Arg.Any<string>(),
                    Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                    Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    published.AddRange(call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1));
                    return Task.FromResult(true);
                });

            var rig = new LiveAdvanceRig();
            var (grain, _, _, saved) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                reClassifyEveryN: 1000,
                treeId: treeId,
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: coordinator,
                cursorReporter: reporter,
                liveAdvance: rig);

            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            await SetStampedAsync(grain, "a", "1");
            await SetStampedAsync(grain, "b", "2");

            Assert.Multiple(() =>
            {
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L),
                    "precondition: the live writes left the checkpoint unset.");
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
                    "precondition: and no snapshot covers them, so the leaf's pin is the block.");
            });

            coordinator.ClearReceivedCalls();
            published.Clear();
            var starvedDeclines = await CountStarvedArmDeclinesAsync(
                () => grain.OnCoverageLagTimerTickAsync(CancellationToken.None));

            await coordinator.DidNotReceive().ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
            Assert.Multiple(() =>
            {
                Assert.That(starvedDeclines, Is.Zero,
                    "the tick itself makes the leaf's pin usable: it never takes the starved arm, whose drive needs a replay permit (#5569, #5570).");
                Assert.That(grain.InterleavedTimerDrivePendingForTest, Is.False, "no starvation drive was dispatched.");
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(1L),
                    "the tick advances the checkpoint over the leaf's own writes without reading the WAL.");
                Assert.That(saved, Is.Not.Empty, "a snapshot is captured over the advanced checkpoint.");
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(1L));
                Assert.That(published.Select(r => r.CheckpointOffset).DefaultIfEmpty(-1L).Max(), Is.EqualTo(1L),
                    "the pin on the wire carries a usable offset after one tick and no replay, which is what lets a freshly written tree's WAL floor advance (#5570).");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    /// <summary>
    /// Runs <paramref name="tick"/> and counts the coverage-lag tick's starved-arm
    /// declines, each of which dispatches a starvation drive that needs a replay
    /// permit (issues #5569, #5570). The fixtures that use it are non-parallelizable.
    /// </summary>
    private static async Task<int> CountStarvedArmDeclinesAsync(Func<Task> tick)
    {
        var starved = 0;
        using (MeterListening.StartForInstrument(LatticeMetrics.LeafSnapshotDriverDeclines, listener =>
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Key
                        && Equals(tag.Value, LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value))
                    {
                        Interlocked.Add(ref starved, (int)value);
                    }
                }
            })))
        {
            await tick();
        }

        return starved;
    }

    /// <summary>A key the production hash routes to <paramref name="partition"/> of <paramref name="partitions"/>.</summary>
    private static string KeyOnPartition(int partition, int partitions, string prefix)
    {
        for (var i = 0; ; i++)
        {
            var key = $"{prefix}{i}";
            if (WalPartitionHash.Compute(key, partitions) == partition)
            {
                return key;
            }
        }
    }
}
