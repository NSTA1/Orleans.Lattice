using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5572: a leaf advances its projection checkpoint over the appends it
/// issued itself, without a WAL replay, whenever that is safe
/// (<c>BPlusLeafGrain.LiveCheckpoint.cs</c>; <c>LiveAdvance</c> in
/// <c>WalDurability.tla</c>).
/// <para>
/// Before the change a live write never moved the checkpoint, so a live-written
/// leaf published the block pin from birth and its first checkpoint came only
/// from a replay of every partition from <c>-1</c>; on a freshly written estate
/// that held every tree's WAL floor (issue #5570). The safety fixtures pin each
/// condition the advance waits for: every own append applied, none failed, no
/// saga prepare pending, and either a birth through a create path or a replay
/// that has already reached the activation's first own append.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly LatticeOptions OnePartition = new() { WalPartitions = 1 };

    /// <summary>
    /// A commit-log writer whose offsets start at <paramref name="firstOffset"/>
    /// and whose appends can be held open, so a test can leave an own append
    /// issued but not yet returned.
    /// </summary>
    private sealed class GatedCommitLogWriter(long firstOffset = 0) : ICommitLogWriter
    {
        private long _next = firstOffset;

        /// <summary>When set, the next append waits on it before returning.</summary>
        public TaskCompletionSource? HoldNext { get; set; }

        /// <summary>When set, the next append throws it.</summary>
        public Exception? ThrowNext { get; set; }

        public async Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            if (ThrowNext is { } fault)
            {
                ThrowNext = null;
                throw fault;
            }

            var offset = _next++;
            if (HoldNext is { } hold)
            {
                HoldNext = null;
                await hold.Task;
            }

            return offset;
        }

        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
        {
            var offsets = new long[entries.Count];
            for (var i = 0; i < offsets.Length; i++)
            {
                offsets[i] = _next++;
            }

            return Task.FromResult<IReadOnlyList<long>>(offsets);
        }
    }

    [Test]
    public async Task A_live_written_newborn_leaf_advances_its_checkpoint_without_a_replay()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var writer = new GatedCommitLogWriter();
        var grain = CreateGrain(state, options: OnePartition, commitLog: writer);

        // Bound through the birth seam, as a create path binds a new leaf.
        state.State.TreeId = null;
        await grain.SetTreeIdAsync("live-advance-tree");

        await grain.SetAsync("a", Utf8("1"));
        await grain.SetAsync("b", Utf8("2"));
        await grain.SetAsync("c", Utf8("3"));

        Assert.Multiple(() =>
        {
            Assert.That(grain.LiveAdvanceBornForTest, Is.True, "precondition: the binding was a birth.");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L),
                "precondition: a live write does not itself move the checkpoint.");
        });

        var advanced = await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(2L),
                "the checkpoint reaches the highest offset the leaf's own applied appends returned, "
                    + "with no WAL read: before issue #5572 only a replay from -1 could move it.");
        });
    }

    [Test]
    public async Task A_live_advance_waits_until_every_issued_append_is_applied()
    {
        var writer = new GatedCommitLogWriter();
        var grain = CreateGrain(options: OnePartition, commitLog: writer);

        await grain.SetAsync("a", Utf8("1"));

        // The second write's append has been issued (offset 1) but has not
        // returned, so it is not applied yet.
        var hold = new TaskCompletionSource();
        writer.HoldNext = hold;
        var pending = grain.SetAsync("b", Utf8("2"));

        Assert.That(grain.LiveAppendsUnappliedForTest, Is.EqualTo(1), "precondition: one own append is in flight.");
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False,
            "an advance while an own append is unapplied would claim an offset the leaf may never hold.");
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));

        hold.SetResult();
        await pending;

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(1L),
            "once every issued append is applied, the advance covers all of them.");
    }

    [Test]
    public async Task A_failed_apply_turns_the_live_advance_off_for_the_activation()
    {
        var writer = new GatedCommitLogWriter();
        var grain = CreateGrain(options: OnePartition, commitLog: writer);

        writer.ThrowNext = new TimeoutException("the append's outcome is unknown");
        Assert.ThrowsAsync<TimeoutException>(async () => await grain.SetAsync("a", Utf8("1")));
        await grain.SetAsync("b", Utf8("2"));

        var unapplied = grain.LiveAppendsUnappliedForTest;
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(unapplied, Is.EqualTo(1),
                "the failed append is never released, so the count cannot return to zero.");
            Assert.That(advanced, Is.False,
                "an append that may have landed without being applied must hold the advance off; the "
                    + "leaf's replay remains the remedy.");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));
        });
    }

    [Test]
    public async Task A_pending_saga_prepare_holds_the_live_advance()
    {
        var writer = new GatedCommitLogWriter();
        var grain = CreateGrain(options: OnePartition, commitLog: writer);

        await grain.SetAsync("a", Utf8("1"));
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await grain.SetAsync("b", Utf8("2"));
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        var pendingCount = grain.PendingTransactionCount;
        var unapplied = grain.LiveAppendsUnappliedForTest;
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(pendingCount, Is.EqualTo(1), "precondition: a prepare is pending.");
            Assert.That(unapplied, Is.Zero, "precondition: every append is applied.");
            Assert.That(advanced, Is.False,
                "a saga's terminal lands on the partition its shard index names, not the prepare's, so "
                    + "the advance must hold on every partition while a prepare is pending.");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));
        });
    }

    [Test]
    public async Task A_reactivated_leaf_does_not_live_advance_past_entries_it_has_not_read()
    {
        // Offsets 0..4 were appended before this activation: the leaf's own
        // appends now start at 5, and nothing has read 0..4 yet.
        var writer = new GatedCommitLogWriter(firstOffset: 5);
        var grain = CreateGrain(options: OnePartition, commitLog: writer);

        await grain.SetAsync("a", Utf8("1"));
        await grain.SetAsync("b", Utf8("2"));

        var born = grain.LiveAdvanceBornForTest;
        var advancedEarly = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(born, Is.False, "precondition: this activation is not a birth.");
            Assert.That(advancedEarly, Is.False,
                "entries below the activation's first own append may be the leaf's own from an earlier "
                    + "activation; passing them unread would lose them.");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));
        });

        // A replay has read through offset 4 (here, the split hint seam).
        await grain.SetCheckpointOffsetHintsAsync(new long[] { 4 });

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(6L),
            "once the read position reaches the first own append, the advance covers the rest.");
    }

    [Test]
    public async Task A_leaf_re_created_from_a_purge_is_not_born()
    {
        var purgeCleared = new FakeRowRecord
        {
            Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId, PurgeCleared = true },
        };
        var (recreated, _, _) = CreateRowlessLeafWithSnapshotStore(purgeCleared);
        await LeafActivationHarness.ActivateAsync(recreated, CancellationToken.None);
        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(recreated)))
        {
            await recreated.SetTreeIdAsync(MaterialiserTreeId);
        }

        var (created, _) = CreateRowlessLeaf(new FakeRowRecord());
        await LeafActivationHarness.ActivateAsync(created, CancellationToken.None);
        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(created)))
        {
            await created.SetTreeIdAsync(MaterialiserTreeId);
        }

        Assert.Multiple(() =>
        {
            Assert.That(recreated.LiveAdvanceBornForTest, Is.False,
                "a leaf re-created from a purge's clear may still have readable purged entries, so its "
                    + "binding is not a birth: it must read past them, not skip them.");
            Assert.That(created.LiveAdvanceBornForTest, Is.True,
                "control: a leaf a create path names is born.");
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

            var (grain, _, _, saved) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                reClassifyEveryN: 1000,
                treeId: treeId,
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: coordinator,
                commitLogWriter: new GatedCommitLogWriter(),
                cursorReporter: reporter);

            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            await grain.SetAsync("a", Utf8("1"));
            await grain.SetAsync("b", Utf8("2"));

            Assert.Multiple(() =>
            {
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L),
                    "precondition: the live writes left the checkpoint unset.");
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
                    "precondition: and no snapshot covers them, so the leaf's pin is the block.");
            });

            coordinator.ClearReceivedCalls();
            published.Clear();
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

            await coordinator.DidNotReceive().ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
            Assert.Multiple(() =>
            {
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(1L),
                    "the tick advances the checkpoint over the leaf's own writes without reading the WAL.");
                Assert.That(saved, Is.Not.Empty, "a snapshot is captured over the advanced checkpoint.");
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(1L));
                Assert.That(published.Select(r => r.CheckpointOffset).DefaultIfEmpty(-1L).Max(), Is.EqualTo(1L),
                    "THE assertion: the pin on the wire carries a usable offset after one tick and no replay, "
                        + "which is what lets a freshly written tree's WAL floor advance.");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
