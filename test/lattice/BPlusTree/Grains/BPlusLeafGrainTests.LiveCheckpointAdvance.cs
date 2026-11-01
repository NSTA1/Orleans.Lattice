using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Storage;

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
/// saga prepare pending, and either a birth (this activation created the leaf,
/// issue #5593) or a replay that has already reached the activation's first own
/// append.
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

    /// <summary>
    /// A commit-log writer that routes each record to a WAL partition as the
    /// production writer does - a saga terminal by the shard index its key
    /// carries, every other record by the hash of its key - and hands out
    /// offsets per partition, starting partition <c>p</c> at
    /// <c>p * <paramref name="stride"/></c> so an offset names its partition.
    /// </summary>
    private sealed class PartitionedCommitLogWriter(int partitions, long stride = 100) : ICommitLogWriter
    {
        private readonly long[] _next = Enumerable.Range(0, partitions).Select(p => p * stride).ToArray();

        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
            => Task.FromResult(_next[Route(entry)]++);

        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<long>>(entries.Select(e => _next[Route(e)]++).ToArray());

        private int Route(WalRecord entry)
            => entry.Op is MutationKind.TxCommit or MutationKind.TxAbort && int.TryParse(entry.Key, out var shard)
                ? shard % partitions
                : WalPartitionHash.Compute(entry.Key ?? string.Empty, partitions);
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

    /// <summary>
    /// A leaf over a row that does not exist yet, bound under a create intent naming
    /// it: the create path of a shard's root leaf. The id is not Guid-keyed, so no
    /// row record is consulted and the admission finds no earlier row.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State)> CreateBornLeafAsync(
        ICommitLogWriter writer, LatticeOptions options, Action<FakePersistentState<LeafNodeState>>? beforeBind = null)
    {
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        var grain = CreateGrain(state, options: options, commitLog: writer);
        state.State.TreeId = null;
        beforeBind?.Invoke(state);
        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            await grain.SetTreeIdAsync("live-advance-tree");
        }

        return (grain, state);
    }

    [Test]
    public async Task A_live_written_newborn_leaf_advances_its_checkpoint_without_a_replay()
    {
        // Offsets 0..6 belong to other leaves of the shard: only the birth, not the
        // arming rule, lets this leaf pass them unread.
        var writer = new GatedCommitLogWriter(firstOffset: 7);
        var (grain, _) = await CreateBornLeafAsync(writer, OnePartition);

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
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(9L),
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
    public async Task A_failed_append_turns_the_live_advance_off_for_the_activation()
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
            var starvedDeclines = await CountStarvedArmDeclinesAsync(
                () => grain.OnCoverageLagTimerTickAsync(CancellationToken.None));

            await coordinator.DidNotReceive().ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
            Assert.Multiple(() =>
            {
                Assert.That(starvedDeclines, Is.Zero,
                    "the tick itself makes the leaf's pin usable: it never takes the starved arm, whose "
                        + "drive needs a replay permit - the scarce resource of issues #5569 and #5570.");
                Assert.That(grain.InterleavedTimerDrivePendingForTest, Is.False, "no starvation drive was dispatched.");
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
    [Test]
    public async Task An_activation_that_adopts_a_concurrently_created_row_is_not_born()
    {
        // Issue #5593. A duplicate activation of the same deterministic-id leaf won
        // the first-create race, so this activation's first write adopts the
        // winner's row (#1557). The winner's writes are in the WAL and this
        // activation never read them: it did not create the leaf.
        var (adopter, _) = await CreateBornLeafAsync(new GatedCommitLogWriter(), OnePartition, state =>
        {
            state.ThrowOnWrite = new InconsistentStateException(
                "Version conflict (WriteState): ETag=. Expected Etag= Received Etag=",
                storedEtag: string.Empty,
                currentEtag: string.Empty);
            state.OnReadState = s =>
            {
                s.RecordExistsValue = true;
                s.State.TreeId = "live-advance-tree";
            };
        });
        var (creator, _) = await CreateBornLeafAsync(new GatedCommitLogWriter(), OnePartition);

        Assert.Multiple(() =>
        {
            Assert.That(adopter.LiveAdvanceBornForTest, Is.False,
                "an adopted row was created by another activation, whose acknowledged writes this one "
                    + "never applied; a birth would let its live advance pass them.");
            Assert.That(creator.LiveAdvanceBornForTest, Is.True, "control: the activation whose write created the row is born.");
        });
    }

    [Test]
    public async Task An_activation_admitted_by_adopting_a_recorded_row_is_not_born()
    {
        // Issue #5593. The duplicate that lost the create race sees the winner's row
        // record, re-reads and adopts the winner's row at admission (#1557).
        var recorded = new FakeRowRecord
        {
            Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId },
        };
        var (adopter, state) = CreateRowlessLeaf(recorded);
        state.OnReadState = s => s.RecordExistsValue = true;
        await LeafActivationHarness.ActivateAsync(adopter, CancellationToken.None);
        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(adopter)))
        {
            await adopter.SetTreeIdAsync(MaterialiserTreeId);
        }

        Assert.That(adopter.LiveAdvanceBornForTest, Is.False,
            "a create intent admitted over another activation's row is not a birth.");
    }

    [Test]
    public async Task A_re_bind_of_an_unbound_leaf_is_not_born()
    {
        // Issue #5593. The #1744 write-path re-bind (the CRDT self-heal) binds an
        // existing leaf whose row has no tree id. It carries no create intent: the
        // leaf existed before this activation, and its entries may be in the WAL.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: OnePartition, commitLog: new GatedCommitLogWriter());
        state.State.TreeId = null;

        await grain.SetBindingAsync("live-advance-tree", 0);

        Assert.That(grain.LiveAdvanceBornForTest, Is.False,
            "a re-bind of an existing unbound leaf is not a birth.");
    }

    [Test]
    public async Task Recovering_the_binding_of_an_unbound_row_is_not_born()
    {
        // Issue #5593. RecoverBindingAsync over a surviving row with no tree id
        // binds an existing leaf; nothing created it in this activation.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: OnePartition, commitLog: new GatedCommitLogWriter());
        state.State.TreeId = null;

        await grain.RecoverBindingAsync("live-advance-tree", 0);

        Assert.That(grain.LiveAdvanceBornForTest, Is.False,
            "recovering the binding of an existing leaf is not a birth.");
    }

    [Test]
    public async Task A_bulk_loaded_leaf_is_born_and_a_split_sibling_is_not()
    {
        // Issue #5593. A bulk load or append graft creates its leaves through
        // InitializeSiblingAsync; their data arrives only through their own appends.
        // A split sibling's range was its donor's, whose entries are in the WAL.
        async Task<BPlusLeafGrain> InitializeAsync(bool bulkBirth)
        {
            var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
            var grain = CreateGrain(state, options: OnePartition, commitLog: new GatedCommitLogWriter());
            state.State.TreeId = null;
            using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
            {
                await grain.InitializeSiblingAsync(new SiblingInitialization
                {
                    TreeId = "live-advance-tree",
                    ShardIndex = 0,
                    LowKeyInclusive = bulkBirth ? null : "m",
                    BulkBirth = bulkBirth,
                });
            }

            return grain;
        }

        var bulk = await InitializeAsync(bulkBirth: true);
        var sibling = await InitializeAsync(bulkBirth: false);

        Assert.Multiple(() =>
        {
            Assert.That(bulk.LiveAdvanceBornForTest, Is.True,
                "a bulk-loaded leaf is created by this activation, so its live advance needs no replay from -1.");
            Assert.That(sibling.LiveAdvanceBornForTest, Is.False, "a split sibling is never born.");
        });
    }

    [Test]
    public async Task A_bulk_loaded_leaf_advances_past_other_leaves_offsets_without_a_replay()
    {
        // Issue #5593: the live advance reaches a bulk-loaded leaf. Its first own
        // offset is 40, past the shard's other entries, so it is not armed: only its
        // birth lets it advance without a replay from -1.
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        var grain = CreateGrain(state, options: OnePartition, commitLog: new GatedCommitLogWriter(firstOffset: 40));
        state.State.TreeId = null;
        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            await grain.InitializeSiblingAsync(new SiblingInitialization
            {
                TreeId = "live-advance-tree",
                ShardIndex = 0,
                BulkBirth = true,
            });
        }

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["a"] = LwwValue<byte[]>.Create(Utf8("1"), HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            ["b"] = LwwValue<byte[]>.Create(Utf8("2"), HybridLogicalClock.Tick(HybridLogicalClock.Tick(HybridLogicalClock.Zero))),
        });

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(41L),
            "the bulk leaf's checkpoint reaches its own highest offset with no WAL read.");
    }

    [Test]
    public async Task Each_partition_advances_to_its_own_highest_offset()
    {
        // Issue #5593 (review S2). Writes hash to both partitions; partition p hands
        // out offsets from p * 100, so an offset attributed to the wrong partition
        // shows as a checkpoint past what that partition holds.
        var options = new LatticeOptions { WalPartitions = 2 };
        var (grain, _) = await CreateBornLeafAsync(new PartitionedCommitLogWriter(2), options);
        await grain.SetAsync(KeyOnPartition(0, 2, "p0-"), Utf8("1"));
        await grain.SetAsync(KeyOnPartition(0, 2, "q0-"), Utf8("2"));
        await grain.SetAsync(KeyOnPartition(1, 2, "p1-"), Utf8("3"));

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(1L),
                "partition 0 holds offsets 0 and 1 only.");
            Assert.That(grain.GetCurrentCheckpointForPartition(1), Is.EqualTo(100L),
                "partition 1 holds offset 100 only.");
        });
    }

    [Test]
    public async Task A_prepare_pending_on_one_partition_holds_every_partition()
    {
        // Issue #5593 (review S2). The prepare's key routes to partition 0, but its
        // terminal is appended to the partition its shard index names, so the clamp
        // must hold partition 1 as well.
        var options = new LatticeOptions { WalPartitions = 2 };
        var (grain, _) = await CreateBornLeafAsync(new PartitionedCommitLogWriter(2), options);
        await grain.SetAsync(KeyOnPartition(1, 2, "live-"), Utf8("1"));
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await grain.SetAsync(KeyOnPartition(0, 2, "prepared-"), Utf8("2"));
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        var pending = grain.PendingTransactionCount;
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(pending, Is.EqualTo(1), "precondition: a prepare is pending on partition 0.");
            Assert.That(advanced, Is.False);
            Assert.That(grain.GetCurrentCheckpointForPartition(1), Is.EqualTo(-1L),
                "partition 1 holds no prepare, but the terminal may land there: it must not advance.");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task The_coverage_lag_tick_makes_a_born_bulk_leafs_pin_usable_without_a_replay()
    {
        // Issue #5593: a born leaf end to end. Its first own offset is 25, so the
        // arming rule cannot advance it; the birth does, on the tick, with no drive.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var treeId = UniqueCoverageRepairTreeId("live-advance-bulk");
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

            var (grain, state, _, saved) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                reClassifyEveryN: 1000,
                treeId: treeId,
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: coordinator,
                commitLogWriter: new GatedCommitLogWriter(firstOffset: 25),
                cursorReporter: reporter);
            state.State.TreeId = null;
            state.RecordExistsValue = false;

            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
            {
                await grain.InitializeSiblingAsync(new SiblingInitialization
                {
                    TreeId = treeId,
                    ShardIndex = 0,
                    BulkBirth = true,
                });
            }

            await grain.SetAsync("a", Utf8("1"));
            await grain.SetAsync("b", Utf8("2"));
            Assert.That(grain.LiveAdvanceBornForTest, Is.True, "precondition: the bulk leaf is born.");

            coordinator.ClearReceivedCalls();
            published.Clear();
            var starvedDeclines = await CountStarvedArmDeclinesAsync(
                () => grain.OnCoverageLagTimerTickAsync(CancellationToken.None));

            await coordinator.DidNotReceive().ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
            Assert.Multiple(() =>
            {
                Assert.That(starvedDeclines, Is.Zero, "the tick never takes the starved arm, whose drive needs a replay permit.");
                Assert.That(grain.InterleavedTimerDrivePendingForTest, Is.False, "no starvation drive was dispatched.");
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(26L));
                Assert.That(saved, Is.Not.Empty, "a snapshot is captured over the advanced checkpoint.");
                Assert.That(published.Select(r => r.CheckpointOffset).DefaultIfEmpty(-1L).Max(), Is.EqualTo(26L),
                    "the born leaf's pin on the wire carries a usable offset after one tick and no replay.");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
