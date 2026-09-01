using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the recovering incremental-flush ceiling
/// (issue #1831). The resumable-replay flush added by #1513 clamped the
/// per-slice checkpoint advance below <c>lowestDeferredOffset</c>, a scalar
/// that was only ever lowered - so the FIRST deferred mutation
/// (<see cref="MutationKind.DeleteRange"/> / <see cref="MutationKind.TxCommit"/> /
/// <see cref="MutationKind.TxAbort"/>) in a partition pinned the ceiling for
/// the whole remainder of the replay. All further durable progress then
/// depended on the post-pass-2 reconciliation, which only runs when the
/// entire replay finishes inside the activation window; for a backlog large
/// enough to outrun that window it never does, so successive activations
/// replayed the identical range forever.
/// <para>
/// The fix tracks deferred offsets in a resolvable per-partition ledger and
/// drains a deferred terminal during pass 1 whenever that is provably safe -
/// which it is once every OTHER partition has completed its pass-1 absorb
/// sweep, because the terminal's cross-partition dependencies (a saga's
/// prepares, a range delete's target rows) are then all in the leaf's cache.
/// Both existing safety clamps are preserved exactly: the ceiling still
/// never passes an undrained deferred offset, and never passes an unresolved
/// saga prepare.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string FlushCeilingTreeId = "tree-flush-ceiling";

    /// <summary>Stable Guid leaf key so the snapshot storage grain resolves via <c>GetGuidKey()</c>.</summary>
    private static readonly Guid FlushCeilingLeafKey = Guid.Parse("22222222-2222-2222-2222-222222222222");

    /// <summary>
    /// Builds a coordinator serving <paramref name="entries"/> in slices of at
    /// most <paramref name="sliceSize"/> records and invoking
    /// <paramref name="onRead"/> with the 1-based read ordinal before each
    /// slice is returned. The callback is the deterministic cancellation seam:
    /// a test cancels at an exact slice boundary rather than racing a timer.
    /// </summary>
    private static ILeafReplayCoordinatorGrain BuildObservableCoordinator(
        long head,
        int sliceSize,
        long tail,
        Action<int>? onRead,
        params CommitLogSliceEntry[] entries)
    {
        var reads = 0;
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(tail));
        coord.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var fromExclusive = call.ArgAt<long>(0);
                var toInclusive = call.ArgAt<long>(1);
                var budget = call.ArgAt<int>(2);
                var cap = Math.Min(sliceSize, budget);
                var slice = new List<CommitLogSliceEntry>();
                foreach (var e in entries)
                {
                    if (e.Offset <= fromExclusive)
                        continue;
                    if (e.Offset > toInclusive)
                        break;
                    slice.Add(e);
                    if (slice.Count >= cap)
                        break;
                }

                onRead?.Invoke(++reads);
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice);
            });
        return coord;
    }

    /// <summary>
    /// Builds a leaf whose replay coordinator is resolved per WAL partition
    /// from <paramref name="coordinators"/> (indexed by partition), so a
    /// multi-partition replay exercises genuinely disjoint offset spaces.
    /// </summary>
    private static BPlusLeafGrain BuildFlushCeilingLeaf(
        FakePersistentState<LeafNodeState> state,
        ILeafReplayCoordinatorGrain[] coordinators,
        ILeafSnapshotStorageGrain snapshotStub,
        int reclassifyEveryN = 0)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>())
            .Returns(call =>
            {
                var key = call.ArgAt<string>(0);
                var slash = key.LastIndexOf('/');
                var partition = slash < 0 ? 0 : int.Parse(key[(slash + 1)..]);
                return coordinators[partition];
            });
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", FlushCeilingLeafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var baseOptions = new LatticeOptions
        {
            // Every-entry flush so each incremental SetCheckpointOffsetAsync
            // persists and the observable checkpoint tracks replay progress.
            MaterialiserCheckpointInterval = TimeSpan.Zero,
            LeafSnapshotReClassifyEveryNCheckpoints = reclassifyEveryN,
            WalPartitions = coordinators.Length,
        };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
    }

    private static FakePersistentState<LeafNodeState> NewFlushCeilingState()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = FlushCeilingTreeId;
        state.State.ProjectionCheckpointOffset = 0;
        return state;
    }

    private static CommitLogSliceEntry FlushSet(long offset, string key, long hlcPhysical = 100) =>
        new(offset, BuildCommittedSet(
            key,
            Encoding.UTF8.GetBytes($"v-{key}"),
            hlcPhysical: hlcPhysical,
            treeId: FlushCeilingTreeId));

    private static CommitLogSliceEntry FlushDeleteRange(long offset, long hlcPhysical = 500) =>
        new(offset, BuildDeleteRange("m0", "m9", hlcPhysical: hlcPhysical, treeId: FlushCeilingTreeId));

    /// <summary>
    /// Builds the 12-offset window whose FIRST record is a deferred
    /// <see cref="MutationKind.DeleteRange"/>: the exact shape that froze the
    /// ceiling at the persisted checkpoint for the whole replay.
    /// </summary>
    private static CommitLogSliceEntry[] WindowOpeningWithADeferredMutation()
    {
        var entries = new CommitLogSliceEntry[12];
        entries[0] = FlushDeleteRange(1);
        for (var i = 2; i <= 12; i++)
            entries[i - 1] = FlushSet(i, $"k{i:D2}");
        return entries;
    }

    [Test]
    public async Task Interrupted_replay_advances_checkpoint_when_window_opens_with_a_deferred_mutation()
    {
        // The regression shape from #1831: the replay window opens with a
        // deferred DeleteRange at offset 1 and is torn down part-way (after
        // the second slice is served). Before the fix the ceiling was pinned
        // at lowestDeferredOffset - 1 = 0 for the whole scan, so the
        // interrupted activation made ZERO durable progress and the next one
        // replayed the identical window. The first slice's contiguous prefix
        // must now be durable instead.
        using var cts = new CancellationTokenSource();
        var coord = BuildObservableCoordinator(
            head: 12,
            sliceSize: 4,
            tail: 0,
            onRead: read =>
            {
                if (read == 2)
                    cts.Cancel();
            },
            WindowOpeningWithADeferredMutation());

        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub);

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(cts.Token));

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L),
            "An interrupted replay whose window opens with a deferred mutation must still bank "
            + "the first slice's fully-applied prefix, not freeze at the persisted checkpoint.");
    }

    [Test]
    public async Task Successive_interrupted_replays_advance_the_checkpoint_until_convergence()
    {
        // Each activation is torn down after exactly one slice. The persisted
        // checkpoint must climb strictly on every attempt until the window is
        // fully absorbed - the livelock #1513 closed and #1831 reopened for
        // any tree that emits deferred mutations.
        var entries = WindowOpeningWithADeferredMutation();
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();

        var observed = new List<long>();
        for (var attempt = 0; attempt < 6; attempt++)
        {
            using var cts = new CancellationTokenSource();
            var coord = BuildObservableCoordinator(
                head: 12,
                sliceSize: 4,
                // The coverage-gated WAL GC trims the snapshot-covered prefix,
                // so the surviving tail tracks the durable checkpoint. That is
                // what lets the next activation rehydrate rather than restart.
                tail: state.State.ProjectionCheckpointOffset,
                onRead: read =>
                {
                    // Cancel as the SECOND slice is served, so exactly one
                    // slice is absorbed and flushed per attempt.
                    if (read == 2)
                        cts.Cancel();
                },
                entries);

            var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub, reclassifyEveryN: 1);
            try
            {
                await ((IGrainBase)grain).OnActivateAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected for every attempt that still had work to read.
            }

            observed.Add(state.State.ProjectionCheckpointOffset);
            if (state.State.ProjectionCheckpointOffset >= 12L)
                break;
        }

        Assert.That(observed, Is.Ordered.Ascending.And.Unique,
            "Each interrupted activation must advance the persisted checkpoint strictly.");
        Assert.That(observed[^1], Is.EqualTo(12L),
            "Successive interrupted activations must converge on the head of the window.");
    }

    [TestCase(1, TestName = "Deferred_mutation_at_the_start_of_the_window_does_not_pin_the_ceiling")]
    [TestCase(6, TestName = "Deferred_mutation_in_the_middle_of_the_window_does_not_pin_the_ceiling")]
    [TestCase(12, TestName = "Deferred_mutation_at_the_end_of_the_window_does_not_pin_the_ceiling")]
    public async Task Deferred_mutation_position_does_not_pin_the_incremental_ceiling(int deferredOffset)
    {
        // Wherever the deferred mutation lands, a single-partition replay can
        // apply it in place, so the per-slice flush must track the slice
        // boundaries (4, 8, 12) exactly. Before the fix the persisted sequence
        // collapsed to whatever prefix preceded the deferred offset.
        var entries = new CommitLogSliceEntry[12];
        for (var i = 1; i <= 12; i++)
        {
            entries[i - 1] = i == deferredOffset
                ? FlushDeleteRange(i)
                : FlushSet(i, $"k{i:D2}");
        }

        var coord = BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: null, entries);
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();

        var persistedOffsets = new List<long>();
        state.OnWriteState = s => persistedOffsets.Add(s.ProjectionCheckpointOffset);

        var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(persistedOffsets, Is.EqualTo(new long[] { 4, 8, 12 }),
            "The incremental flush must reach every slice boundary regardless of where the "
            + "deferred mutation lands in the window.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
    }

    [Test]
    public async Task Interleaved_sagas_release_the_ceiling_as_their_terminals_drain()
    {
        // Three overlapping sagas. The ceiling must rise as each terminal
        // drains and its prepare clamp lifts, rather than staying pinned at
        // the first terminal's offset for the whole replay.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        var tx3 = Guid.NewGuid();
        var entries = new[]
        {
            new CommitLogSliceEntry(1, BuildPreparedSet(tx1, "s1", Encoding.UTF8.GetBytes("v1"), treeId: FlushCeilingTreeId)),
            new CommitLogSliceEntry(2, BuildPreparedSet(tx2, "s2", Encoding.UTF8.GetBytes("v2"), treeId: FlushCeilingTreeId)),
            new CommitLogSliceEntry(3, BuildTerminal(tx1, committed: true, treeId: FlushCeilingTreeId)),
            new CommitLogSliceEntry(4, BuildPreparedSet(tx3, "s3", Encoding.UTF8.GetBytes("v3"), treeId: FlushCeilingTreeId)),
            new CommitLogSliceEntry(5, BuildTerminal(tx2, committed: true, treeId: FlushCeilingTreeId)),
            new CommitLogSliceEntry(6, BuildTerminal(tx3, committed: false, treeId: FlushCeilingTreeId)),
            FlushSet(7, "s7"),
            FlushSet(8, "s8"),
        };

        var coord = BuildObservableCoordinator(head: 8, sliceSize: 2, tail: 0, onRead: null, entries);
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();

        var persistedOffsets = new List<long>();
        state.OnWriteState = s => persistedOffsets.Add(s.ProjectionCheckpointOffset);

        var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(persistedOffsets, Does.Contain(6L),
            "Once every saga terminal has drained the ceiling must recover to the applied frontier.");
        Assert.That(persistedOffsets, Is.Ordered.Ascending,
            "The checkpoint must advance monotonically as terminals drain.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(8L));
        Assert.That(grain.PendingTransactionCount, Is.Zero, "Every saga terminated during the replay.");
        Assert.That(await grain.GetAsync("s1"), Is.Not.Null, "tx1 committed.");
        Assert.That(await grain.GetAsync("s2"), Is.Not.Null, "tx2 committed.");
        Assert.That(await grain.GetAsync("s3"), Is.Null, "tx3 aborted.");
    }

    [Test]
    public async Task Inline_drained_delete_range_matches_the_deferred_projection_outcome()
    {
        // Targets written BEFORE the range delete are tombstoned; a write
        // appended AFTER it (and therefore carrying a later HLC) survives.
        // That is the same post-state the deferred pass-2 apply produces for
        // a monotonically stamped WAL, so draining in place is not a silent
        // behavioural change.
        var entries = new[]
        {
            FlushSet(1, "m1", hlcPhysical: 100),
            FlushSet(2, "m2", hlcPhysical: 200),
            FlushDeleteRange(3, hlcPhysical: 300),
            FlushSet(4, "m4", hlcPhysical: 400),
            FlushSet(5, "z5", hlcPhysical: 500),
        };

        var coord = BuildObservableCoordinator(head: 5, sliceSize: 2, tail: 0, onRead: null, entries);
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(await grain.GetAsync("m1"), Is.Null, "in-range key written before the range delete is tombstoned");
        Assert.That(await grain.GetAsync("m2"), Is.Null, "in-range key written before the range delete is tombstoned");
        Assert.That(await grain.GetAsync("m4"), Is.Not.Null, "in-range key written after the range delete survives");
        Assert.That(await grain.GetAsync("z5"), Is.Not.Null, "out-of-range key is untouched");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L));
    }

    [Test]
    public async Task Cross_partition_delete_range_still_holds_the_incremental_ceiling()
    {
        // Two WAL partitions. Partition 0's DeleteRange at offset 3 targets a
        // key whose Set lives in partition 1, which has NOT been absorbed when
        // partition 0 is scanned - so the terminal must stay deferred and
        // partition 0's ceiling must stay clamped at 2 until pass 2 drains it.
        var p0 = BuildObservableCoordinator(
            head: 5,
            sliceSize: 2,
            tail: 0,
            onRead: null,
            FlushSet(1, "m1"),
            FlushSet(2, "m2"),
            FlushDeleteRange(3),
            FlushSet(4, "z4"),
            FlushSet(5, "z5"));

        var p1 = BuildObservableCoordinator(
            head: 3,
            sliceSize: 2,
            tail: 0,
            onRead: null,
            FlushSet(1, "m5"),
            FlushSet(2, "m6"),
            FlushSet(3, "z7"));

        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();

        var partitionZeroPersists = new List<long>();
        state.OnWriteState = s => partitionZeroPersists.Add(s.ProjectionCheckpointOffset);

        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(partitionZeroPersists, Does.Not.Contain(3L),
            "Partition 0 must never checkpoint AT a deferred cross-partition DeleteRange.");
        Assert.That(partitionZeroPersists, Does.Not.Contain(4L),
            "Partition 0 must not advance past the deferred DeleteRange before pass 2 applies it.");
        Assert.That(partitionZeroPersists, Does.Contain(2L),
            "The contiguous prefix below the deferred terminal must still flush incrementally.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L),
            "Post-pass-2 reconciliation advances partition 0 to its applied frontier.");

        // The cross-partition targets are tombstoned, proving the deferral
        // still orders the range delete after every partition's Sets.
        Assert.That(await grain.GetAsync("m5"), Is.Null);
        Assert.That(await grain.GetAsync("m6"), Is.Null);
        Assert.That(await grain.GetAsync("z7"), Is.Not.Null);
    }

    [Test]
    public async Task Unresolved_prepare_holds_the_ceiling_even_when_a_terminal_drains_in_place()
    {
        // A range delete at offset 3 drains in place (single partition), but
        // the saga prepare at offset 2 never terminates inside the window. The
        // prepare clamp must still pin the checkpoint at 1 - a resumed replay
        // has to re-read the prepare to rebuild the pending-tx bucket.
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            FlushSet(1, "q1"),
            new CommitLogSliceEntry(2, BuildPreparedSet(txId, "q2", Encoding.UTF8.GetBytes("v2"), treeId: FlushCeilingTreeId)),
            FlushDeleteRange(3),
            FlushSet(4, "q4"),
            FlushSet(5, "q5"),
        };

        var coord = BuildObservableCoordinator(head: 5, sliceSize: 2, tail: 0, onRead: null, entries);
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();

        var persistedOffsets = new List<long>();
        state.OnWriteState = s => persistedOffsets.Add(s.ProjectionCheckpointOffset);

        var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
        Assert.That(persistedOffsets, Is.All.LessThanOrEqualTo(1L),
            "No checkpoint persist may advance past the unresolved prepare at offset 2.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
            "The checkpoint stays clamped one below the open prepare offset.");
    }

    [TestCase(1, 4L)]
    [TestCase(2, 8L)]
    [TestCase(3, 12L)]
    public async Task Cancellation_at_each_slice_boundary_banks_the_absorbed_prefix(int cancelAfterRead, long expected)
    {
        // Whichever slice boundary the teardown lands on, the fully-applied
        // prefix up to that boundary must be durable.
        using var cts = new CancellationTokenSource();
        var coord = BuildObservableCoordinator(
            head: 12,
            sliceSize: 4,
            tail: 0,
            onRead: read =>
            {
                if (read == cancelAfterRead + 1)
                    cts.Cancel();
            },
            WindowOpeningWithADeferredMutation());

        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub);

        try
        {
            await ((IGrainBase)grain).OnActivateAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected whenever a further slice was still to be read.
        }

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(expected));
    }

    // -- deferred-offset ledger ------------------------------------------

    [Test]
    public void Ledger_reports_the_lowest_unresolved_offset_per_partition()
    {
        var ledger = new BPlusLeafGrain.DeferredOffsetLedger(3);

        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(long.MaxValue), "an empty partition has no clamp");

        ledger.Add(0, 10);
        ledger.Add(0, 20);
        ledger.Add(2, 5);

        Assert.Multiple(() =>
        {
            Assert.That(ledger.MinUnresolved(0), Is.EqualTo(10L));
            Assert.That(ledger.MinUnresolved(1), Is.EqualTo(long.MaxValue));
            Assert.That(ledger.MinUnresolved(2), Is.EqualTo(5L));
        });
    }

    [Test]
    public void Ledger_ceiling_rises_as_offsets_resolve()
    {
        var ledger = new BPlusLeafGrain.DeferredOffsetLedger(1);
        ledger.Add(0, 10);
        ledger.Add(0, 20);
        ledger.Add(0, 30);

        ledger.Resolve(0, 10);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(20L));

        ledger.Resolve(0, 20);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(30L));

        ledger.Resolve(0, 30);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(long.MaxValue));
    }

    [Test]
    public void Ledger_resolving_out_of_order_keeps_the_lowest_unresolved_offset()
    {
        var ledger = new BPlusLeafGrain.DeferredOffsetLedger(1);
        ledger.Add(0, 10);
        ledger.Add(0, 20);
        ledger.Add(0, 30);

        ledger.Resolve(0, 20);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(10L),
            "resolving a later offset must not lift the clamp off an earlier unresolved one");

        ledger.Resolve(0, 10);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(30L),
            "the head must skip the already-resolved middle entry");
    }

    [Test]
    public void Ledger_ignores_unknown_and_repeated_resolutions()
    {
        var ledger = new BPlusLeafGrain.DeferredOffsetLedger(2);
        ledger.Resolve(0, 99);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(long.MaxValue));

        ledger.Add(0, 7);
        ledger.Resolve(0, 99);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(7L), "an unknown offset must not advance the head");

        ledger.Resolve(0, 7);
        ledger.Resolve(0, 7);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(long.MaxValue));
        Assert.That(ledger.MinUnresolved(1), Is.EqualTo(long.MaxValue));
    }

    [Test]
    public void Ledger_accepts_offset_zero_without_treating_it_as_resolved()
    {
        // Offset 0 is a real WAL offset under the cold-replay override, and
        // the ledger's resolved marker must not collide with it.
        var ledger = new BPlusLeafGrain.DeferredOffsetLedger(1);
        ledger.Add(0, 0);
        ledger.Add(0, 1);

        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(0L));
        ledger.Resolve(0, 0);
        Assert.That(ledger.MinUnresolved(0), Is.EqualTo(1L));
    }

    /// <summary>
    /// Sink for the allocation probes below, so a measured loop cannot be
    /// optimised away as dead code. Never read for its value.
    /// </summary>
    private static long _ledgerAllocationProbeSink;

    /// <summary>
    /// Escape hatch for the battery test's deliberately-allocating loop.
    /// <b>Load-bearing: do not remove, and do not "simplify" the assignment
    /// away.</b> Storing the allocated reference into a static field is what
    /// makes the allocation genuinely escape, which is the only thing that
    /// forces the JIT to put it on the heap. See
    /// <see cref="Allocation_probe_detects_a_deliberately_allocating_loop"/>.
    /// </summary>
    private static object? _escapingAllocationSink;

    /// <summary>
    /// Runs the loop built by <paramref name="prepare"/> at
    /// <paramref name="baseIterations"/> and at twice that, and returns how many
    /// more bytes the larger loop allocated. Zero means the loop body is
    /// allocation-free; a body allocating <c>k</c> bytes per iteration grows by
    /// <c>baseIterations * k</c>, which no amount of ambient noise can hide.
    /// <para>
    /// This differential form is what makes an allocation probe safe in a
    /// shared test host, and an absolute <c>Is.Zero</c> against the counter is
    /// not. Tiered JIT can compile - or on-stack-replace - the measured loop
    /// partway through the measured window, and the counter attributes that
    /// one-off runtime cost to the loop; whether it lands inside the window
    /// depends on what the host has already compiled, so an unrelated fixture
    /// in the same batch can flip the result. Any such constant appears in both
    /// measurements and cancels in the difference.
    /// </para>
    /// <para>
    /// <paramref name="prepare"/> returns a ready-to-run closure so per-size
    /// setup (building a ledger to resolve, say) happens outside the measured
    /// window. The measurement is repeated and the smallest growth kept, then
    /// clamped at zero: allocation noise is strictly additive, so the minimum is
    /// the closest observation to the true cost, and a negative difference can
    /// only mean the smaller loop absorbed more noise than the larger one.
    /// Growth is measured on the per-thread counter, which is exact for these
    /// synchronous loops - unlike the process-wide counter, it cannot pick up
    /// another thread's allocation.
    /// </para>
    /// </summary>
    private static long AllocationGrowthBetweenLoopSizes(
        Func<int, Action> prepare,
        int baseIterations,
        int attempts = 5)
    {
        // Warm up at the LARGER size so tiering and on-stack replacement have
        // both settled before either measured window, whatever the surrounding
        // batch happened to compile first.
        prepare(baseIterations * 2)();

        var smallestGrowth = long.MaxValue;
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            var smallLoop = prepare(baseIterations);
            var largeLoop = prepare(baseIterations * 2);

            var mark = GC.GetAllocatedBytesForCurrentThread();
            smallLoop();
            var small = GC.GetAllocatedBytesForCurrentThread() - mark;

            mark = GC.GetAllocatedBytesForCurrentThread();
            largeLoop();
            var large = GC.GetAllocatedBytesForCurrentThread() - mark;

            var growth = large - small;
            if (growth < smallestGrowth)
                smallestGrowth = growth;
        }

        // Every attempt is kept rather than short-circuiting on the first
        // non-positive difference: a single noisy attempt on a loop that DOES
        // allocate would otherwise be reported as allocation-free, which is the
        // false negative the battery test exists to catch.
        return smallestGrowth > 0 ? smallestGrowth : 0;
    }

    [Test]
    public void Allocation_probe_detects_a_deliberately_allocating_loop()
    {
        // Smoke-detector battery test for AllocationGrowthBetweenLoopSizes
        // itself, in the spirit of the mojibake gate's own self-check. A
        // differential allocation probe that could never fail would silently
        // approve a regression, so prove it reports growth for a loop that
        // genuinely allocates once per iteration.
        //
        // THE ESCAPE IS LOAD-BEARING. Storing each allocation into the static
        // _escapingAllocationSink is the whole point: it is what makes the
        // reference outlive the iteration, and only an escaping allocation is
        // guaranteed to land on the heap. A non-escaping allocation of constant
        // size - `new long[1].Length`, say - is precisely what .NET 10's tier-1
        // escape analysis is designed to stack-allocate or elide outright, so
        // the loop would allocate nothing, this probe would truthfully report
        // zero growth, and the battery test would become the very false
        // negative it exists to prevent. That is not hypothetical: it is how
        // this test first failed, and it is the mirror image of the tiering
        // effect the differential probe above was written to survive (tier-1
        // ADDING a one-off cost inside the window, versus tier-1 REMOVING the
        // allocation under test). Do not replace this with a cheaper-looking
        // allocation that does not escape.
        var growth = AllocationGrowthBetweenLoopSizes(
            iterations => () =>
            {
                for (var i = 0; i < iterations; i++)
                    _escapingAllocationSink = new object();
            },
            baseIterations: 2048);

        Assert.That(_escapingAllocationSink, Is.Not.Null,
            "the measured loop must have run and stored an escaping allocation");
        Assert.That(growth, Is.GreaterThan(0L),
            "the probe must report growth for a loop that allocates per iteration, "
            + "or it cannot catch a real per-record regression");
    }

    [Test]
    public void Ledger_min_unresolved_does_not_allocate_per_query()
    {
        // The replay hot path queries the clamp once per slice and, for a
        // non-deferred record, does nothing else with the ledger. Neither may
        // allocate, or a long replay pays for the clamp per record. See
        // AllocationGrowthBetweenLoopSizes for why the assertion compares two
        // loop sizes rather than asserting an absolute zero.
        var ledger = new BPlusLeafGrain.DeferredOffsetLedger(8);
        for (var i = 0; i < 8; i++)
            ledger.Add(i, i + 1);

        var growth = AllocationGrowthBetweenLoopSizes(
            iterations => () =>
            {
                long sink = 0;
                for (var i = 0; i < iterations; i++)
                    sink += ledger.MinUnresolved(i & 7);
                _ledgerAllocationProbeSink = sink;
            },
            baseIterations: 50_000);

        Assert.That(_ledgerAllocationProbeSink, Is.GreaterThan(0L),
            "guard against the measured loop being optimised away");
        Assert.That(growth, Is.Zero,
            "MinUnresolved must not allocate per query on the replay hot path");
    }

    [Test]
    public void Ledger_add_does_not_allocate_per_deferred_offset_beyond_amortised_growth()
    {
        // Adds amortise by doubling, so a run of adds allocates O(log n) buffers
        // totalling O(n) bytes - not a fresh allocation per add. Doubling the
        // add count must therefore at most double the bytes; a per-add
        // allocation (a boxed offset or a per-add node) would blow past that.
        // Measured as a ratio rather than a difference because the amortised
        // growth is genuinely non-zero by design.
        static long MeasureAdds(int count, int attempts)
        {
            var smallest = long.MaxValue;
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                var ledger = new BPlusLeafGrain.DeferredOffsetLedger(1);
                var mark = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < count; i++)
                    ledger.Add(0, i);
                var allocated = GC.GetAllocatedBytesForCurrentThread() - mark;
                _ledgerAllocationProbeSink = ledger.MinUnresolved(0);
                if (allocated < smallest)
                    smallest = allocated;
            }
            return smallest;
        }

        // Warm up at the larger size before either measurement.
        _ = MeasureAdds(2048, attempts: 1);

        var small = MeasureAdds(1024, attempts: 5);
        var large = MeasureAdds(2048, attempts: 5);

        Assert.That(small, Is.GreaterThan(0L), "the buffer growth must have been observed");
        Assert.That(large, Is.LessThanOrEqualTo(small * 3),
            $"amortised doubling must keep growth linear in the DEFERRED count "
            + $"(1024 adds -> {small} bytes, 2048 adds -> {large} bytes)");
    }

    [Test]
    public void Ledger_resolve_does_not_allocate_per_drained_terminal()
    {
        // Pass 2 strikes one offset off per drained terminal, so the drain loop
        // must not allocate for the bookkeeping. The ledger is rebuilt by
        // prepare(), outside the measured window, so only Resolve is measured.
        var growth = AllocationGrowthBetweenLoopSizes(
            iterations =>
            {
                var ledger = new BPlusLeafGrain.DeferredOffsetLedger(1);
                for (var i = 0; i < iterations; i++)
                    ledger.Add(0, i);
                return () =>
                {
                    for (var i = 0; i < iterations; i++)
                        ledger.Resolve(0, i);
                    _ledgerAllocationProbeSink = ledger.MinUnresolved(0);
                };
            },
            baseIterations: 2048);

        Assert.That(_ledgerAllocationProbeSink, Is.EqualTo(long.MaxValue),
            "the measured loop must have resolved every offset");
        Assert.That(growth, Is.Zero, "Resolve must not allocate per drained terminal");
    }

    [Test]
    public async Task Replay_allocation_does_not_grow_super_linearly_with_record_count()
    {
        // Doubling the replayed window must roughly double the allocation, not
        // square it. A per-record allocation introduced by the deferred-offset
        // bookkeeping (re-deriving the clamp by rebuilding a collection per
        // record, say) would show up here as a super-linear jump.
        //
        // The replay awaits, so its continuations are not guaranteed to stay on
        // the calling thread and the per-thread counter would under-report:
        // the process-wide counter is the only sound measure here. It is also
        // the noisier one, so each size is measured repeatedly and the smallest
        // observation kept - ambient allocation only ever adds - and the bound
        // is a ratio rather than an absolute figure.
        static async Task<long> MeasureAsync(int recordCount)
        {
            var entries = new CommitLogSliceEntry[recordCount];
            entries[0] = FlushDeleteRange(1);
            for (var i = 2; i <= recordCount; i++)
                entries[i - 1] = FlushSet(i, $"k{i:D6}");

            var coord = BuildObservableCoordinator(
                head: recordCount,
                sliceSize: recordCount,
                tail: 0,
                onRead: null,
                entries);
            var state = NewFlushCeilingState();
            var store = new InMemorySnapshotStore();
            var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub);

            var before = GC.GetTotalAllocatedBytes(precise: true);
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
            return GC.GetTotalAllocatedBytes(precise: true) - before;
        }

        // Warm up at the larger size so no first-call JIT cost lands in either
        // measured window.
        _ = await MeasureAsync(512);

        var small = long.MaxValue;
        var large = long.MaxValue;
        for (var attempt = 0; attempt < 3; attempt++)
        {
            small = Math.Min(small, await MeasureAsync(256));
            large = Math.Min(large, await MeasureAsync(512));
        }

        Assert.That(small, Is.GreaterThan(0L), "the measurement must have observed the replay");
        Assert.That(large, Is.LessThan(small * 3),
            $"replay allocation must stay linear in the record count (256 -> {small} bytes, 512 -> {large} bytes)");
    }
}
