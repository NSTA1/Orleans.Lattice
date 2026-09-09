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
        int reclassifyEveryN = 0,
        int maxDurableUnresolvedReplayWork = LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
        int maxLeafReplayEntries = LatticeOptions.DefaultMaxLeafReplayEntries,
        bool recordUnresolvedPreparesBeyondCap = true)
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
            MaxDurableUnresolvedReplayWork = maxDurableUnresolvedReplayWork,
            MaxLeafReplayEntries = maxLeafReplayEntries,
        };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default())
        {
            RecordUnresolvedPreparesBeyondCap = recordUnresolvedPreparesBeyondCap,
        };
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

    /// <summary>
    /// Builds a 12-offset window carrying a fully self-contained saga near its
    /// head: a prepare at offset 2 terminated by its own
    /// <see cref="MutationKind.TxCommit"/> at offset 3, with plain Sets either
    /// side. Both records live in the SAME partition, so the saga has no
    /// cross-partition dependency whatsoever and nothing about its content
    /// requires deferral - the only thing that decides whether the terminal
    /// resolves the prepare during pass 1 is whether this partition is the one
    /// pass 1 absorbs last.
    /// </summary>
    private static CommitLogSliceEntry[] WindowWithASelfContainedSaga(Guid txId)
    {
        var entries = new CommitLogSliceEntry[12];
        entries[0] = FlushSet(1, "g01");
        entries[1] = new CommitLogSliceEntry(2, BuildPreparedSet(
            txId, "g02", Encoding.UTF8.GetBytes("v2"), treeId: FlushCeilingTreeId));
        entries[2] = new CommitLogSliceEntry(3, BuildTerminal(
            txId, committed: true, treeId: FlushCeilingTreeId));
        for (var i = 4; i <= 12; i++)
            entries[i - 1] = FlushSet(i, $"g{i:D2}");
        return entries;
    }

    /// <summary>
    /// Drives <paramref name="attempts"/> activations over the same durable
    /// state, tearing each one down at the second slice boundary of the
    /// backlogged partition, and returns the persisted checkpoint observed
    /// after each attempt.
    /// </summary>
    private static async Task<List<long>> RunInterruptedReplaysAsync(
        FakePersistentState<LeafNodeState> state,
        Func<CancellationTokenSource, ILeafReplayCoordinatorGrain[]> buildCoordinators,
        int attempts,
        int maxDurableUnresolvedReplayWork = LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
        int maxLeafReplayEntries = LatticeOptions.DefaultMaxLeafReplayEntries,
        bool recordUnresolvedPreparesBeyondCap = true)
    {
        var store = new InMemorySnapshotStore();
        var observed = new List<long>();

        for (var attempt = 0; attempt < attempts; attempt++)
        {
            using var cts = new CancellationTokenSource();
            var grain = BuildFlushCeilingLeaf(
                state, buildCoordinators(cts), store.Stub, reclassifyEveryN: 1,
                maxDurableUnresolvedReplayWork: maxDurableUnresolvedReplayWork,
                maxLeafReplayEntries: maxLeafReplayEntries,
                recordUnresolvedPreparesBeyondCap: recordUnresolvedPreparesBeyondCap);

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

        return observed;
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

    [Test]
    public async Task Successive_interrupted_replays_converge_when_the_saga_terminal_drains_inline()
    {
        // CONTROL. Identical window to the multi-partition case below, but
        // replayed as a SINGLE-partition tree, which is drain-eligible
        // throughout: the terminal drains in place during pass 1, resolving
        // the prepare and releasing the clamp. This is the only configuration
        // the pre-existing convergence coverage exercised, and it must keep
        // converging.
        var txId = Guid.NewGuid();
        var state = NewFlushCeilingState();

        var observed = await RunInterruptedReplaysAsync(
            state,
            cts =>
            [
                BuildObservableCoordinator(
                    head: 12,
                    sliceSize: 2,
                    tail: state.State.ProjectionCheckpointOffset,
                    onRead: read =>
                    {
                        if (read == 2)
                            cts.Cancel();
                    },
                    WindowWithASelfContainedSaga(txId)),
            ],
            attempts: 12);

        Assert.That(observed, Is.Ordered.Ascending.And.Unique,
            "With the terminal drained inline the prepare resolves and each "
            + "interrupted activation must advance the persisted checkpoint.");
        Assert.That(observed[^1], Is.EqualTo(12L),
            "Successive interrupted activations must converge on the head of the window.");
    }

    [Test]
    public async Task Successive_interrupted_replays_converge_when_the_backlog_is_on_a_non_last_partition()
    {
        // Issue #2089. Pass 1 can only drain a deferred terminal in place for
        // the partition it absorbs LAST, so on a multi-partition tree an
        // unresolved saga prepare pins the flush ceiling at (prepare - 1) for
        // every other partition. Once the checkpoint reaches that floor no
        // further flush occurs, an activation torn down before pass 2 banks
        // nothing, and the next replays the identical range forever.
        //
        // Here the whole backlog sits on partition 0 and partition 1 is empty.
        // Sweeping in fixed index order would hand the single drain-eligible
        // slot to the EMPTY partition 1 and livelock partition 0 at offset 1.
        // Ordering the sweep by backlog ascending gives the slot to partition
        // 0, which is the only partition with anything to bank.
        var txId = Guid.NewGuid();
        var state = NewFlushCeilingState();

        var observed = await RunInterruptedReplaysAsync(
            state,
            cts =>
            [
                BuildObservableCoordinator(
                    head: 12,
                    sliceSize: 2,
                    tail: state.State.ProjectionCheckpointOffset,
                    onRead: read =>
                    {
                        if (read == 2)
                            cts.Cancel();
                    },
                    WindowWithASelfContainedSaga(txId)),
                // Empty, and therefore last by index but first by backlog.
                BuildObservableCoordinator(head: 0, sliceSize: 2, tail: 0, onRead: null),
            ],
            attempts: 12);

        Assert.That(observed, Is.Ordered.Ascending.And.Unique,
            "Each interrupted activation must advance the persisted checkpoint strictly. "
            + "A repeated value means the unresolved-prepare clamp has pinned the ceiling "
            + "and successive activations are replaying the identical range forever.");
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
        //
        // Partition 1 is deliberately given the LARGER backlog (8 against 5),
        // because pass 1 sweeps by backlog ascending (issue #2089) and only
        // the partition absorbed LAST is drain-eligible. That puts partition 0
        // first and therefore genuinely non-last, which is the configuration
        // this test exists to cover: a partition that must hold its ceiling
        // because its cross-partition dependencies are not yet in the cache.
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
            head: 8,
            sliceSize: 2,
            tail: 0,
            onRead: null,
            FlushSet(1, "m5"),
            FlushSet(2, "m6"),
            FlushSet(3, "z7"),
            FlushSet(4, "z8"),
            FlushSet(5, "z9"),
            FlushSet(6, "za"),
            FlushSet(7, "zb"),
            FlushSet(8, "zc"));

        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();

        var partitionZeroPersists = new List<long>();
        state.OnWriteState = s => partitionZeroPersists.Add(s.ProjectionCheckpointOffset);

        // Guards the NO-RECORD path (issue #2165): with no durable record of
        // the deferred DeleteRange, the clamp is the only thing that stops it
        // being lost, so partition 0 must hold its ceiling. The ledgered path
        // is guarded by its own counterpart below.
        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub, maxDurableUnresolvedReplayWork: 0);

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
    public async Task Ledgered_cross_partition_delete_range_advances_the_ceiling_without_losing_the_delete()
    {
        // Enabled-path counterpart to
        // Cross_partition_delete_range_still_holds_the_incremental_ceiling
        // (issue #2165). This is the highest-risk shape in the change: if
        // advancing the ceiling past a deferred range delete dropped it, a
        // whole key range would silently survive a delete. The counterpart
        // therefore asserts BOTH halves - the ceiling advances (the fix), AND
        // the range delete still lands on every partition's keys (the
        // invariant the clamp used to protect).
        //
        // Identical topology to the no-record guard: partition 1 carries the
        // larger backlog so pass 1 sweeps partition 0 first, leaving it
        // genuinely non-drain-eligible. On the deployed box 7 of 8 partitions
        // are in exactly this position on every activation.
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
            head: 8,
            sliceSize: 2,
            tail: 0,
            onRead: null,
            FlushSet(1, "m5"),
            FlushSet(2, "m6"),
            FlushSet(3, "z7"),
            FlushSet(4, "z8"),
            FlushSet(5, "z9"),
            FlushSet(6, "za"),
            FlushSet(7, "zb"),
            FlushSet(8, "zc"));

        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();

        // Sample the ledger at each persist, so we can prove the record was
        // durable AT THE MOMENT the ceiling advanced - not merely present at
        // some later point, which would not survive a teardown.
        var persists = new List<(long Offset, int Ledgered)>();
        state.OnWriteState = s => persists.Add((s.ProjectionCheckpointOffset, s.UnresolvedReplayWork?.Count ?? 0));

        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub, maxDurableUnresolvedReplayWork: 1024);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(persists.Select(p => p.Offset), Does.Contain(4L),
            "With the deferred DeleteRange durably recorded, partition 0 advances past it in pass 1 "
            + "instead of freezing at 2 until pass 2 - which on a non-last partition never arrives.");

        var advancing = persists.Where(p => p.Offset >= 3L).ToList();
        Assert.That(advancing, Is.Not.Empty);
        Assert.That(advancing.All(p => p.Ledgered >= 1), Is.True,
            "Every persist that advanced to or past the deferred DeleteRange must have carried the "
            + "record in the same state write - atomicity is what makes advancing safe.");

        // The invariant the clamp protected is unchanged: the range delete
        // still orders after every partition's Sets, including the
        // cross-partition ones it could not see in pass 1.
        Assert.That(await grain.GetAsync("m5"), Is.Null, "cross-partition in-range key is still deleted");
        Assert.That(await grain.GetAsync("m6"), Is.Null, "cross-partition in-range key is still deleted");
        Assert.That(await grain.GetAsync("z7"), Is.Not.Null, "out-of-range key is untouched");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L),
            "Reconciliation still converges on partition 0's applied frontier.");
        Assert.That(state.State.UnresolvedReplayWork ?? [], Is.Empty,
            "Once pass 2 drains the terminal the record is resolved, so the ledger does not grow without bound.");
    }

    [Test]
    public async Task A_sweep_order_head_probe_fault_degrades_the_ordering_without_aborting_the_sweep()
    {
        // The pass-1 sweep-order pre-pass probes each partition's head to rank
        // the backlogs (issue #2089). A fault on one probe must NOT abort the
        // sweep: nothing has been banked at that point, so aborting would cost
        // EVERY partition's progress rather than only the faulting one's -
        // strictly worse than today's behaviour on a box already banking
        // almost nothing. Nor may it be swallowed into a silently wrong order,
        // which is the fault-masking shape issue #2082 closed on the
        // trimmed-prefix probe. The faulting partition keeps its natural
        // position with an unprobed head, and ReplayPartitionAsync re-probes
        // it in its own turn so any persistent fault still surfaces there.
        var p0 = BuildObservableCoordinator(
            head: 4,
            sliceSize: 2,
            tail: 0,
            onRead: null,
            FlushSet(1, "a1"),
            FlushSet(2, "a2"),
            FlushSet(3, "a3"),
            FlushSet(4, "a4"));

        // Fault only the FIRST head read - the sweep-order pre-pass - and let
        // the re-probe inside ReplayPartitionAsync succeed, modelling exactly
        // the transient coordinator timeout this fallback exists for.
        var headReads = 0;
        p0.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            headReads++ == 0
                ? throw new TimeoutException("sweep-order probe fault")
                : Task.FromResult(4L));

        var p1 = BuildObservableCoordinator(
            head: 2,
            sliceSize: 2,
            tail: 0,
            onRead: null,
            FlushSet(1, "b1"),
            FlushSet(2, "b2"));

        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(await grain.GetAsync("b2"), Is.Not.Null,
            "A probe fault on one partition must not cost another partition its replay.");
        Assert.That(await grain.GetAsync("a4"), Is.Not.Null,
            "The faulting partition must still replay once its head is re-probed.");
        Assert.That(headReads, Is.GreaterThanOrEqualTo(2),
            "The unprobed partition must be re-probed by ReplayPartitionAsync, not skipped.");
    }

    [Test]
    public async Task All_partitions_at_the_sentinel_are_still_ranked_by_head_so_ordering_survives_a_cold_start()
    {
        // The pass-1 sweep ranks partitions by backlog and awards the single
        // drain-eligible slot (absorbed LAST) to the largest (issue #2089).
        //
        // A partition at the "nothing applied" sentinel (-1) is NOT comparable
        // with one holding a real checkpoint, because head - -1 measures the
        // whole shard partition's WAL rather than this leaf's pending work.
        // But that objection is about MIXING two baselines. When EVERY
        // partition is at the sentinel they share one baseline, so head is a
        // valid relative measure and the ordering must still apply.
        //
        // This is the dominant case, not a corner: the cold-start cache-empty
        // override drives checkpointOverride to -1 for every partition, which
        // is exactly the activation with the most to replay and therefore the
        // one #2089's ordering exists to help. Excluding sentinel partitions
        // wholesale would silently collapse the sweep to index order here and
        // make (b) inert on every cold start - a fix that does nothing in the
        // only case that matters, while still passing a mixed-baseline test.
        var readOrder = new List<int>();

        var p0 = BuildObservableCoordinator(
            head: 4,
            sliceSize: 8,
            tail: 0,
            onRead: _ => { if (!readOrder.Contains(0)) readOrder.Add(0); },
            FlushSet(1, "a1"),
            FlushSet(2, "a2"),
            FlushSet(3, "a3"),
            FlushSet(4, "a4"));

        // An order of magnitude more to read, so index order and backlog order
        // disagree and the assertion discriminates between them.
        var p1 = BuildObservableCoordinator(
            head: 100,
            sliceSize: 8,
            tail: 0,
            onRead: _ => { if (!readOrder.Contains(1)) readOrder.Add(1); },
            FlushSet(1, "b1"),
            FlushSet(2, "b2"));

        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(readOrder, Has.Count.EqualTo(2), "Both partitions must be swept.");
        Assert.That(readOrder[^1], Is.EqualTo(1),
            "With every partition on the same (sentinel) baseline, the partition with the "
            + "largest head has the most to replay and must be absorbed last to take the "
            + "drain slot. Collapsing to index order here would make issue #2089's ordering "
            + "inert on exactly the cold-start activation it exists to help.");

        // Ordering must never skip: both partitions still replay in full.
        Assert.That(await grain.GetAsync("a4"), Is.Not.Null);
        Assert.That(await grain.GetAsync("b2"), Is.Not.Null);
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

        // Guards the NO-RECORD path (issue #2165). This test's own comment
        // states the rationale precisely - "a resumed replay has to re-read
        // the prepare" - and that holds exactly when the prepare is not
        // recorded durably, which is what this configuration pins.
        var grain = BuildFlushCeilingLeaf(state, [coord], store.Stub, maxDurableUnresolvedReplayWork: 0);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
        Assert.That(persistedOffsets, Is.All.LessThanOrEqualTo(1L),
            "No checkpoint persist may advance past the unresolved prepare at offset 2.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
            "The checkpoint stays clamped one below the open prepare offset.");
    }

    /// <summary>
    /// Builds a 12-offset single-partition window carrying TWO unresolved saga
    /// prepares near its head (offsets 2 and 3) that never terminate, with
    /// plain Sets either side. This is the issue #2183 shape: with a durable
    /// ledger too small to hold both, the second prepare cannot be recorded
    /// and - before the fix - falls back to the in-memory clamp that pins the
    /// ceiling at (prepare - 1) forever.
    /// </summary>
    private static CommitLogSliceEntry[] WindowWithTwoUnresolvedPrepares(Guid txA, Guid txB)
    {
        var entries = new CommitLogSliceEntry[12];
        entries[0] = FlushSet(1, "p01");
        entries[1] = new CommitLogSliceEntry(2, BuildPreparedSet(
            txA, "p02", Encoding.UTF8.GetBytes("vA"), treeId: FlushCeilingTreeId));
        entries[2] = new CommitLogSliceEntry(3, BuildPreparedSet(
            txB, "p03", Encoding.UTF8.GetBytes("vB"), treeId: FlushCeilingTreeId));
        for (var i = 4; i <= 12; i++)
            entries[i - 1] = FlushSet(i, $"p{i:D2}");
        return entries;
    }

    [Test]
    public async Task Saturated_ledger_resident_prepare_still_advances_the_checkpoint_across_activations()
    {
        // FIX ARM (issue #2183). A leaf whose durable unresolved-work ledger is
        // saturated (cap 1, but TWO resident unresolved prepares at offsets 2
        // and 3) and whose partition gap (12) far exceeds an advisory
        // MaxLeafReplayEntries budget of 2. The second prepare cannot fit the
        // capped ledger, but a resident prepare must never be dropped, so the
        // fix records it beyond the cap. The checkpoint must therefore advance
        // PAST both prepares and climb strictly across successive interrupted
        // activations until it converges on the head of the window.
        //
        // The gap (12) is the whole partition's extent and is only an upper
        // bound on the per-leaf work; the advisory budget (2) is compared
        // against this leaf's own applied entries. The two are deliberately
        // different quantities (issue #2149 units trap) and the checkpoint
        // still crosses the saturated prepare regardless of either.
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();
        var state = NewFlushCeilingState();

        var observed = await RunInterruptedReplaysAsync(
            state,
            cts =>
            [
                BuildObservableCoordinator(
                    head: 12,
                    sliceSize: 2,
                    tail: state.State.ProjectionCheckpointOffset,
                    onRead: read =>
                    {
                        if (read == 2)
                            cts.Cancel();
                    },
                    WindowWithTwoUnresolvedPrepares(txA, txB)),
            ],
            attempts: 12,
            maxDurableUnresolvedReplayWork: 1,
            maxLeafReplayEntries: 2,
            recordUnresolvedPreparesBeyondCap: true);

        Assert.That(observed, Is.Ordered.Ascending.And.Unique,
            "With the resident prepare recorded beyond the saturated cap, each interrupted "
            + "activation must advance the persisted checkpoint strictly - a repeated value would "
            + "mean the prepare clamp has pinned the ceiling and the leaf is banking zero progress.");
        Assert.That(observed[^1], Is.EqualTo(12L),
            "Successive interrupted activations must converge on the head of the window despite "
            + "the resident unresolved prepares.");
    }

    [Test]
    public async Task Saturated_ledger_without_the_fix_pins_the_checkpoint_below_the_resident_prepare()
    {
        // CONTROL ARM (issue #2183). Identical scenario and configuration as
        // the fix arm above, differing ONLY in the fix: the resident prepare
        // that overflows the cap is dropped back onto the in-memory clamp
        // exactly as it shipped. The checkpoint must then pin at (prepare - 1)
        // = 2 and never move, so successive activations replay the identical
        // range forever. This is the positive control: it proves the scenario
        // genuinely reproduces the livelock, so the fix arm's strict advance is
        // evidence of the fix and not of a scenario that never froze.
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();
        var state = NewFlushCeilingState();

        var observed = await RunInterruptedReplaysAsync(
            state,
            cts =>
            [
                BuildObservableCoordinator(
                    head: 12,
                    sliceSize: 2,
                    tail: state.State.ProjectionCheckpointOffset,
                    onRead: read =>
                    {
                        if (read == 2)
                            cts.Cancel();
                    },
                    WindowWithTwoUnresolvedPrepares(txA, txB)),
            ],
            attempts: 6,
            maxDurableUnresolvedReplayWork: 1,
            maxLeafReplayEntries: 2,
            recordUnresolvedPreparesBeyondCap: false);

        Assert.That(observed, Has.Count.GreaterThan(1),
            "The control must run several activations to demonstrate the pin persists.");
        Assert.That(observed, Is.All.EqualTo(2L),
            "Without the fix the dropped prepare pins the ceiling at (prepare - 1) = 2 on every "
            + "activation - the leaf banks zero durable forward progress, which is the #2183 freeze.");
        Assert.That(observed[^1], Is.LessThan(12L),
            "The control must never converge: a saturated-ledger resident prepare freezes the leaf.");
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

    // Shared scenario for the two-arm #2165 discriminator below. The two arms
    // differ ONLY in whether the durable replay-work ledger is enabled, so the
    // comparison is made inside one binary, in one test run, with no source
    // stashing and therefore no dependence on a clean working tree. That
    // matters: the conventional "stash the fix and re-run" control is unsafe
    // in this repository because the stash stack is shared across every linked
    // worktree on the machine, so a sibling agent's stash can land in the
    // control arm and manufacture a red result that was never earned.
    private static async Task<List<long>> RunPinnedSagaPartitionScenarioAsync(
        int maxDurableUnresolvedReplayWork)
    {
        var txId = Guid.NewGuid();
        var state = NewFlushCeilingState();

        // Both partitions must hold a REAL checkpoint. BuildPassOneSweepOrder
        // sorts a partition whose backlog is not comparable (the -1 "nothing
        // applied" sentinel) strictly FIRST, so leaving partition 1 at the
        // sentinel would hand partition 0 the last slot - and with it the
        // drain eligibility this scenario exists to deny it. Production always
        // has a real checkpoint on every partition, so seeding both is the
        // faithful shape, not a convenience.
        state.State.ProjectionCheckpointOffsetsByPartition = [0L, 0L];

        return await RunInterruptedReplaysAsync(
            state,
            cts =>
            [
                // Partition 0: the saga backlog. Smaller (12) than partition 1
                // (20), so the backlog-ASCENDING sweep absorbs it FIRST and it
                // is never the drain-eligible last partition. The teardown
                // below lands inside partition 0's own scan, so partition 1 is
                // never swept at all and its backlog never shrinks - which is
                // what keeps the ordering stable across every attempt.
                BuildObservableCoordinator(
                    head: 12,
                    sliceSize: 2,
                    tail: state.State.ProjectionCheckpointOffset,
                    onRead: read =>
                    {
                        // Tear the activation down as the second slice is
                        // served, so pass 1 never finishes and pass 2 - which
                        // would drain the terminal and lift both clamps - never
                        // runs. This is the 30 s RuntimeRequested teardown.
                        if (read == 2)
                            cts.Cancel();
                    },
                    WindowWithASelfContainedSaga(txId)),
                // Partition 1: a larger, saga-free backlog, so it wins the
                // drain slot on every sweep and partition 0 never does.
                BuildObservableCoordinator(
                    head: 20,
                    sliceSize: 2,
                    tail: 0,
                    onRead: null,
                    [.. Enumerable.Range(1, 20).Select(i => FlushSet(i, $"p1-{i:D2}"))]),
            ],
            attempts: 12,
            maxDurableUnresolvedReplayWork: maxDurableUnresolvedReplayWork);
    }

    [Test]
    public async Task Pinned_saga_partition_relives_the_livelock_when_replay_work_is_not_recorded()
    {
        // CONTROL ARM (red without the fix) for issue #2165 - the residual
        // #2089's own comment documents but does not remove. With the durable
        // ledger DISABLED this asserts the defect is present, so the arm is a
        // permanent in-suite regression test rather than a transient stash.
        //
        // #2089 moved the single drain-eligible pass-1 slot to the partition
        // with the LARGEST backlog. That converges only while exactly one
        // partition is backlogged. Here TWO are, and the saga sits on the
        // SMALLER one, so the slot goes to the other partition and the saga
        // partition is non-drain-eligible on every single activation.
        //
        // Every activation then recomputes the identical pin:
        //   offset 2 is an unresolved prepare  -> ceiling <= 1
        //   offset 3 is its deferred terminal  -> ceiling <= 2
        // so the ceiling lands on 1, equals the checkpoint already persisted
        // by the first attempt, and TryFlushRecoveredCeilingAsync returns
        // without flushing. Pass 2 - the only thing that lifts either clamp -
        // is never reached because the activation is torn down inside pass 1.
        //
        // That is the production shape measured on the deployed repocontext
        // image: leaf bplusleaf/6262caad..., partition 2 of 8, 69 activations
        // over 6h51m52s at ONE distinct checkpoint value, delta ZERO.
        var observed = await RunPinnedSagaPartitionScenarioAsync(
            maxDurableUnresolvedReplayWork: 0);

        Assert.That(observed, Has.Count.EqualTo(12),
            "The scenario must run every attempt: converging early would mean the pin is absent "
            + "and this arm is no longer measuring the defect. Observed: "
            + string.Join(", ", observed));
        Assert.That(observed.Distinct().Count(), Is.EqualTo(1),
            "Without a durable record of the unresolved prepare and its deferred terminal, every "
            + "activation must re-derive the identical clamped ceiling and bank nothing - one "
            + "distinct checkpoint across every attempt, delta ZERO. Observed: "
            + string.Join(", ", observed));
        Assert.That(observed[^1], Is.LessThan(12L),
            "The pinned partition must never reach the head of its window.");
    }

    [Test]
    public async Task Successive_interrupted_replays_converge_when_the_saga_partition_never_wins_the_drain_slot()
    {
        // DISCRIMINATOR (green with the fix). Identical scenario to the
        // control arm above; the ONLY difference is that the unresolved
        // prepare and the undrained deferred terminal are now recorded
        // durably, so the ceiling need not clamp behind work a resumed replay
        // will not have to re-read. The drain slot is NOT widened - the
        // cross-partition dependency argument that keeps it at one still
        // holds, and pass 2 remains the only place a terminal drains.
        var observed = await RunPinnedSagaPartitionScenarioAsync(
            maxDurableUnresolvedReplayWork: 1024);

        Assert.That(observed, Is.Ordered.Ascending.And.Unique,
            "Each interrupted activation must advance the saga partition's persisted checkpoint "
            + "strictly. A repeated value is the #2165 livelock: the partition re-reads the "
            + "identical range forever because the unresolved prepare and its undrained deferred "
            + "terminal pin the ceiling at the checkpoint that is already persisted. Observed: "
            + string.Join(", ", observed));
        Assert.That(observed[^1], Is.EqualTo(12L),
            "Successive interrupted activations must converge on the head of the window.");
    }

    [Test]
    public async Task Unresolved_replay_work_is_recorded_durably_and_reconstructs_the_saga_outcome()
    {
        // GUARD on the mechanism the convergence above relies on: the ceiling
        // may only pass an unresolved prepare or an undrained deferred
        // terminal because that work is durably recorded in the leaf's own
        // state, so the resumed replay reconstructs it instead of re-reading
        // it. Assert both halves - that the record exists while the work is
        // outstanding, and that the saga's prepared write still lands.
        var txId = Guid.NewGuid();
        var state = NewFlushCeilingState();

        // Attempt 1: absorb the prepare (offset 2) and the terminal (offset 3)
        // on a non-drain-eligible partition, then tear down.
        using (var cts = new CancellationTokenSource())
        {
            var store = new InMemorySnapshotStore();
            var grain = BuildFlushCeilingLeaf(
                state,
                [
                    BuildObservableCoordinator(
                        head: 12, sliceSize: 4, tail: 0,
                        onRead: read => { if (read == 2) cts.Cancel(); },
                        WindowWithASelfContainedSaga(txId)),
                    BuildObservableCoordinator(
                        head: 20, sliceSize: 2, tail: 0, onRead: null,
                        [.. Enumerable.Range(1, 20).Select(i => FlushSet(i, $"p1-{i:D2}"))]),
                ],
                store.Stub,
                reclassifyEveryN: 1,
                maxDurableUnresolvedReplayWork: 1024);

            try
            {
                await ((IGrainBase)grain).OnActivateAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected: the activation is torn down inside pass 1.
            }
        }

        Assert.That(state.State.ProjectionCheckpointOffset, Is.GreaterThan(1L),
            "The ceiling must pass the unresolved prepare at offset 2 and the deferred terminal "
            + "at offset 3 once both are durably recorded.");
        Assert.That(state.State.UnresolvedReplayWork, Is.Not.Null.And.Not.Empty,
            "Work the ceiling was allowed to pass must be durably recorded, or the advance has "
            + "licensed a checkpoint past an offset nothing can reconstruct.");

        // Attempt 2: run to completion. The recorded prepare and terminal are
        // reconstructed from state (they are BELOW the checkpoint, so the WAL
        // is never re-read for them) and the saga's write must still commit.
        var finalStore = new InMemorySnapshotStore();
        var resumed = BuildFlushCeilingLeaf(
            state,
            [
                BuildObservableCoordinator(
                    head: 12, sliceSize: 4,
                    tail: state.State.ProjectionCheckpointOffset,
                    onRead: null,
                    WindowWithASelfContainedSaga(txId)),
                BuildObservableCoordinator(
                    head: 20, sliceSize: 20, tail: 0, onRead: null,
                    [.. Enumerable.Range(1, 20).Select(i => FlushSet(i, $"p1-{i:D2}"))]),
            ],
            finalStore.Stub,
            reclassifyEveryN: 1,
            maxDurableUnresolvedReplayWork: 1024);

        await ((IGrainBase)resumed).OnActivateAsync(CancellationToken.None);

        Assert.That(await resumed.GetAsync("g02"), Is.Not.Null,
            "The saga's prepared write must still be committed by the reconstructed terminal - "
            + "advancing the checkpoint past it must not silently drop it.");
        Assert.That(resumed.PendingTransactionCount, Is.Zero,
            "The reconstructed terminal must resolve the reconstructed prepare.");
        Assert.That(state.State.UnresolvedReplayWork, Is.Null.Or.Empty,
            "Resolved work must be struck off the durable record rather than accumulating.");
    }
}
