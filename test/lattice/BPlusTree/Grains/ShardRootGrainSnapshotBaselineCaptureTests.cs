using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1961, which reviewed
/// <c>ShardRootGrain.CaptureSnapshotBaselineAsync</c> and settled on two
/// mitigations that bound the shard hold without making the freeze/fold walk
/// resumable: the hard end-to-end stall ceiling added by issue 2003 is applied
/// to the capture, and its fold pass is fanned out.
/// <para>
/// The fold pass is the only part that may be reordered. Point-in-time
/// consistency comes from <c>capturedHead</c> dominating every leaf's frozen
/// frontier, not from the exclusive hold, so folding leaves concurrently
/// against an already-captured head cannot change what is captured. What the
/// fan-out must not disturb is the <em>union</em>: results are consumed in
/// strict leaf-chain order so a donor-orphan collision resolves exactly as it
/// did under a serial fold, including the merge-mode adoption tie cases.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainSnapshotBaselineCaptureTests
{
    private const string TreeId = "capture-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// A leaf chain wired for a baseline capture, with per-leaf control over
    /// when each fold completes.
    /// </summary>
    private sealed class CaptureHarness
    {
        private readonly List<int> _foldsDispatched = [];
        private readonly Dictionary<int, TaskCompletionSource> _dispatchWaiters = [];

        public ShardRootGrain Grain { get; set; } = null!;

        /// <summary>The baseline handed to <c>ISnapshotLeafGrain.SeedAsync</c>, once captured.</summary>
        public SnapshotShardBaseline? Seeded { get; set; }

        /// <summary>Highest number of folds observed in flight at the same instant.</summary>
        public int PeakConcurrentFolds;

        /// <summary>Gates each leaf's fold when the harness was built parked.</summary>
        public TaskCompletionSource<IReadOnlyList<LeafSnapshotRow>>[] Gates { get; set; } = [];

        /// <summary>
        /// Leaf-chain index of every fold, in the order the folds were
        /// dispatched. A snapshot: folds are dispatched from the thread pool, so
        /// the live list must never be read without its lock.
        /// </summary>
        public IReadOnlyList<int> FoldsDispatched
        {
            get
            {
                lock (_foldsDispatched)
                {
                    return _foldsDispatched.ToList();
                }
            }
        }

        public void RecordDispatch(int leafIndex)
        {
            lock (_foldsDispatched)
            {
                _foldsDispatched.Add(leafIndex);
                foreach (var (at, waiter) in _dispatchWaiters)
                {
                    if (_foldsDispatched.Count >= at)
                    {
                        waiter.TrySetResult();
                    }
                }
            }
        }

        /// <summary>
        /// A task that completes when the <paramref name="count"/>th fold has
        /// been dispatched, so a test waits on the dispatch <em>event</em>
        /// rather than on a guessed number of yields or a sleep. Folds are
        /// dispatched on the thread pool, so yielding on the test thread
        /// establishes no happens-before with one and cannot be used to decide
        /// the window has settled - which is what made this fixture flake on CI.
        /// </summary>
        public Task DispatchCountReaches(int count)
        {
            lock (_foldsDispatched)
            {
                if (_foldsDispatched.Count >= count)
                {
                    return Task.CompletedTask;
                }

                if (!_dispatchWaiters.TryGetValue(count, out var waiter))
                {
                    waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    _dispatchWaiters[count] = waiter;
                }

                return waiter.Task;
            }
        }
    }

    private static LeafSnapshotRow Row(string key, string value, long hlc, LatticeMergeMode? mode = null) =>
        new(key, new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes(value),
            Timestamp = new HybridLogicalClock { WallClockTicks = hlc },
        }, mode);

    /// <summary>
    /// Builds a chain of <paramref name="leafCount"/> leaves whose folds return
    /// <paramref name="rowsPerLeaf"/>.
    /// </summary>
    /// <param name="foldConcurrency">The fan-out under test.</param>
    /// <param name="rowsPerLeaf">Rows each leaf's fold returns, by chain index.</param>
    /// <param name="parkFolds">
    /// When true every fold parks on its own gate, so a test drives completion
    /// order explicitly. When false each fold completes after a yield, with
    /// later leaves completing first so consumption order cannot accidentally
    /// match completion order.
    /// </param>
    /// <param name="stallDuration">The hard ceiling to arm, or infinite.</param>
    /// <param name="parkFreezeAtLeaf">A leaf whose freeze never returns, or -1.</param>
    private static CaptureHarness CreateChain(
        int leafCount,
        Func<int, IReadOnlyList<LeafSnapshotRow>> rowsPerLeaf,
        int foldConcurrency = 4,
        bool parkFolds = false,
        TimeSpan? stallDuration = null,
        int parkFreezeAtLeaf = -1)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[leafCount];
        var keys = new Guid[leafCount];
        for (var i = 0; i < leafCount; i++)
        {
            keys[i] = Guid.NewGuid();
            // The capture resolves each leaf by its Guid key, so the node id's
            // key has to round-trip through GetGuidKey().
            ids[i] = GrainId.Create("leaf", keys[i].ToString("N"));
        }

        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var gates = new TaskCompletionSource<IReadOnlyList<LeafSnapshotRow>>[leafCount];
        for (var i = 0; i < leafCount; i++)
            gates[i] = new TaskCompletionSource<IReadOnlyList<LeafSnapshotRow>>(
                TaskCreationOptions.RunContinuationsAsynchronously);

        var harness = new CaptureHarness { Gates = gates };

        var inFlight = 0;
        var neverCompletes = new TaskCompletionSource<LeafBaselineFreeze>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();

            leaf.FreezeProjectionAsync(Arg.Any<CancellationToken>())
                .Returns(_ => index == parkFreezeAtLeaf
                    ? neverCompletes.Task
                    : Task.FromResult(new LeafBaselineFreeze
                    {
                        Rows = [],
                        FrontierPerPartition = [index],
                        Pending = [],
                    }));

            leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                index + 1 < leafCount ? (GrainId?)ids[index + 1] : null));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));

            leaf.FoldTailOntoFrozenAsync(
                    Arg.Any<LeafBaselineFreeze>(), Arg.Any<long[]>(), Arg.Any<CancellationToken>())
                .Returns(_ =>
                {
                    harness.RecordDispatch(index);

                    var now = Interlocked.Increment(ref inFlight);
                    InterlockedMax(ref harness.PeakConcurrentFolds, now);

                    if (parkFolds)
                    {
                        return gates[index].Task.ContinueWith(
                            t =>
                            {
                                Interlocked.Decrement(ref inFlight);
                                return t.Result;
                            },
                            TaskScheduler.Default);
                    }

                    return CompleteOutOfOrderAsync(index, leafCount, rowsPerLeaf, () =>
                        Interlocked.Decrement(ref inFlight));
                });

            factory.GetGrain<IBPlusLeafGrain>(keys[index]).Returns(leaf);
            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
        }

        // The uniform head read after the freeze pass.
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(99L));
        factory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/0").Returns(coordinator);

        var snapshotLeaf = Substitute.For<ISnapshotLeafGrain>();
        snapshotLeaf.SeedAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<SnapshotShardBaseline>(),
                Arg.Any<Guid>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                harness.Seeded = call.Arg<SnapshotShardBaseline>();
                return Task.CompletedTask;
            });
        factory.GetGrain<ISnapshotLeafGrain>(Arg.Any<string>()).Returns(snapshotLeaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaxConcurrentSnapshotBaselineFolds = foldConcurrency,
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration ?? Timeout.InfiniteTimeSpan,
            },
            shardCount: 1,
            factory: factory);

        harness.Grain = new ShardRootGrain(context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());
        return harness;
    }

    /// <summary>
    /// Completes leaf <paramref name="index"/>'s fold after a number of yields
    /// that <em>decreases</em> with the chain position, so later leaves finish
    /// before earlier ones and completion order is never consumption order.
    /// </summary>
    private static async Task<IReadOnlyList<LeafSnapshotRow>> CompleteOutOfOrderAsync(
        int index,
        int leafCount,
        Func<int, IReadOnlyList<LeafSnapshotRow>> rowsPerLeaf,
        Action onComplete)
    {
        for (var i = 0; i < (leafCount - index) * 2; i++)
            await Task.Yield();
        onComplete();
        return rowsPerLeaf(index);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        var seen = Volatile.Read(ref target);
        while (value > seen)
        {
            var previous = Interlocked.CompareExchange(ref target, value, seen);
            if (previous == seen) return;
            seen = previous;
        }
    }

    // --- the union is unchanged by the fan-out ---

    /// <summary>
    /// The load-bearing equivalence: a fanned-out fold must produce a baseline
    /// byte-for-byte identical to a serial one. The chain deliberately contains
    /// a donor-orphan collision - the same key returned by two leaves with
    /// different clocks and different merge modes - because that is the only
    /// place the union's outcome depends on the order rows arrive in, and the
    /// merge-mode adoption rule is written against a single accumulating pass.
    /// </summary>
    [Test]
    public async Task Fanned_out_fold_produces_the_same_baseline_as_a_serial_fold()
    {
        static IReadOnlyList<LeafSnapshotRow> Rows(int leaf) => leaf switch
        {
            // The donor orphan: leaf 0 holds the stale copy, leaf 3 the live
            // one. The later (higher-HLC) row must win, and take its mode with
            // it, whichever order the folds complete in.
            0 => [Row("orphan", "stale", hlc: 10, LatticeMergeMode.LwwRegister), Row($"k{leaf:D2}", "v", 20)],
            3 => [Row("orphan", "live", hlc: 30, LatticeMergeMode.OrSet), Row($"k{leaf:D2}", "v", 20)],
            _ => [Row($"k{leaf:D2}", "v", 20)],
        };

        var serial = CreateChain(leafCount: 6, Rows, foldConcurrency: 1);
        await serial.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);

        var fanned = CreateChain(leafCount: 6, Rows, foldConcurrency: 6);
        await fanned.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(serial.Seeded, Is.Not.Null);
            Assert.That(fanned.Seeded, Is.Not.Null);
            Assert.That(fanned.PeakConcurrentFolds, Is.GreaterThan(1),
                "the fan-out must actually overlap, or this asserts nothing");
            Assert.That(serial.PeakConcurrentFolds, Is.EqualTo(1),
                "a concurrency of 1 must remain strictly serial");
        });

        AssertSameBaseline(serial.Seeded!, fanned.Seeded!);

        var orphan = fanned.Seeded!.Rows.Single(r => r.Key == "orphan");
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(orphan.Value.Value!), Is.EqualTo("live"),
                "LWW must keep the highest-timestamp variant of a donor orphan");
            Assert.That(orphan.MergeMode, Is.EqualTo(LatticeMergeMode.OrSet),
                "the per-key merge mode must follow the value the LWW merge kept");
        });
    }

    /// <summary>
    /// The same equivalence where the colliding rows arrive the other way round
    /// in chain order - the live copy first, the stale one later. The incoming
    /// row loses, so the existing mode must survive. This is the tie-sensitive
    /// half of the adoption rule and the one a naive "last write wins the mode"
    /// fan-out would break.
    /// </summary>
    [Test]
    public async Task Fanned_out_fold_keeps_the_winning_rows_mode_when_the_later_leaf_loses()
    {
        static IReadOnlyList<LeafSnapshotRow> Rows(int leaf) => leaf switch
        {
            0 => [Row("orphan", "live", hlc: 30, LatticeMergeMode.OrSet)],
            3 => [Row("orphan", "stale", hlc: 10, LatticeMergeMode.LwwRegister)],
            _ => [Row($"k{leaf:D2}", "v", 20)],
        };

        var serial = CreateChain(leafCount: 6, Rows, foldConcurrency: 1);
        await serial.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);

        var fanned = CreateChain(leafCount: 6, Rows, foldConcurrency: 6);
        await fanned.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);

        AssertSameBaseline(serial.Seeded!, fanned.Seeded!);

        var orphan = fanned.Seeded!.Rows.Single(r => r.Key == "orphan");
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(orphan.Value.Value!), Is.EqualTo("live"));
            Assert.That(orphan.MergeMode, Is.EqualTo(LatticeMergeMode.OrSet),
                "a losing incoming row must not overwrite the surviving row's mode");
        });
    }

    private static void AssertSameBaseline(SnapshotShardBaseline expected, SnapshotShardBaseline actual)
    {
        Assert.Multiple(() =>
        {
            Assert.That(actual.Rows.Select(r => r.Key), Is.EqualTo(expected.Rows.Select(r => r.Key)),
                "key order must be identical, not merely the same set");
            Assert.That(actual.Rows.Select(r => r.MergeMode), Is.EqualTo(expected.Rows.Select(r => r.MergeMode)));
            Assert.That(
                actual.Rows.Select(r => r.Value.Value is null ? null : Encoding.UTF8.GetString(r.Value.Value)),
                Is.EqualTo(expected.Rows.Select(r => r.Value.Value is null ? null : Encoding.UTF8.GetString(r.Value.Value))));
            Assert.That(actual.RowBytes, Is.EqualTo(expected.RowBytes));
            Assert.That(actual.CapturedHeadPerPartition, Is.EqualTo(expected.CapturedHeadPerPartition));
        });
    }

    // --- the fan-out window is bounded ---

    /// <summary>
    /// The fan-out is a sliding window, not an unbounded dispatch: at most
    /// <see cref="LatticeOptions.MaxConcurrentSnapshotBaselineFolds"/> folds may
    /// be outstanding, and a slot only frees when its result is
    /// <em>consumed</em>. A plain semaphore gate would satisfy the first half
    /// and fail the second, letting folds run arbitrarily far ahead of a slow
    /// leaf at the head of the chain and pile every folded row set up in memory
    /// alongside the accumulating union.
    /// </summary>
    [Test]
    public async Task Fold_fan_out_never_exceeds_the_configured_window()
    {
        var harness = CreateChain(
            leafCount: 10,
            _ => [],
            foldConcurrency: 3,
            parkFolds: true);

        var capture = harness.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);

        await AwaitDispatchAsync(harness, 3);
        await AssertNoFurtherDispatchAsync(harness, 3,
            "only the window may be dispatched before any result is consumed");

        // Releasing the chain-head fold consumes one result, which frees exactly
        // one slot and admits exactly one more fold.
        harness.Gates[0].SetResult([]);
        await AwaitDispatchAsync(harness, 4);
        await AssertNoFurtherDispatchAsync(harness, 4,
            "consuming one result must admit exactly one more fold");

        // Completing an out-of-order fold must NOT admit more work: the window
        // slides on consumption, and leaf 1 has not been consumed yet.
        harness.Gates[2].SetResult([]);
        await AssertNoFurtherDispatchAsync(harness, 4,
            "a fold completing out of order must not slide the window");

        for (var i = 1; i < 10; i++)
            harness.Gates[i].TrySetResult([]);

        await capture;

        var dispatched = harness.FoldsDispatched;
        Assert.Multiple(() =>
        {
            Assert.That(dispatched, Has.Count.EqualTo(10), "every leaf must be folded exactly once");
            Assert.That(dispatched.Order(), Is.EqualTo(Enumerable.Range(0, 10)));
            Assert.That(harness.PeakConcurrentFolds, Is.LessThanOrEqualTo(3));
        });
    }

    [Test]
    public async Task A_single_leaf_chain_folds_without_fanning_out()
    {
        var harness = CreateChain(leafCount: 1, leaf => [Row($"k{leaf}", "v", 20)], foldConcurrency: 8);

        var result = await harness.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RowCount, Is.EqualTo(1));
            Assert.That(harness.PeakConcurrentFolds, Is.EqualTo(1),
                "the window must be clamped to the chain length");
        });
    }

[Test]
    public async Task A_shard_whose_leaves_hold_no_rows_captures_an_empty_baseline()
    {
        // The degenerate end of the fold pass. A shard root always has at least
        // one leaf (a null root materialises one), so "empty" here means every
        // leaf folds to zero rows: the union stays empty, the fan-out has
        // nothing to reorder, and the baseline is still seeded so a cursor
        // opened against it reads an empty shard rather than faulting.
        var harness = CreateChain(leafCount: 3, _ => []);

        var result = await harness.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RowCount, Is.Zero);
            Assert.That(result.CapturedHeadPerPartition, Is.EqualTo(new[] { 99L }));
            Assert.That(harness.Seeded, Is.Not.Null);
            Assert.That(harness.Seeded!.Rows, Is.Empty);
            Assert.That(harness.FoldsDispatched, Has.Count.EqualTo(3),
                "every leaf is still folded; an empty result is not a short circuit");
        });
    }

    [Test]
    public void An_empty_baseline_token_is_rejected_before_the_shard_is_held()
    {
        var harness = CreateChain(leafCount: 1, _ => []);

        Assert.Throws<ArgumentException>(() =>
            harness.Grain.CaptureSnapshotBaselineAsync(Guid.Empty, CancellationToken.None),
            "the guard must fault synchronously, before the walk is armed");
    }

    // --- the hard stall ceiling (issue 2003) now covers the capture ---

    /// <summary>
    /// A freeze that never returns used to hold the non-reentrant shard root
    /// indefinitely. Abandoning it is safe: the capture is read-only right up to
    /// its closing <c>SeedAsync</c>, so nothing is half-applied, and the failed
    /// snapshot open is retried with a fresh baseline token. Point-in-time
    /// consistency is not at stake, because it comes from <c>capturedHead</c>
    /// dominating every frontier rather than from the hold.
    /// </summary>
    [Test]
    public void A_freeze_that_never_returns_is_faulted_by_the_hard_ceiling()
    {
        var harness = CreateChain(
            leafCount: 3,
            _ => [],
            stallDuration: TimeSpan.FromMilliseconds(250),
            parkFreezeAtLeaf: 1);

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Operation, Is.EqualTo(nameof(ShardRootGrain.CaptureSnapshotBaselineAsync)));
            Assert.That(ex.Phase, Is.EqualTo("leaf-walk"),
                "the stall must be attributed to the freeze pass");
            Assert.That(ex.LeavesVisited, Is.EqualTo(1),
                "leaf 0 was frozen; the parked read is leaf 1");
            Assert.That(ex.ShardIndex, Is.Zero);
            Assert.That(ex.TreeId, Is.EqualTo(TreeId));
        });
    }

    /// <summary>
    /// The fold pass gets its own phase rather than reusing <c>leaf-walk</c>,
    /// because the serial walk's "the read in flight was leaf N + 1" is not true
    /// of a fanned-out pass with several folds outstanding. A stall must not
    /// point an operator at one leaf when it was waiting on several.
    /// </summary>
    [Test]
    public void A_fold_that_never_returns_is_faulted_and_attributed_to_the_fold_phase()
    {
        var harness = CreateChain(
            leafCount: 3,
            _ => [],
            foldConcurrency: 2,
            parkFolds: true,
            stallDuration: TimeSpan.FromMilliseconds(250));

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Operation, Is.EqualTo(nameof(ShardRootGrain.CaptureSnapshotBaselineAsync)));
            Assert.That(ex.Phase, Is.EqualTo("baseline-fold"),
                "a stalled fold must not be reported as a chain read");
            Assert.That(ex.LeavesVisited, Is.EqualTo(3),
                "the whole chain was frozen before the fold pass began");
        });

        foreach (var gate in harness.Gates)
            gate.TrySetResult([]);
    }

    /// <summary>
    /// Bounds a wait that should succeed. The wait itself completes on the
    /// dispatch event, so a healthy run never spends this; it exists only to
    /// turn a broken window into a named failure instead of a hang.
    /// </summary>
    private static readonly TimeSpan DispatchTimeout = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long to hold still to prove no <em>further</em> fold is dispatched.
    /// A negative cannot be event-driven, so this is the one place a duration is
    /// load-bearing - and it is safe to keep short, because a window that
    /// over-dispatches does so the instant a gate is released rather than after
    /// a delay.
    /// </summary>
    private static readonly TimeSpan QuietPeriod = TimeSpan.FromMilliseconds(250);

    /// <summary>Waits for the <paramref name="count"/>th fold to be dispatched.</summary>
    private static async Task AwaitDispatchAsync(CaptureHarness harness, int count)
    {
        var reached = harness.DispatchCountReaches(count);

        if (await Task.WhenAny(reached, Task.Delay(DispatchTimeout)) != reached)
        {
            Assert.Fail(
                $"timed out waiting for fold {count} to be dispatched; " +
                $"only {harness.FoldsDispatched.Count} were");
        }
    }

    /// <summary>
    /// Asserts exactly <paramref name="expected"/> folds have been dispatched,
    /// and that no further one follows while the window is held still.
    /// </summary>
    private static async Task AssertNoFurtherDispatchAsync(
        CaptureHarness harness, int expected, string because)
    {
        var overshoot = harness.DispatchCountReaches(expected + 1);
        var raced = await Task.WhenAny(overshoot, Task.Delay(QuietPeriod));

        Assert.Multiple(() =>
        {
            Assert.That(raced, Is.Not.SameAs(overshoot), because);
            Assert.That(harness.FoldsDispatched, Has.Count.EqualTo(expected), because);
        });
    }
}
