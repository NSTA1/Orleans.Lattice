using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the cold-activation hydration admission gate
/// (issue #2765).
/// <para>
/// The defect these pin is a <b>managed</b> <see cref="OutOfMemoryException"/>.
/// Orleans activates many leaves concurrently on a cold start and each one
/// materialised its whole persisted snapshot blob with nothing bounding how
/// many did so at once, so the aggregate transient allocation was a function of
/// the activation storm rather than of the process's heap. Under a container
/// memory limit the .NET heap hard limit is sized from the cgroup, so the
/// process is never OOM-killed: it throws, exits 0, and restarts, which is why
/// the loop read for a long time as an unexplained clean exit. Nothing ever
/// divided the oversized leaves, so every restart faced the same corpus and the
/// write-ahead log never shrank.
/// </para>
/// <para>
/// Three clauses are load-bearing and each is tested separately here, because
/// they fail independently: the gate that bounds aggregate in-flight bytes, the
/// escalation break that stops a memory failure asking for more memory, and the
/// durable size hint that stops every restart re-learning what the last one
/// measured. A test that only asserted "the process survives" would be
/// satisfied by any one of them and would tell you nothing about the other two.
/// </para>
/// </summary>
[TestFixture]
public class BPlusLeafGrainColdActivationAdmissionTests
{
    private static string UniqueAdmissionTree()
        => $"tree-admission-{Guid.NewGuid():N}";

    /// <summary>
    /// Builds a leaf wired to an explicit admission gate. The gate is injected
    /// through the activation's services rather than a static, so concurrently
    /// running fixtures cannot see each other's budget.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot, ILeafReplayCoordinatorGrain Coordinator) CreateGatedGrain(
        LeafSnapshotHydrationAdmission admission,
        string treeId,
        Func<Task<LeafSnapshotBlob?>> load,
        long maxLeafBytes = 64L * 1024 * 1024,
        FallOffLogDecision? fallOffDecision = null,
        long projectionCheckpoint = 0L,
        long walHead = 10L)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>()).Returns(_ => load());

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(walHead));
        coord.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(admission);
        if (fallOffDecision is { } decision)
        {
            var detector = Substitute.For<ILatticeFallOffLogDetector>();
            detector.ClassifyAsync(
                    Arg.Any<string>(),
                    Arg.Any<int>(),
                    Arg.Any<long>(),
                    Arg.Any<TimeSpan>(),
                    Arg.Any<ResolvedLatticeOptions>(),
                    Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(decision));
            sc.AddSingleton(detector);
        }

        var services = sc.BuildServiceProvider();

        var leafKey = Guid.NewGuid();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        state.State.ProjectionCheckpointOffset = projectionCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                MaxLeafBytes = maxLeafBytes,
                WalPartitions = 1,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state, snapshotStub, coord);
    }

    private static LeafSnapshotBlob NewSizedBlob(long offset, long snapshotBytes)
        => new()
        {
            SnapshotOffset = offset,
            Rows = new List<LeafSnapshotRow>
            {
                new("a", new LwwValue<byte[]> { Value = [1], Timestamp = HybridLogicalClock.Zero }),
            },
            CapturedAtTicks = 1L,
            SnapshotBytes = snapshotBytes,
        };

    private static async Task SpinUntilAsync(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Timed out waiting for {because}.");
            }

            await Task.Delay(10);
        }
    }

    // ---------------------------------------------------------------------
    // Clause 1 - the gate bounds AGGREGATE in-flight hydration bytes.
    // ---------------------------------------------------------------------

    [Test]
    public async Task Concurrent_cold_hydrations_are_bounded_by_the_aggregate_byte_budget()
    {
        // The headline regression. Six leaves activate together against a budget
        // that fits three of them, so exactly three may be materialising a blob
        // at any instant and the other three wait. Before the gate, all six read
        // their blobs simultaneously, which is the shape that crossed the heap
        // hard limit in the deployed process.
        const long PerLeafBytes = 100L;
        const int Capacity = 3;
        const int Leaves = 6;

        var admission = new LeafSnapshotHydrationAdmission(
            LeafSnapshotHydrationAdmission.ToHeapCostBytes(PerLeafBytes) * Capacity);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = 0;
        var peakInFlight = 0;

        async Task<LeafSnapshotBlob?> LoadAsync()
        {
            var now = Interlocked.Increment(ref inFlight);
            int observed;
            do
            {
                observed = Volatile.Read(ref peakInFlight);
            }
            while (now > observed
                && Interlocked.CompareExchange(ref peakInFlight, now, observed) != observed);

            try
            {
                await release.Task;
                return NewSizedBlob(offset: 10L, snapshotBytes: PerLeafBytes);
            }
            finally
            {
                Interlocked.Decrement(ref inFlight);
            }
        }

        var treeId = UniqueAdmissionTree();
        var rehydrates = new List<Task<bool>>(Leaves);
        for (var i = 0; i < Leaves; i++)
        {
            var (grain, state, _, _) = CreateGatedGrain(admission, treeId, LoadAsync);

            // A known per-leaf size, so the arithmetic under test is the gate's
            // and not an estimate the test cannot predict.
            state.State.SnapshotLoadHintBytes = PerLeafBytes;
            rehydrates.Add(Task.Run(() => grain.TryRehydrateFromSnapshotAsync(CancellationToken.None)));
        }

        await SpinUntilAsync(
            () => admission.AdmittedCount + admission.QueuedCount == Leaves,
            "every leaf to reach the admission gate");

        Assert.Multiple(() =>
        {
            Assert.That(admission.AdmittedCount, Is.EqualTo(Capacity),
                "the budget admits exactly three concurrent hydrations");
            Assert.That(admission.QueuedCount, Is.EqualTo(Leaves - Capacity),
                "and the remaining three wait rather than all reading their blobs at once, "
                + "which is the unbounded fan-out that exhausted the heap");
            Assert.That(admission.InFlightBytes, Is.LessThanOrEqualTo(admission.BudgetBytes),
                "reserved bytes never exceed the budget");
        });

        release.SetResult();
        var results = await Task.WhenAll(rehydrates);

        Assert.Multiple(() =>
        {
            Assert.That(results, Is.All.True, "every leaf must still rehydrate - the gate defers work, "
                + "it does not drop it");
            Assert.That(Volatile.Read(ref peakInFlight), Is.LessThanOrEqualTo(Capacity),
                "and at no point were more blobs being materialised than the budget allows");
            Assert.That(admission.InFlightBytes, Is.Zero,
                "every lease is released, so a later storm starts from a clean budget rather than "
                + "inheriting a leak");
        });
    }

    [Test]
    public async Task A_snapshot_larger_than_the_whole_budget_is_still_admitted_alone()
    {
        // Forward progress. The corpus this issue is about is precisely the one
        // whose leaves exceed their own size bound, so a gate that refused a
        // claim larger than its budget would convert a crash loop into a
        // permanent stall - the leaf could never activate, so it could never
        // divide, so it could never shrink.
        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: 1_000L);

        using var lease = await admission.AcquireAsync(50_000_000L, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(lease.HeldBytes,
                Is.EqualTo(LeafSnapshotHydrationAdmission.ToHeapCostBytes(50_000_000L)),
                "and it reserves the heap the hydration costs, not the bytes on disk");
            Assert.That(lease.Queued, Is.False,
                "the sole occupant is admitted immediately; waiting for a budget it can never fit "
                + "inside would be waiting forever");
        });
    }

    [Test]
    public async Task A_queued_claim_is_never_barged_by_a_later_smaller_one()
    {
        // Fairness is load-bearing, not decoration. Admission is by arrival, so
        // a stream of small claims cannot starve the oversized leaf behind them,
        // and the oversized leaves are the only ones whose division ever brings
        // the corpus back under bound.
        // Budget leaves 100 heap bytes spare while the first claim is held, so
        // the tiny claim below genuinely FITS. That is what makes the assertion
        // non-vacuous: only the arrival-order rule holds it back.
        var admission = new LeafSnapshotHydrationAdmission(
            LeafSnapshotHydrationAdmission.ToHeapCostBytes(100L) + 100L);

        var first = await admission.AcquireAsync(100L, CancellationToken.None);
        var large = admission.AcquireAsync(100L, CancellationToken.None);
        var small = admission.AcquireAsync(1L, CancellationToken.None);

        await SpinUntilAsync(() => admission.QueuedCount == 2, "both later claims to queue");

        Assert.Multiple(() =>
        {
            Assert.That(large.IsCompleted, Is.False, "the large claim does not fit yet");
            Assert.That(small.IsCompleted, Is.False,
                "and the small one - which WOULD fit in the spare budget - must not overtake it");
        });

        first.Dispose();
        var admittedLarge = await large;

        Assert.That(admittedLarge.Queued, Is.True, "the head of the queue is admitted first");

        admittedLarge.Dispose();
        (await small).Dispose();
    }

    [Test]
    public async Task Reconciling_a_measured_size_tightens_admission_within_the_same_storm()
    {
        // An estimate is enough to get admitted, but an estimate that is never
        // corrected bounds nothing: a run of underestimates would let the gate
        // admit far past its budget, which is the unbounded behaviour with extra
        // steps. Correcting on measurement clamps the rest of THIS cold start,
        // not merely the next one.
        var admission = new LeafSnapshotHydrationAdmission(
            LeafSnapshotHydrationAdmission.ToHeapCostBytes(100L));

        using var lease = await admission.AcquireAsync(10L, CancellationToken.None);
        Assert.That(admission.InFlightBytes,
            Is.EqualTo(LeafSnapshotHydrationAdmission.ToHeapCostBytes(10L)));

        lease.Reconcile(100L);

        Assert.That(admission.InFlightBytes,
            Is.EqualTo(LeafSnapshotHydrationAdmission.ToHeapCostBytes(100L)),
            "the reservation now reflects what was actually loaded");

        var next = admission.AcquireAsync(10L, CancellationToken.None);
        await SpinUntilAsync(() => admission.QueuedCount == 1, "the next claim to queue");

        Assert.That(next.IsCompleted, Is.False,
            "so the next claim waits - before reconciliation the gate still believed it had nine "
            + "tenths of its budget spare that the loaded blob had already consumed");

        lease.Reconcile(10L);
        (await next).Dispose();
    }

    [Test]
    public void The_budget_is_derived_from_the_heap_hard_limit_and_never_configured()
    {
        // Self-sizing is a requirement, not an implementation detail: a bound
        // that only works once an operator sets a knob cannot fix a process that
        // is already restart-looping, and raising the container's memory limit
        // was explicitly ruled out as a remedy.
        Assert.Multiple(() =>
        {
            Assert.That(
                LeafSnapshotHydrationAdmission.ResolveBudgetBytes(8L * 1024 * 1024 * 1024),
                Is.EqualTo(1024L * 1024 * 1024),
                "one eighth of the heap hard limit, leaving the rest for resident projections, the "
                + "replay path and the foreground request path, all of which are live during a cold start");
            Assert.That(
                LeafSnapshotHydrationAdmission.ResolveBudgetBytes(8L * 1024 * 1024),
                Is.EqualTo(LeafSnapshotHydrationAdmission.MinimumBudgetBytes),
                "a very small heap takes the floor rather than serialising every hydration end to end");
            Assert.That(
                LeafSnapshotHydrationAdmission.ResolveBudgetBytes(0L),
                Is.EqualTo(LeafSnapshotHydrationAdmission.UnknownHeapLimitBudgetBytes),
                "and an unreported limit takes a fixed conservative bound - an unknown ceiling is not "
                + "licence to be unbounded, since unbounded is the defect");
        });
    }

    [Test]
    public void A_claim_is_sized_by_the_peak_heap_a_hydration_costs_not_by_bytes_on_disk()
    {
        // The amplification, pinned. Leaf state is persisted by the ADO.NET
        // provider as JSON TEXT, so reading a blob back is not a byte copy: the
        // live failing stack shows a UTF-16 System.String of the whole document
        // built by String.Ctor from a char[] that is still live while it copies,
        // and Newtonsoft's object graph on top of both. Four multiples are spent
        // before the parse allocates anything, which is why the OutOfMemory
        // lands in String.Ctor rather than in the blob read.
        //
        // A gate sized against stored bytes would therefore be several times too
        // permissive and would fail in exactly the same way on the next deploy -
        // the single most likely way this fix could look right and not work.
        Assert.Multiple(() =>
        {
            Assert.That(
                LeafSnapshotHydrationAdmission.ToHeapCostBytes(226L * 1000 * 1000),
                Is.GreaterThan(1_000_000_000L),
                "the measured 226 MB maximum costs over a gigabyte of peak heap, so against the "
                + "deployed 1.5 GiB budget exactly one such hydration is admitted at a time");
            Assert.That(
                LeafSnapshotHydrationAdmission.ToHeapCostBytes(613L * 1024),
                Is.LessThan(4L * 1024 * 1024),
                "while the 613 KB mean leaf costs a few megabytes, so several hundred ordinary "
                + "leaves still activate together - the gate binds on the oversized population only");
            Assert.That(
                LeafSnapshotHydrationAdmission.ToHeapCostBytes(long.MaxValue / 2),
                Is.EqualTo(long.MaxValue),
                "and the conversion saturates rather than overflowing negative, which would make an "
                + "enormous claim look free and admit it unconditionally");
            Assert.That(LeafSnapshotHydrationAdmission.ToHeapCostBytes(0L), Is.Zero);
        });
    }

    // ---------------------------------------------------------------------
    // Clause 2 - a memory failure must not escalate into a larger allocation.
    // ---------------------------------------------------------------------

    [Test]
    public void A_hydration_that_exhausts_memory_declines_the_activation_instead_of_replaying_the_whole_WAL()
    {
        // The feedback loop, pinned. A leaf that failed for want of heap used to
        // fall through to the -1 replay-start override and replay its entire
        // readable WAL window, which allocates MORE than the load that had just
        // failed. Under a container limit that is positive feedback, and it is
        // what turned a memory shortage into a restart loop.
        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: 1024L * 1024);
        var treeId = UniqueAdmissionTree();
        var (grain, _, _, coord) = CreateGatedGrain(
            admission,
            treeId,
            () => throw new InvalidOperationException(
                "activation failed",
                new OutOfMemoryException("Exception of type 'System.OutOfMemoryException' was thrown.")));

        var thrown = Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.TreeId, Is.EqualTo(treeId));
            Assert.That(thrown.BudgetBytes, Is.EqualTo(admission.BudgetBytes),
                "the verdict carries the budget it was measured against, so an operator reading one "
                + "line can tell a genuinely oversized corpus from a badly sized host");
        });

        // The negative assertion is the one that matters. Without it, a fix that
        // threw AND still replayed would pass.
        coord.DidNotReceive().ReadSliceAsync(
            -1L,
            Arg.Any<long>(),
            Arg.Any<int>(),
            Arg.Any<CancellationToken>());

        Assert.That(admission.InFlightBytes, Is.Zero,
            "and the reservation is released on the throwing path too, or one memory failure would "
            + "permanently shrink the budget for every leaf after it");
    }

    [Test]
    public async Task An_ordinary_storage_fault_still_falls_through_to_the_WAL_replay()
    {
        // The control, and it is what keeps the clause above honest. Only the
        // MEMORY arm is self-defeating: when the store is merely unreachable
        // there is nothing wrong with replaying the WAL, and it may well
        // succeed. A change that made every load failure fatal would pass the
        // test above and fail this one.
        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: 1024L * 1024);
        var (grain, state, _, coord) = CreateGatedGrain(
            admission,
            UniqueAdmissionTree(),
            () => throw new InvalidOperationException("storage unreachable"));

        state.State.ProjectionCheckpointOffset = 5L;

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        await coord.Received().ReadSliceAsync(
            -1L,
            Arg.Any<long>(),
            Arg.Any<int>(),
            Arg.Any<CancellationToken>());
    }

    // ---------------------------------------------------------------------
    // Clause 3 - the measured size is banked durably.
    // ---------------------------------------------------------------------

    [Test]
    public async Task A_successful_hydration_banks_the_measured_size_for_the_next_activation()
    {
        // Durability of progress. Without this the gate re-learns every leaf's
        // size by overshooting on every single restart, so a process that died
        // part-way through a storm restarts knowing exactly nothing more than
        // the one before it.
        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: 1024L * 1024);
        var (grain, state, _, _) = CreateGatedGrain(
            admission,
            UniqueAdmissionTree(),
            () => Task.FromResult<LeafSnapshotBlob?>(NewSizedBlob(offset: 10L, snapshotBytes: 4242L)));

        Assert.That(state.State.SnapshotLoadHintBytes, Is.Zero, "nothing observed yet");

        var rehydrated = await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.True);
            Assert.That(state.State.SnapshotLoadHintBytes, Is.EqualTo(4242L),
                "the observed wire size is stamped onto the persisted state, so it rides the next "
                + "ordinary write rather than costing a write of its own during the storm");
        });
    }

    /// <summary>
    /// The half of clause 3 that makes progress genuinely survive a restart.
    /// The rehydrate-path stamp above records the size in memory only - it must,
    /// because forcing a state write per leaf during a cold-start storm would
    /// add back exactly the unbounded concurrent work being removed. So the
    /// value only becomes durable by riding a write the leaf was making anyway,
    /// and the capture path is that write.
    /// </summary>
    [Test]
    public async Task A_capture_banks_the_measured_size_onto_a_write_it_was_already_making()
    {
        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: 1024L * 1024);
        var (grain, state, snapshot, _) = CreateGatedGrain(
            admission,
            UniqueAdmissionTree(),
            () => Task.FromResult<LeafSnapshotBlob?>(null),
            fallOffDecision: FallOffLogDecision.SnapshotPending,
            projectionCheckpoint: 12L,
            walHead: 12L);

        LeafSnapshotBlob? saved = null;
        snapshot
            .When(x => x.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>()))
            .Do(ci => saved = ci.Arg<LeafSnapshotBlob>());

        // The hint observed at each persist, so the test can tell "stamped onto
        // the state object" from "stamped and actually written down" - which is
        // the whole difference between surviving a restart and not.
        var hintAtEachWrite = new List<long>();
        state.OnWriteState = s => hintAtEachWrite.Add(s.SnapshotLoadHintBytes);

        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = [1, 2, 3, 4, 5],
            Timestamp = HybridLogicalClock.Zero,
        };

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        await ((IBPlusLeafGrain)grain).CaptureSnapshotAsync();

        Assert.That(saved, Is.Not.Null,
            "precondition: the capture must actually have reached the store, otherwise there "
            + "is no measured size to bank and this test would pass for the wrong reason.");

        var measured = saved!.EncodedRows is { Length: > 0 } frame
            ? frame.Length
            : saved.SnapshotBytes;

        Assert.That(measured, Is.GreaterThan(0),
            "precondition: a zero-sized capture would make the assertions below vacuous.");

        Assert.That(hintAtEachWrite, Does.Not.Contain(measured),
            "precondition, and the reason the assertion below is worth making: the capture "
            + "itself buys no write. Like the durable snapshot-coverage record stamped beside "
            + "it, the hint reaches storage only on the leaf's NEXT ordinary persist, and that "
            + "is deliberate - forcing a write per leaf here would add back exactly the "
            + "unbounded concurrent work this issue removes.");

        // An ordinary checkpoint advance: the persist the leaf was going to make
        // anyway, which is the write the hint is designed to ride.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(13L, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SnapshotLoadHintBytes, Is.EqualTo(measured),
                "the size the leaf will have to read back is learned at the one moment it is "
                + "known for free - immediately after the blob has been written.");
            Assert.That(hintAtEachWrite, Does.Contain(measured),
                "and it must land on PERSISTED state rather than an activation-local field, or "
                + "a restart inherits nothing and the gate overshoots every leaf all over "
                + "again. That failure is invisible in memory: a transient stamp is plainly "
                + "there for the rest of this activation's life.");
        });
    }

    [Test]
    public async Task A_banked_size_hint_sizes_the_next_activations_claim()
    {
        // Stamping the hint is worthless if nothing reads it, and "it is
        // persisted" and "it is used" fail independently. Here the banked hint
        // is far larger than the configured leaf bound, so a claim sized from
        // the hint queues behind an existing lease while a claim sized from the
        // bound would have been admitted immediately.
        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: 1_000L);
        using var occupant = await admission.AcquireAsync(180L, CancellationToken.None);

        var (grain, state, _, _) = CreateGatedGrain(
            admission,
            UniqueAdmissionTree(),
            () => Task.FromResult<LeafSnapshotBlob?>(NewSizedBlob(offset: 10L, snapshotBytes: 10L)),
            maxLeafBytes: 10L);

        state.State.SnapshotLoadHintBytes = 5_000L;

        var rehydrate = Task.Run(() => grain.TryRehydrateFromSnapshotAsync(CancellationToken.None));
        await SpinUntilAsync(() => admission.QueuedCount == 1, "the hinted claim to queue");

        Assert.That(rehydrate.IsCompleted, Is.False,
            "a claim sized from the 10-byte leaf bound would have fitted in the spare budget; only "
            + "a claim sized from the banked 5000-byte hint queues here");

        occupant.Dispose();
        Assert.That(await rehydrate, Is.True);
    }

    // ---------------------------------------------------------------------
    // Observability.
    // ---------------------------------------------------------------------

    [Test]
    public async Task The_admission_series_is_primed_to_zero_so_a_quiet_gate_is_distinguishable_from_an_absent_one()
    {
        // A counter that is only ever incremented reads as an absent series in
        // three different situations - the gate never queued, the build does not
        // carry the gate, nothing has activated yet - and those have opposite
        // responses. Priming turns the healthy steady state into an explicit
        // zero.
        var treeId = UniqueAdmissionTree();
        var records = new List<(string Outcome, long Value)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSnapshotHydrationAdmissions,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var copied = tags.ToArray();
                if (!copied.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId))
                {
                    return;
                }

                var outcome = copied.Single(t => t.Key == LatticeMetrics.TagOutcome).Value as string;
                lock (records)
                {
                    records.Add((outcome ?? string.Empty, value));
                }
            }));

        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: 1024L * 1024);
        var (grain, _, _, _) = CreateGatedGrain(
            admission,
            treeId,
            () => Task.FromResult<LeafSnapshotBlob?>(NewSizedBlob(offset: 10L, snapshotBytes: 10L)));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);

        List<(string Outcome, long Value)> observed;
        lock (records)
        {
            observed = [.. records];
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Where(r => r.Value == 0).Select(r => r.Outcome),
                Is.EquivalentTo(new[]
                {
                    LatticeMetrics.SnapshotHydrationAdmittedImmediately.Value as string,
                    LatticeMetrics.SnapshotHydrationQueued.Value as string,
                }),
                "BOTH arms are primed, or the ratio is unreadable until each has happened at least once");
            Assert.That(
                observed.Where(r => r.Value == 1).Select(r => r.Outcome),
                Is.EquivalentTo(new[] { LatticeMetrics.SnapshotHydrationAdmittedImmediately.Value as string }),
                "and the hydration that actually happened is counted on the immediate arm");
        });
    }
}
