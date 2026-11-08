using System.Collections.Concurrent;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Retention of a refused replay's queue position (issue #6006, the remainder of
/// #5992). A wait-arm refusal of an Interactive replay refuses its callers fast,
/// and the replay's own gate wait stays parked in arrival order, bounded by R in
/// count and by <see cref="BPlusLeafGrain.RetainedReplayMaxAge"/> in time.
/// Refinement: <c>RetainOnRefusal</c>, <c>ExpireRetained</c> and the retention
/// properties of <c>spec/scheduling/ReplayPermits</c>.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly TimeSpan RetentionObservationTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A seeded queue whose every permit is held and whose fresh smoothed wait is
    /// at the maximum: an Interactive arrival is refused on the wait arm and
    /// retained. Returns the seeded admitted-waiter count.
    /// </summary>
    private static async Task<int> SeedRetainableRefusalAsync()
    {
        await SeedAdmittedWaitersAsync(_ => 1);
        return BPlusLeafGrain.QueuedReplayPermitWaitersForTest;
    }

    /// <summary>Activates a fresh leaf against the seeded refusal, expecting its caller to be refused.</summary>
    private static BPlusLeafGrain ActivateRetained(string treeId, out LatticeSaturatedException refusal)
    {
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        state.State.TreeId = treeId;
        refusal = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None))!;
        return grain;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow + RetentionObservationTimeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                Assert.Fail(because);
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// Returns the held permits, lets every retained replay leave, and only then
    /// clears the seeded queue, so no departure lands on a cleared counter.
    /// </summary>
    /// <remarks>
    /// A retained wait that cannot be drained would sit parked on the shared gate
    /// and take a later test's permit, so this replaces the gate before it fails:
    /// the failure stays with the test that caused it.
    /// </remarks>
    private static async Task DrainRetainedReplaysAsync(int seededQueued)
    {
        ReturnSeededHeldReplayPermits();
        var deadline = DateTime.UtcNow + RetentionObservationTimeout;
        while (BPlusLeafGrain.RetainedReplayWaitersForTest != 0
            || BPlusLeafGrain.QueuedReplayPermitWaitersForTest > seededQueued)
        {
            if (DateTime.UtcNow > deadline)
            {
                var retained = BPlusLeafGrain.RetainedReplayWaitersForTest;
                var queued = BPlusLeafGrain.QueuedReplayPermitWaitersForTest;
                BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
                Assert.Fail(
                    $"teardown: every retained replay must leave the gate (retained {retained}, queued {queued} "
                    + $"against a seeded {seededQueued}); the gate was replaced so later tests start clean");
            }

            await Task.Delay(10);
        }

        ClearSeededAdmittedWaiters();
        BPlusLeafGrain.RetainedReplayMaxAgeForTest = null;
    }

    [Test]
    [NonParallelizable]
    public async Task A_wait_exceeded_refusal_keeps_the_interactive_replay_parked_in_its_position()
    {
        var seeded = await SeedRetainableRefusalAsync();
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var treeId = UniqueReplayPermitTree();
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            BPlusLeafGrain grain;
            LatticeSaturatedException refusal;
            using (ListenForSaturationRefusals(treeId, refusals))
            {
                grain = ActivateRetained(treeId, out refusal);
            }

            Assert.Multiple(() =>
            {
                Assert.That(refusal.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission),
                    "the caller is refused fast, exactly as before retention");
                Assert.That(refusals.Select(ArmOf), Is.EqualTo(new[] { "wait_exceeded" }),
                    "the caller's refusal is the wait arm's, counted once");
                Assert.That(grain.ReplayRetainedForTest, Is.True, "the replay keeps its gate position");
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.EqualTo(1));
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(seeded + 1),
                    "a retained wait is a queued waiter");
                Assert.That(grain.ReplayBarrierForTest, Is.Not.Null.And.Property(nameof(Task.IsCompleted)).False,
                    "the barrier stays armed behind the parked wait");
                Assert.That(gate.CurrentCount, Is.Zero, "a retained wait takes no permit");
            });

            Assert.ThrowsAsync<LatticeSaturatedException>(async () => await grain.GetAsync("k"),
                "a retry that finds the replay retained but not admitted is refused fast");

            gate.Release();
            _seededHeldReplayPermits--;
            await WaitUntilAsync(() => grain.ReplayBarrierForTest is null or { IsCompleted: true },
                "the next released permit admits the retained replay");

            Assert.Multiple(() =>
            {
                Assert.That(grain.ReplayBarrierForTest?.IsCompletedSuccessfully ?? true, Is.True);
                Assert.That(grain.ReplayRetainedForTest, Is.False);
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.Zero);
            });
            Assert.DoesNotThrowAsync(async () => await grain.GetAsync("k"),
                "once admitted, callers join the barrier as before");
        }
        finally
        {
            await DrainRetainedReplaysAsync(seeded);
        }
    }

    [Test]
    [NonParallelizable]
    public void Retention_stops_at_the_circulating_permits_and_a_walk_down_evicts_nothing()
    {
        const int Ceiling = 4;
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(Ceiling, queued: 0);
            for (var i = 0; i < Ceiling; i++)
                Assert.That(BPlusLeafGrain.TryReserveReplayRetention(), Is.True, "below R a refusal is retained");
            Assert.That(BPlusLeafGrain.TryReserveReplayRetention(), Is.False,
                "no new retention once the retained count has reached R, the permits in circulation");

            // A walk-down to two circulating permits.
            BPlusLeafGrain.SeedWithheldReplayPermitsForTest(2);
            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.EqualTo(Ceiling),
                    "a walk-down evicts nothing already retained");
                Assert.That(BPlusLeafGrain.TryReserveReplayRetention(), Is.False,
                    "and admits no new retention while the retained count is at or above the new R");
            });

            for (var i = 0; i < 3; i++)
                BPlusLeafGrain.ReleaseReplayRetention();
            Assert.That(BPlusLeafGrain.TryReserveReplayRetention(), Is.True, "one retained, below the new R of two");
            Assert.That(BPlusLeafGrain.TryReserveReplayRetention(), Is.False, "two retained, at the new R");
            Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.EqualTo(2));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_retained_replay_is_dropped_at_its_maximum_age_and_never_admitted_after_it()
    {
        var seeded = await SeedRetainableRefusalAsync();
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        BPlusLeafGrain.RetainedReplayMaxAgeForTest = TimeSpan.FromMilliseconds(200);
        try
        {
            var grain = ActivateRetained(UniqueReplayPermitTree(), out _);
            var barrier = grain.ReplayBarrierForTest;
            Assert.That(barrier, Is.Not.Null, "precondition: the retained replay holds its barrier");

            await WaitUntilAsync(() => barrier!.IsCompleted, "a retained wait must be dropped at its maximum age");

            var free = gate.CurrentCount;
            gate.Release();
            _seededHeldReplayPermits--;
            await Task.Delay(100);

            Assert.Multiple(() =>
            {
                Assert.That(barrier!.IsFaulted, Is.True, "the aged-out replay ends unadmitted");
                Assert.That(barrier.Exception?.InnerException, Is.TypeOf<LatticeSaturatedException>(),
                    "as the refusal its callers already received");
                Assert.That(grain.ReplayRetainedForTest, Is.False);
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.Zero, "the retention slot is returned");
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(seeded),
                    "the dropped wait leaves the queue");
                Assert.That(gate.CurrentCount, Is.EqualTo(free + 1),
                    "a permit released after the maximum age is not taken by the dropped wait");
                Assert.That(grain.ReplayAdmissionPhaseForTest,
                    Is.Not.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.HoldsPermit));
            });
        }
        finally
        {
            await DrainRetainedReplaysAsync(seeded);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Parked_retained_and_ordinary_waiters_are_granted_in_arrival_order()
    {
        var seeded = await SeedRetainableRefusalAsync();
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;

        // R is read from the ceiling; seed one wide enough for two retained
        // replays whatever this machine's CPU grant sized the gate to.
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(
            Math.Max(4, BPlusLeafGrain.ReplayConcurrencyCeilingForTest), seeded);
        try
        {
            // Arrival order: retained, ordinary, retained, ordinary. Each ordinary
            // waiter records, at the instant it is granted, which retained replays
            // are still parked.
            var first = ActivateRetained(UniqueReplayPermitTree(), out _);
            BPlusLeafGrain? third = null;
            var second = gate.WaitAsync().ContinueWith(
                _ => (FirstParked: first.ReplayRetainedForTest, ThirdParked: third!.ReplayRetainedForTest),
                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            third = ActivateRetained(UniqueReplayPermitTree(), out _);
            var fourth = gate.WaitAsync().ContinueWith(
                _ => third.ReplayRetainedForTest,
                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

            Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.EqualTo(2), "precondition: both replays retained");

            // One permit at a time. A retained replay returns its permit when it
            // completes, which serves the next waiter; an ordinary waiter keeps it
            // until the test returns it.
            gate.Release();
            _seededHeldReplayPermits--;
            var atSecond = await second.WaitAsync(RetentionObservationTimeout);
            gate.Release();
            var thirdParkedAtFourth = await fourth.WaitAsync(RetentionObservationTimeout);
            gate.Release();

            Assert.Multiple(() =>
            {
                Assert.That(atSecond.FirstParked, Is.False,
                    "the first retained replay, which arrived first, is granted before the ordinary waiter behind it");
                Assert.That(atSecond.ThirdParked, Is.True,
                    "and the ordinary waiter is granted before the retained replay that arrived after it");
                Assert.That(thirdParkedAtFourth, Is.False,
                    "the second retained replay is granted before the last ordinary waiter (#5992 G2: one queue, "
                    + "arrival order)");
            });
        }
        finally
        {
            await DrainRetainedReplaysAsync(seeded);
        }
    }

    [TestCase(2)]
    [TestCase(1)]
    [NonParallelizable]
    public void A_bulk_arrival_excludes_retained_entries_but_is_refused_at_the_interactive_bound(int depthPerPermit)
    {
        const int Ceiling = 4;
        var interactive = Ceiling * depthPerPermit;
        var bulk = Math.Max(1, interactive - Ceiling);
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            bool Admit(int queued, int retained, LatticeReplayAdmissionClass admissionClass)
            {
                BPlusLeafGrain.SeedReplayAdmissionStateForTest(Ceiling, queued);
                BPlusLeafGrain.SeedRetainedReplayWaitersForTest(retained);
                return BPlusLeafGrain.TryAdmitReplayPermitWaiter(depthPerPermit, admissionClass, out _, out _);
            }

            Assert.Multiple(() =>
            {
                Assert.That(Admit(interactive - 1, Math.Min(Ceiling, interactive - 1), LatticeReplayAdmissionClass.Bulk), Is.True,
                    "Bulk's own depth check excludes retained entries, which are Interactive work holding its place");
                Assert.That(Admit(interactive - 1, 0, LatticeReplayAdmissionClass.Bulk), Is.EqualTo(interactive - 1 < bulk),
                    "with nothing retained Bulk's bound is unchanged");
                Assert.That(Admit(interactive, interactive, LatticeReplayAdmissionClass.Bulk), Is.False,
                    "a Bulk arrival is refused once the total queue reaches the current Interactive bound, even when "
                    + "every entry is retained (a walk-down can leave more retained than the current R)");
                Assert.That(Admit(interactive - 1, Math.Min(Ceiling, interactive - 1), LatticeReplayAdmissionClass.Interactive), Is.True,
                    "Interactive still counts retained entries against its own bound");
                Assert.That(Admit(interactive, 0, LatticeReplayAdmissionClass.Interactive), Is.False);
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_cancelled_retained_waiter_returns_no_permit_and_leaves_no_residue()
    {
        var seeded = await SeedRetainableRefusalAsync();
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        try
        {
            var grain = ActivateRetained(UniqueReplayPermitTree(), out _);
            var barrier = grain.ReplayBarrierForTest!;
            var free = gate.CurrentCount;

            // The barrier's own lifetime, which a deactivation also cancels.
            grain.CancelReplayBarrierForTest();
            await WaitUntilAsync(() => barrier.IsCompleted, "cancelling the barrier ends the retained wait");

            Assert.Multiple(() =>
            {
                Assert.That(barrier.IsCanceled, Is.True);
                Assert.That(gate.CurrentCount, Is.EqualTo(free), "a cancelled retained waiter returns no permit");
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.Zero, "and no retention slot is left");
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(seeded), "and no queued residue");
                Assert.That(grain.ReplayRetainedForTest, Is.False);
            });

            gate.Release();
            _seededHeldReplayPermits--;
            Assert.That(gate.CurrentCount, Is.EqualTo(free + 1), "nothing is double-granted to the cancelled waiter");
        }
        finally
        {
            await DrainRetainedReplaysAsync(seeded);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Deactivating_a_retained_replay_drops_its_wait_and_leaves_the_permit_ledgers_unchanged()
    {
        var seeded = await SeedRetainableRefusalAsync();
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        try
        {
            var grain = ActivateRetained(UniqueReplayPermitTree(), out _);
            var barrier = grain.ReplayBarrierForTest!;
            Assert.That(grain.ResidentFootprintRegisteredForTest, Is.True,
                "a parked replay is resident, so the working set can shed it like any other leaf");

            var free = gate.CurrentCount;
            var withheld = BPlusLeafGrain.WithheldReplayPermitsForTest;
            await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"),
                CancellationToken.None);
            await WaitUntilAsync(() => barrier.IsCompleted, "a deactivation drops the retained wait");

            Assert.Multiple(() =>
            {
                Assert.That(gate.CurrentCount, Is.EqualTo(free), "the drop returns no permit it never held");
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(withheld));
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.Zero);
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(seeded));
                Assert.That(grain.ResidentFootprintRegisteredForTest, Is.False);
            });
        }
        finally
        {
            await DrainRetainedReplaysAsync(seeded);
        }
    }

    /// <summary>
    /// Re-arms a replay with its options cached, so the #5877 pre-check runs,
    /// against a fresh wait-arm reading that refuses an Interactive arrival.
    /// Returns the grain, its snapshot stub's rehydrate count, and the re-armed
    /// request's outcome.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, Func<int> Loads, Exception? Rearmed)> RearmAgainstARetainableReadingAsync(
        Action? duringRehydrate = null)
    {
        await SeedStalledQueuePastTheBoundAsync(sinceHolderProgress: null);
        var (grain, state, snapshot, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        state.State.TreeId = UniqueReplayPermitTree();
        int Loads() => snapshot.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILeafSnapshotStorageGrain.LoadAsync));

        // A stalled queue past the bound refuses on no_progress, which is never
        // retained, so the first replay faults and caches its options.
        Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));
        Assert.ThrowsAsync<LatticeSaturatedException>(async () => await grain.GetAsync("k"));
        Assert.That(Loads(), Is.EqualTo(1), "instrument validation: the first attempt rehydrates");

        // Now a fresh mean at the maximum, below the depth bound: the wait arm
        // refuses an Interactive arrival, and that refusal is retainable.
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, 1);
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            new LatticeOptions().WalReplayPermitMaxQueueWait, sinceLastProgress: TimeSpan.Zero);
        if (duringRehydrate is not null)
        {
            snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                duringRehydrate();
                return Task.FromResult<LeafSnapshotBlob?>(null);
            });
        }

        Exception? rearmed = null;
        try
        {
            await grain.GetAsync("k");
        }
        catch (Exception ex)
        {
            rearmed = ex;
        }

        return (grain, Loads, rearmed);
    }

    [Test]
    [NonParallelizable]
    public async Task A_retainable_arrival_passes_the_pre_rehydrate_check_and_parks_after_rehydrating()
    {
        try
        {
            var (grain, loads, rearmed) = await RearmAgainstARetainableReadingAsync();

            Assert.Multiple(() =>
            {
                Assert.That(rearmed, Is.TypeOf<LatticeSaturatedException>(), "the caller is still refused fast");
                Assert.That(loads(), Is.EqualTo(2),
                    "a retainable arrival is not refused before the rehydrate: it rehydrates, then keeps its position");
                Assert.That(grain.ReplayRetainedForTest, Is.True, "and it parks retained");
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.EqualTo(1));
            });
        }
        finally
        {
            await DrainRetainedReplaysAsync(1);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_retainable_reading_that_fills_up_during_the_rehydrate_is_refused_after_it_without_a_permit()
    {
        var limit = 0;
        try
        {
            // Retention fills between the pre-check and the acquisition: the
            // pre-check's reading is advisory, and the acquisition refuses.
            var (grain, loads, rearmed) = await RearmAgainstARetainableReadingAsync(() =>
            {
                limit = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
                BPlusLeafGrain.SeedRetainedReplayWaitersForTest(limit);
            });

            Assert.Multiple(() =>
            {
                Assert.That(rearmed, Is.TypeOf<LatticeSaturatedException>());
                Assert.That(loads(), Is.EqualTo(2), "instrument validation: the pre-check passed and the replay rehydrated");
                Assert.That(grain.ReplayRetainedForTest, Is.False, "a full retention is never retained anyway");
                Assert.That(BPlusLeafGrain.RetainedReplayWaitersForTest, Is.EqualTo(limit), "and takes no slot");
                Assert.That(grain.ReplayAdmissionPhaseForTest,
                    Is.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.RefusedAdmission),
                    "it is refused at acquisition, holding no permit");
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(1), "and never queued");
            });
        }
        finally
        {
            BPlusLeafGrain.SeedRetainedReplayWaitersForTest(0);
            await DrainRetainedReplaysAsync(1);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_retained_wait_never_enters_the_queue_wait_mean()
    {
        var seeded = await SeedRetainableRefusalAsync();
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        try
        {
            var grain = ActivateRetained(UniqueReplayPermitTree(), out _);
            var mean = BPlusLeafGrain.ReplayPermitWaitMeanForTest;
            var sample = BPlusLeafGrain.ReplayPermitWaitSampleTimestampForTest;

            await Task.Delay(50);
            gate.Release();
            _seededHeldReplayPermits--;
            await WaitUntilAsync(() => grain.ReplayBarrierForTest is null or { IsCompleted: true },
                "the retained replay is admitted");

            Assert.Multiple(() =>
            {
                Assert.That(grain.ReplayBarrierForTest?.IsCompletedSuccessfully ?? true, Is.True,
                    "instrument validation: the retained wait ended by acquiring");
                Assert.That(BPlusLeafGrain.ReplayPermitWaitMeanForTest, Is.EqualTo(mean),
                    "a retained wait is the longest by construction; folding it into the mean would refuse more");
                Assert.That(BPlusLeafGrain.ReplayPermitWaitSampleTimestampForTest, Is.EqualTo(sample),
                    "and it is not a fresh sample of the mean either");
            });
        }
        finally
        {
            await DrainRetainedReplaysAsync(seeded);
        }
    }
}
