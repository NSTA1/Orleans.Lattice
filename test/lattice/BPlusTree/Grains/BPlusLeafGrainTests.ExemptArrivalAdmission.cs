using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3299. A caller that is exempt from the replay permit admission bound -
/// a starvation drive of either origin - must not change whether a non-exempt
/// activation is admitted. The exemption has to remove the caller from the
/// <b>denominator</b> the bound is measured against, not only from the decision,
/// or the remedy for saturation contributes to the measure of saturation.
/// <para>
/// <b>Why these tests assert on the admission outcome and not on the waiter
/// count.</b> The count also rises for arrivals that acquire without ever
/// blocking, which is intended behaviour. An assertion that the count is lower
/// passes or fails for reasons that have nothing to do with this defect. These
/// tests therefore enable the bound, put the non-exempt caller one waiter below
/// it, and check whether that caller is admitted with an exempt competitor in
/// flight (it must be) and with a non-exempt competitor in flight (it must not
/// be, which proves the bound was live).
/// </para>
/// <para>
/// The production fix is b66d0a9e8 (#3568, issue #3480). Starvation drives
/// return from <c>AcquireReplayPermitAsync</c> before the arrival is registered.
/// Before that change they skipped only the admission check and then queued like
/// any other activation.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// A four-permit gate: a two-slot GC share, so a drive of either origin is
    /// eligible for a slot and is turned away only because the gate itself is full.
    /// </summary>
    private const int ExemptArrivalCeiling = 4;

    private static readonly TimeSpan ExemptArrivalSettle = TimeSpan.FromSeconds(10);

    // The origin is passed by name because StarvationDriveOrigin is internal and
    // a public test method cannot take it as a parameter.
    [TestCase(nameof(BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep))]
    [TestCase(nameof(BPlusLeafGrain.StarvationDriveOrigin.CoverageLagTimer))]
    [NonParallelizable]
    public async Task An_exempt_starvation_drive_in_flight_does_not_cause_a_non_exempt_activation_to_be_refused(
        string originName)
    {
        var origin = Enum.Parse<BPlusLeafGrain.StarvationDriveOrigin>(originName);
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var driveTreeId = UniqueStarvationDriveTree();
        var wal = new GrowingWal();
        var (driver, driverState, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: driveTreeId,
            persistedCheckpoint: -1,
            starvationDriveBudget: TimeSpan.FromSeconds(10),
            maxConcurrentReplays: ExemptArrivalCeiling);
        await ActivateAsync(driver);

        // Rows and no durable checkpoint, so a coverage-lag tick routes the leaf
        // to the drive (issue #3300). The WAL grows only after activation, so the
        // sweep drive has work to replay too.
        SeedRow(driver);
        wal.GrowTo(3);

        var (subject, subjectState, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        subjectState.State.TreeId = UniqueReplayPermitTree();

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldEveryReplayPermit(gate);
        var seeded = SeedOneBelowInteractiveBound();

        var reasons = new List<string>();
        Task? drive = null;
        Task? activation = null;
        try
        {
            using (ListenForDriverDeclinesOnTree(driveTreeId, reasons))
            {
                drive = origin == BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep
                    ? driver.DriveStarvedCheckpointAsync()
                    : driver.OnCoverageLagTimerTickAsync(CancellationToken.None);

                // This only synchronises; it asserts nothing. The drive has either
                // settled (it never queues, so a full gate refuses it at once) or it
                // has registered an arrival and is parked behind the full gate. In
                // both cases it has done whatever it will do to the admission input
                // before the non-exempt caller reads it.
                SpinWait.SpinUntil(
                    () => drive.IsCompleted || BPlusLeafGrain.QueuedReplayPermitWaitersForTest != seeded,
                    ExemptArrivalSettle);

                activation = LeafActivationHarness.ActivateAsync(subject, CancellationToken.None);
                var phase = AwaitAdmissionVerdict(subject, activation);

                Assert.That(phase, Is.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.QueuedForPermit),
                    $"a {origin} starvation drive is exempt from the replay admission bound. Its presence must "
                    + "not push a non-exempt activation that sits one waiter below the bound over it. A refusal "
                    + "here means the exempt caller was counted into the depth other callers are judged "
                    + $"against (issue #3299). Activation fault: {activation.Exception?.GetBaseException().Message}");

                ReleaseReplayPermits(gate, ref held);
                Assert.DoesNotThrowAsync(
                    async () => await activation.WaitAsync(ExemptArrivalSettle),
                    "an admitted activation must go on to acquire a released permit and finish its replay.");

                // Input check: the exempt caller really did contend. A drive that
                // never reached the gate would make the admission above prove
                // nothing about exempt callers.
                if (origin == BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep)
                {
                    var refusal = Assert.ThrowsAsync<LatticeSaturatedException>(
                        async () => await drive.WaitAsync(ExemptArrivalSettle),
                        "input check: the sweep drive must have reached the full gate and been turned away "
                        + "without queueing.");
                    Assert.That(refusal!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
                }
                else
                {
                    await drive.WaitAsync(ExemptArrivalSettle);
                    Assert.That(reasons, Does.Contain(TimerDriveRefusedReason),
                        "input check: the coverage-lag tick must have offered its drive to the full gate and "
                        + "had it refused.");
                }

                Assert.That(driverState.State.ProjectionCheckpointOffset, Is.EqualTo(-1L),
                    "a drive turned away at the gate must not have replayed anything.");
            }
        }
        finally
        {
            ReleaseReplayPermits(gate, ref held);
            await DrainQuietlyAsync(drive);
            await DrainQuietlyAsync(activation);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    /// <summary>
    /// The control for
    /// <see cref="An_exempt_starvation_drive_in_flight_does_not_cause_a_non_exempt_activation_to_be_refused"/>.
    /// The setup is the same, except that the competitor is an ordinary
    /// activation. It is refused, which proves the bound is live in that setup,
    /// so the admission the exempt case observes comes from the exemption.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_non_exempt_activation_in_flight_does_cause_the_next_non_exempt_activation_to_be_refused()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var (warm, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1,
            maxConcurrentReplays: ExemptArrivalCeiling);
        await ActivateAsync(warm);

        var (competitor, competitorState, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        competitorState.State.TreeId = UniqueReplayPermitTree();
        var (subject, subjectState, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        subjectState.State.TreeId = UniqueReplayPermitTree();

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldEveryReplayPermit(gate);
        SeedOneBelowInteractiveBound();

        Task? queued = null;
        Task? activation = null;
        try
        {
            queued = LeafActivationHarness.ActivateAsync(competitor, CancellationToken.None);
            Assert.That(AwaitAdmissionVerdict(competitor, queued),
                Is.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.QueuedForPermit),
                "precondition: the competitor sits one waiter below the bound, so it must be admitted and "
                + "park on the full gate.");

            activation = LeafActivationHarness.ActivateAsync(subject, CancellationToken.None);
            Assert.That(AwaitAdmissionVerdict(subject, activation),
                Is.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.RefusedAdmission),
                "a non-exempt competitor parked on the gate is a real waiter, so it takes the last slot below "
                + "the bound and the next non-exempt caller must be refused. Admission here means the bound "
                + "was not live, and then the exempt-caller test's admission proves nothing.");

            var refusal = Assert.ThrowsAsync<LatticeSaturatedException>(
                async () => await activation.WaitAsync(ExemptArrivalSettle));
            Assert.That(refusal!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));

            ReleaseReplayPermits(gate, ref held);
            Assert.DoesNotThrowAsync(
                async () => await queued.WaitAsync(ExemptArrivalSettle),
                "the admitted competitor must finish once permits are released.");
        }
        finally
        {
            ReleaseReplayPermits(gate, ref held);
            await DrainQuietlyAsync(queued);
            await DrainQuietlyAsync(activation);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    /// <summary>
    /// Takes every permit of the sized gate, so that an admitted activation parks
    /// on it and a starvation drive, which never queues, is turned away.
    /// </summary>
    private static int HoldEveryReplayPermit(SemaphoreSlim gate)
    {
        Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.EqualTo(ExemptArrivalCeiling),
            "precondition: the first activation must have sized the gate to the pinned ceiling.");

        var held = 0;
        while (gate.Wait(0))
            held++;

        Assert.That(held, Is.EqualTo(ExemptArrivalCeiling),
            "precondition: every permit must be free before the scenario starts, or something else "
            + "holds permits this fixture cannot account for.");
        return held;
    }

    private static void ReleaseReplayPermits(SemaphoreSlim gate, ref int held)
    {
        if (held > 0)
            gate.Release(held);
        held = 0;
    }

    /// <summary>
    /// Puts the admitted-waiter count one below the interactive bound and marks
    /// the queue as failing to drain, so the refusal predicate can fire and one
    /// more counted waiter tips the next interactive caller over the bound.
    /// </summary>
    /// <returns>The seeded waiter count.</returns>
    private static int SeedOneBelowInteractiveBound()
    {
        var options = new LatticeOptions();
        var bound = ExemptArrivalCeiling * options.WalReplayPermitQueueDepthPerPermit;
        Assert.That(bound, Is.GreaterThan(1),
            "precondition: the depth bound must be enabled and wide enough to seed one below it.");

        var seeded = bound - 1;
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ExemptArrivalCeiling, seeded);
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            options.WalReplayPermitMaxQueueWait, sinceLastProgress: TimeSpan.Zero);

        Assert.That(BPlusLeafGrain.IsReplayPermitQueueNotDraining(options.WalReplayPermitMaxQueueWait), Is.True,
            "precondition: the drain half of the refusal predicate must hold, or depth alone never refuses "
            + "and the bound is not live.");
        return seeded;
    }

    /// <summary>
    /// Waits until the activation's replay has been admitted to the queue or
    /// refused, and returns the phase it reached.
    /// </summary>
    private static BPlusLeafGrain.ReplayAdmissionPhase AwaitAdmissionVerdict(BPlusLeafGrain grain, Task activation)
    {
        SpinWait.SpinUntil(
            () => activation.IsCompleted
                || grain.ReplayAdmissionPhaseForTest is BPlusLeafGrain.ReplayAdmissionPhase.QueuedForPermit
                    or BPlusLeafGrain.ReplayAdmissionPhase.RefusedAdmission,
            ExemptArrivalSettle);
        return grain.ReplayAdmissionPhaseForTest;
    }

    private static async Task DrainQuietlyAsync(Task? task)
    {
        if (task is null)
            return;
        try
        {
            await task.WaitAsync(ExemptArrivalSettle);
        }
        catch
        {
            // Teardown only: the outcome was already asserted, or the test has failed.
        }
    }
}
