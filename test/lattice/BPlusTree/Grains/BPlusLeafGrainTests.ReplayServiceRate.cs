using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task Old_starvation_hold_blocks_grading_after_new_completions_and_interrupted_probe_rolls_back()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = gate.CurrentCount;
            BPlusLeafGrain.SeedReplayServiceRateForTest(0, 100);
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(
                gate, BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep), Is.True);
            var oldDrive = BPlusLeafGrain.BeginReplayServiceRateHold();
            while (gate.Wait(0)) { }
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 1);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 100);
            BPlusLeafGrain.SeedReplayServiceRateForTest(100, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 200), Is.True);

            var newHold = BPlusLeafGrain.BeginReplayServiceRateHold();
            using (SimulatedHeap(100, 1000))
                BPlusLeafGrain.FinishReplayPermit(gate, false, true, 1, "probe", "probe", newHold);
            Assert.That(gate.Wait(0), Is.True);
            BPlusLeafGrain.SeedReplayServiceRateForTest(200, 100);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 300);
            BPlusLeafGrain.SeedReplayServiceRateForTest(300, 100);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 400);

            // If new completions incorrectly drain the old cohort, these windows
            // accept the reduction and loss of demand would no longer undo it.
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 0);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 500);
            Assert.That(BPlusLeafGrain.ServiceWithheldReplayPermitsForTest, Is.Zero);
            Assert.That(gate.CurrentCount, Is.EqualTo(1));
            BPlusLeafGrain.CompleteReplayServiceRateHold(oldDrive);
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
            Assert.That(gate.CurrentCount, Is.EqualTo(2));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    [NonParallelizable]
    public async Task Throwing_memory_diagnostics_cannot_strand_or_duplicate_a_replay_permit(bool enabled)
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = gate.CurrentCount;
            if (enabled)
                BPlusLeafGrain.SeedReplayServiceRateForTest(0, 100);
            using var listener = MeterListening.StartForInstrument(LatticeMetrics.WalReplayPermitAdaptations,
                l => l.SetMeasurementEventCallback<long>((_, _, _, _) =>
                    throw new InvalidOperationException("Broken memory diagnostics consumer.")));
            using (SimulatedHeap(900, 1000))
                await ActivateWithCleanReplayAsync();
            Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - 1));
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1));
            using (SimulatedHeap(100, 1000))
                await ActivateWithCleanReplayAsync();
            Assert.That(gate.CurrentCount, Is.EqualTo(ceiling));
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero);
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Completed_release_can_be_reacquired_before_hold_metrics_without_blocking_probe_grading()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = gate.CurrentCount;
            while (gate.Wait(0)) { }
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 1);
            BPlusLeafGrain.SeedReplayServiceRateForTest(0, 100);
            var finishedHold = BPlusLeafGrain.BeginReplayServiceRateHold();
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 100);
            var lateHold = -1L;
            using var listener = MeterListening.StartForInstrument(LatticeMetrics.WalReplayPermitHold,
                l => l.SetMeasurementEventCallback<double>((_, _, _, _) =>
                {
                    Assert.That(gate.Wait(0), Is.True);
                    lateHold = BPlusLeafGrain.BeginReplayServiceRateHold();
                }));
            using (SimulatedHeap(100, 1000))
                BPlusLeafGrain.FinishReplayPermit(gate, false, true, 1, "probe", "probe", finishedHold);
            listener.Dispose();
            Assert.That(lateHold, Is.GreaterThanOrEqualTo(0));
            BPlusLeafGrain.SeedReplayServiceRateForTest(100, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 200), Is.True);
            BPlusLeafGrain.CompleteReplayServiceRateHold(lateHold);
            BPlusLeafGrain.SeedReplayServiceRateForTest(200, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 300), Is.False);
            BPlusLeafGrain.SeedReplayServiceRateForTest(300, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 400), Is.False);
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 0);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 500), Is.False);
            Assert.That(BPlusLeafGrain.ServiceWithheldReplayPermitsForTest, Is.EqualTo(1),
                "the probe was graded and accepted, not permanently waiting on the already finished hold");
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Acquired_but_not_enrolled_slot_and_starvation_reacquisition_cannot_corrupt_the_old_cohort()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = gate.CurrentCount;
            BPlusLeafGrain.SeedReplayServiceRateForTest(0, 100);
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(
                gate, BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep), Is.True);
            var oldDrive = BPlusLeafGrain.BeginReplayServiceRateHold();
            while (gate.Wait(0)) { }
            // The other slots have been acquired, but have not started replay
            // or enrolled. They cannot be counted as old work.
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 1);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 100);
            BPlusLeafGrain.SeedReplayServiceRateForTest(100, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 200), Is.True);
            var lateAdmission = BPlusLeafGrain.BeginReplayServiceRateHold();
            BPlusLeafGrain.CompleteReplayServiceRateHold(oldDrive);
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(
                gate, BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep), Is.True);
            var newDrive = BPlusLeafGrain.BeginReplayServiceRateHold();
            BPlusLeafGrain.SeedReplayServiceRateForTest(200, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 300), Is.False);
            BPlusLeafGrain.SeedReplayServiceRateForTest(300, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 400), Is.False);
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 0);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 500);
            Assert.That(BPlusLeafGrain.ServiceWithheldReplayPermitsForTest, Is.EqualTo(1),
                "late enrollment and the new drive cannot become phantom old holds");
            BPlusLeafGrain.CompleteReplayServiceRateHold(lateAdmission);
            BPlusLeafGrain.CompleteReplayServiceRateHold(newDrive);
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    [NonParallelizable]
    public async Task Activation_latches_the_service_controller_opt_in_on_first_gate_sizing(bool enabled)
    {
        Assert.That(new LatticeOptions().WalReplayServiceRateControllerEnabled, Is.False);
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                null, 0, 0, configureOptions: o =>
                {
                    o.WalMaterialiserMaxConcurrentReplays = 6;
                    o.WalReplayServiceRateControllerEnabled = enabled;
                });
            state.State.TreeId = UniqueReplayPermitTree();
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            var (laterGrain, laterState, _, _) = CreateGrainWithSnapshotAndCoordinator(
                null, 0, 0, configureOptions: o =>
                {
                    o.WalMaterialiserMaxConcurrentReplays = 2;
                    o.WalReplayServiceRateControllerEnabled = !enabled;
                });
            laterState.State.TreeId = UniqueReplayPermitTree();
            await LeafActivationHarness.ActivateAsync(laterGrain, CancellationToken.None);
            Assert.That(BPlusLeafGrain.ReplayServiceRateControllerEnabledForTest, Is.EqualTo(enabled));
            Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.EqualTo(6));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    [NonParallelizable]
    public async Task Service_adaptation_metrics_preserve_attribution_and_physical_disposition_when_listener_throws(bool throws)
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
            while (gate.Wait(0)) { }
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 1);
            BPlusLeafGrain.SeedReplayServiceRateForTest(0, 100);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 100);
            BPlusLeafGrain.SeedReplayServiceRateForTest(100, 100);
            var records = new System.Collections.Concurrent.ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();
            using var listener = MeterListening.StartForInstrument(LatticeMetrics.WalReplayPermitAdaptations, l =>
                l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                {
                    records.Add((value, tags.ToArray()));
                    if (throws)
                        throw new InvalidOperationException("Broken diagnostics consumer.");
                }));
            Assert.That(() => BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 200), Is.True);
            Assert.That(BPlusLeafGrain.ServiceWithheldReplayPermitsForTest, Is.EqualTo(1));
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 0);
            using (SimulatedHeap(100, 1000))
                Assert.DoesNotThrow(() => BPlusLeafGrain.FinishReplayPermit(
                    gate, false, true, 1, "probe", "probe"));
            Assert.That(gate.CurrentCount, Is.EqualTo(2),
                "the current permit and the interrupted probe's permit must both return");
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero);
            var adaptations = ProjectPermitAdaptations(records);
            Assert.That(adaptations, Has.Length.EqualTo(2));
            Assert.That(adaptations.All(a => Equals(a.Trigger, "service_time") && a.Value == 1), Is.True);
            Assert.That(adaptations.Select(a => a.Outcome), Is.EquivalentTo(new[] { "withheld", "restored" }));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
        Assert.That(BPlusLeafGrain.ReplayServiceRateControllerEnabledForTest, Is.False);
        Assert.That(BPlusLeafGrain.ServiceWithheldReplayPermitsForTest, Is.Zero);
    }

    [Test]
    [NonParallelizable]
    public async Task Clean_activation_memory_restore_cannot_return_a_service_owned_permit()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
            Assert.That(ceiling, Is.GreaterThan(2));
            var held = gate.CurrentCount;
            while (gate.Wait(0)) { }
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 1);
            BPlusLeafGrain.SeedReplayServiceRateForTest(0, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 100), Is.False);
            BPlusLeafGrain.SeedReplayServiceRateForTest(100, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 200), Is.True);
            for (var i = 1; i < held; i++)
            {
                gate.Release();
                BPlusLeafGrain.RecordReplayPermitHold(1, "probe", "probe");
            }
            BPlusLeafGrain.RecordReplayPermitHold(1, "probe", "probe");
            // Establish that the reduction was accepted before memory changes;
            // an interrupted, ungraded probe is correctly rolled back later.
            while (gate.Wait(0)) { }
            BPlusLeafGrain.SeedReplayServiceRateForTest(200, 100);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 300);
            BPlusLeafGrain.SeedReplayServiceRateForTest(300, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 400), Is.False);
            for (var i = 0; i < ceiling - 1; i++)
                gate.Release();

            Assert.That(BPlusLeafGrain.TryWithholdReplayPermitOnPressure(LatticeMetrics.PermitAdaptationTriggerOccupancy), Is.True);
            Assert.That(gate.Wait(0), Is.True);
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 0);
            using (SimulatedHeap(100, 1000))
            {
                await ActivateWithCleanReplayAsync();
                await ActivateWithCleanReplayAsync();
            }

            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.ServiceWithheldReplayPermitsForTest, Is.EqualTo(1));
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1));
                Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - 1));
                Assert.That(BPlusLeafGrain.TryRestoreWithheldReplayPermit(), Is.False);
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Memory_and_service_withholding_share_one_floor_under_concurrent_pressure()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
            while (gate.Wait(0)) { }
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 1);
            BPlusLeafGrain.SeedReplayServiceRateForTest(0, 100);
            BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 100);
            BPlusLeafGrain.SeedReplayServiceRateForTest(100, 100);
            Assert.That(BPlusLeafGrain.AdaptReplayServiceRate(gate, true, 200), Is.True);
            for (var i = 1; i < ceiling; i++)
                gate.Release();

            Parallel.For(0, ceiling * 4, _ =>
            {
                if (BPlusLeafGrain.TryWithholdReplayPermitOnPressure(LatticeMetrics.PermitAdaptationTriggerFault))
                    Assert.That(gate.Wait(0), Is.True);
            });

            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(ceiling - 1));
                Assert.That(gate.CurrentCount, Is.EqualTo(1));
                Assert.That(BPlusLeafGrain.SweepStarvationShare, Is.EqualTo(1));
            });
            while (BPlusLeafGrain.TryRestoreWithheldReplayPermit())
                gate.Release();
            Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - 1));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Fresh_live_heap_burst_withholds_even_after_a_low_simulated_completed_gc()
    {
        // A previous completed GC is not a restore license. The release seam
        // must still observe the current allocation burst, not reuse that low read.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync();
            var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
            using (SimulatedHeap(100, 1000))
                await ActivateWithCleanReplayAsync();
            using (SimulatedHeap(900, 1000))
                await ActivateWithCleanReplayAsync();

            Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - 1));
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
