using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
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
