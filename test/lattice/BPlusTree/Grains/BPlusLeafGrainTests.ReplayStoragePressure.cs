using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    private static void RecordStoragePressureWindow(int reads, int retries)
    {
        for (var i = 0; i < reads; i++)
            ReplayStoragePressure.RecordSliceRead(0);
        for (var i = 0; i < retries; i++)
            ReplayStoragePressure.NoteStorageRetry(429);
    }

    [Test]
    [NonParallelizable]
    public async Task Storage_throttling_walks_the_gate_down_one_physical_permit_per_release_and_recovery_walks_it_back()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        ReplayStoragePressure.ResetForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync(permitCeiling: 8);
            BPlusLeafGrain.SeedReplayStoragePressureForTest(100);
            Assert.That(BPlusLeafGrain.AdaptReplayStoragePressure(gate, 0), Is.False, "opens the first window");

            RecordStoragePressureWindow(reads: 100, retries: 100);
            var withheld = 0;
            for (var release = 0; release < 8; release++)
            {
                Assert.That(gate.Wait(0), Is.True);
                if (BPlusLeafGrain.AdaptReplayStoragePressure(gate, 100 + release))
                    withheld++;
                else
                    gate.Release();
            }

            Assert.That(withheld, Is.EqualTo(2), "a quarter of eight, one permit per release");
            Assert.That(BPlusLeafGrain.StorageWithheldReplayPermitsForTest, Is.EqualTo(2));
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(2));
            Assert.That(gate.CurrentCount, Is.EqualTo(6), "the ledger and the gate agree");

            Assert.That(BPlusLeafGrain.TryRestoreWithheldReplayPermit(), Is.False,
                "memory relief cannot hand back a permit storage pressure withheld");

            for (var window = 2; window <= 8; window++)
            {
                RecordStoragePressureWindow(reads: 20, retries: 0);
                Assert.That(gate.Wait(0), Is.True);
                Assert.That(BPlusLeafGrain.AdaptReplayStoragePressure(gate, window * 100), Is.False);
                gate.Release();
            }

            Assert.That(BPlusLeafGrain.StorageWithheldReplayPermitsForTest, Is.EqualTo(0),
                "two recovery steps after six clean windows (the first also spends the cooldown)");
            Assert.That(gate.CurrentCount, Is.EqualTo(8), "and never past the ceiling");
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            ReplayStoragePressure.ResetForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task After_a_quiet_spell_the_next_activation_finds_the_full_ceiling_restored()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        ReplayStoragePressure.ResetForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync(permitCeiling: 8);
            BPlusLeafGrain.SeedReplayStoragePressureForTest(100);
            BPlusLeafGrain.AdaptReplayStoragePressure(gate, 0);
            RecordStoragePressureWindow(reads: 100, retries: 100);
            for (var release = 0; release < 2; release++)
            {
                Assert.That(gate.Wait(0), Is.True);
                Assert.That(BPlusLeafGrain.AdaptReplayStoragePressure(gate, 100 + release), Is.True);
            }

            Assert.That(gate.CurrentCount, Is.EqualTo(6));

            var triggers = new List<string?>();
            using var counter = MeterListening.StartForInstrument(LatticeMetrics.WalReplayPermitAdaptations,
                l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                {
                    if (value == 0)
                        return;
                    foreach (var tag in tags)
                        if (tag.Key == LatticeMetrics.TagTrigger)
                            lock (triggers)
                                triggers.Add(tag.Value as string);
                }));

            BPlusLeafGrain.RestoreReplayStorageWhenIdle(gate, 100 + (ReplayStoragePressureGovernor.IdleWindowsToReset * 100));

            Assert.That(gate.CurrentCount, Is.EqualTo(8), "no permit is released while replay is idle, so acquisition is where it returns");
            Assert.That(BPlusLeafGrain.StorageWithheldReplayPermitsForTest, Is.Zero);
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.Zero);
            Assert.That(triggers, Is.EqualTo(new[] { "storage_idle" }));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            ReplayStoragePressure.ResetForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_latency_step_is_tagged_replay_latency_because_it_includes_in_process_queueing()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        ReplayStoragePressure.ResetForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync(permitCeiling: 4);
            BPlusLeafGrain.SeedReplayStoragePressureForTest(100);
            BPlusLeafGrain.AdaptReplayStoragePressure(gate, 0);

            var triggers = new List<string?>();
            using var counter = MeterListening.StartForInstrument(LatticeMetrics.WalReplayPermitAdaptations,
                l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                {
                    if (value == 0)
                        return;
                    foreach (var tag in tags)
                        if (tag.Key == LatticeMetrics.TagTrigger)
                            lock (triggers)
                                triggers.Add(tag.Value as string);
                }));

            var fast = 4 * System.Diagnostics.Stopwatch.Frequency / 1000;
            for (var i = 0; i < 20; i++)
                ReplayStoragePressure.RecordSliceRead(fast);
            Assert.That(gate.Wait(0), Is.True);
            Assert.That(BPlusLeafGrain.AdaptReplayStoragePressure(gate, 100), Is.False, "seeds the baseline");
            gate.Release();

            for (var i = 0; i < 20; i++)
                ReplayStoragePressure.RecordSliceRead(fast * 4);
            Assert.That(gate.Wait(0), Is.True);
            Assert.That(BPlusLeafGrain.AdaptReplayStoragePressure(gate, 200), Is.True);

            Assert.That(triggers, Is.EqualTo(new[] { "replay_latency" }));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            ReplayStoragePressure.ResetForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Storage_pressure_publishes_the_walked_ceiling_and_a_reason_for_each_step()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        ReplayStoragePressure.ResetForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync(permitCeiling: 4);
            BPlusLeafGrain.SeedReplayStoragePressureForTest(100);
            BPlusLeafGrain.AdaptReplayStoragePressure(gate, 0);

            var triggers = new List<(string Outcome, string? Trigger)>();
            using var counter = MeterListening.StartForInstrument(LatticeMetrics.WalReplayPermitAdaptations,
                l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                {
                    if (value == 0)
                        return;
                    string? outcome = null, trigger = null;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == LatticeMetrics.TagOutcome) outcome = tag.Value as string;
                        if (tag.Key == LatticeMetrics.TagTrigger) trigger = tag.Value as string;
                    }

                    lock (triggers)
                        triggers.Add((outcome!, trigger));
                }));

            RecordStoragePressureWindow(reads: 100, retries: 100);
            Assert.That(gate.Wait(0), Is.True);
            Assert.That(BPlusLeafGrain.AdaptReplayStoragePressure(gate, 100), Is.True);

            var effective = new List<int>();
            using (var gauge = MeterListening.StartForMeter(LatticeMetrics.Meter,
                       new[] { LatticeMetrics.WalReplayPermitsEffectiveName },
                       l => l.SetMeasurementEventCallback<int>((_, value, _, _) => effective.Add(value))))
            {
                gauge.RecordObservableInstruments();
            }

            Assert.That(effective, Is.EqualTo(new[] { 3 }), "four sized, one withheld by storage pressure");
            Assert.That(triggers, Is.EqualTo(new[] { ("withheld", (string?)"storage_throttle") }));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            ReplayStoragePressure.ResetForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_silo_left_at_the_defaults_on_a_remote_wal_store_replays_32_leaves_at_once_with_the_controller_on()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var remote = Substitute.For<IWalStorageProvider>();
            remote.IsRemote.Returns(true);
            var gate = await QuiescentReplayGateAsync(controller: null, walStorageProvider: remote);

            Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.EqualTo(32));
            Assert.That(gate.CurrentCount, Is.EqualTo(32));
            Assert.That(BPlusLeafGrain.ReplayServiceRateControllerEnabledForTest, Is.True);
            Assert.That(BPlusLeafGrain.ReplayStoragePressureGovernorEnabledForTest, Is.True);
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [TestCase(1)]
    [TestCase(6)]
    [TestCase(64)]
    [NonParallelizable]
    public async Task An_explicit_replay_ceiling_wins_over_the_default(int configured)
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            await QuiescentReplayGateAsync(permitCeiling: configured);
            Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.EqualTo(configured));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    public void Zero_restores_the_cpu_derived_replay_ceiling()
    {
        Assert.That(BPlusLeafGrain.ResolveGateSizing(0, 16, 4).Ceiling, Is.EqualTo(4));
        Assert.That(BPlusLeafGrain.ResolveGateSizing(0, 6, null).Ceiling, Is.EqualTo(6));
        Assert.That(BPlusLeafGrain.ResolveGateSizing(LatticeOptions.DefaultWalMaterialiserMaxConcurrentReplays, 4, 4, walStoreIsRemote: true).Ceiling,
            Is.EqualTo(32), "the default on a remote store does not scale with CPU");
    }

    [Test]
    [NonParallelizable]
    public async Task The_storage_pressure_governor_is_off_when_the_service_rate_controller_is_off()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        ReplayStoragePressure.ResetForTest();
        try
        {
            var gate = await QuiescentReplayGateAsync(permitCeiling: 4, controller: false);
            Assert.That(BPlusLeafGrain.ReplayStoragePressureGovernorEnabledForTest, Is.False);

            BPlusLeafGrain.AdaptReplayStoragePressure(gate, 0);
            RecordStoragePressureWindow(reads: 100, retries: 100);
            Assert.That(gate.Wait(0), Is.True);
            Assert.That(BPlusLeafGrain.AdaptReplayStoragePressure(gate, 100), Is.False,
                "a fixed ceiling is the opt-out for stores with nothing to throttle");
            gate.Release();
            Assert.That(gate.CurrentCount, Is.EqualTo(4));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            ReplayStoragePressure.ResetForTest();
        }
    }
}
