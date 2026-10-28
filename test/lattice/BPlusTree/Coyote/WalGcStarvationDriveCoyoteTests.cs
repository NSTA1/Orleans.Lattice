using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>Exercises the actual shared replay gate, not a second admission policy.</summary>
[TestFixture]
[Category("Coyote")]
[NonParallelizable]
public sealed class WalGcStarvationDriveCoyoteTests
{
    [TestCase(2, 0)]
    [TestCase(6, 0)]
    [TestCase(6, 2)]
    [TestCase(6, 5)]
    public void Sweep_timer_and_cold_returns_preserve_reservations_and_conditional_headroom(int ceiling, int withheld)
        => CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new ArbitrationModel(ceiling, withheld, false));

    [Test]
    public void Calling_the_timer_as_a_sweep_is_detected_by_the_reserved_headroom_assertion()
    {
        var result = CoyoteModelHarness.Explore(
            new ArbitrationModel(6, 0, true), seed: CoyoteModelHarness.GuardSeed);
        Assert.That(result.BugsFound, Is.GreaterThan(0));
        Assert.That(result.BugReports.Any(report => report.Contains("[ShareRespected]", StringComparison.Ordinal)),
            Is.True, "the guard must reach reserved headroom, not an unrelated failure");
    }

    private sealed class ArbitrationModel(int ceiling, int withheld, bool timerUsesSweepOrigin) : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            const BPlusLeafGrain.StarvationDriveOrigin Sweep = BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep;
            const BPlusLeafGrain.StarvationDriveOrigin Timer = BPlusLeafGrain.StarvationDriveOrigin.CoverageLagTimer;
            var circulating = Math.Max(1, ceiling - withheld);
            var share = Math.Max(1, circulating / 2);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, queued: 0);
            BPlusLeafGrain.SeedWithheldReplayPermitsForTest(withheld);
            using var gate = new SemaphoreSlim(circulating, ceiling);
            var held = 0;
            var cold = 0;
            try
            {
                for (var step = 0; step < 12; step++)
                {
                    var choice = runtime.RandomInteger(4);
                    if (choice == 0 && held > 0)
                    {
                        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
                        held--;
                    }
                    else if (choice == 1 && gate.Wait(0))
                    {
                        cold++;
                    }
                    else if (choice == 2 && cold > 0)
                    {
                        gate.Release();
                        cold--;
                    }
                    else
                    {
                        var timer = runtime.RandomBoolean();
                        var before = gate.CurrentCount;
                        var admitted = BPlusLeafGrain.TryAcquireStarvationReplayPermit(
                            gate, timer && !timerUsesSweepOrigin ? Timer : Sweep);
                        if (admitted)
                        {
                            Specification.Assert(before > 0, "[PhysicalCapacity] admission needs an immediate permit");
                            Specification.Assert(held < (timer && share > 1 ? share - 1 : share),
                                "[ShareRespected] timer cannot consume reserved sweep headroom, regardless of holder origins");
                            held++;
                        }
                        else
                        {
                            Specification.Assert(gate.CurrentCount == before,
                                "[RefusalConservation] refusal retains neither a physical permit nor a reservation");
                        }
                    }

                    Specification.Assert(gate.CurrentCount + cold + held == circulating,
                        "[PermitConservation] ordinary owners are physical holders, not GC reservations");
                }

                while (held > 0)
                {
                    BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
                    held--;
                }
                if (cold > 0)
                    gate.Release(cold);
                cold = 0;

                // Finite competing load has ended. A refused single-slot sweep
                // now owns conditional headroom; no scheduler fairness is inferred.
                Specification.Assert(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep),
                    "[FloorHolderEventuallyDriven] a real sweep opportunity with returned capacity must admit");
                held++;
                if (share == 1)
                {
                    Specification.Assert(!BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep),
                        "[SingleSlotHold] establish a real refused sweep");
                    BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
                    held--;
                    Specification.Assert(!BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer),
                        "[SingleSlotHold] timers yield while the refusal is outstanding");
                    Specification.Assert(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep),
                        "[SingleSlotHold] the sweep retry takes the released slot");
                    held++;
                }
            }
            finally
            {
                while (held-- > 0)
                    BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
                if (cold > 0)
                    gate.Release(cold);
                BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            }
        }
    }
}
