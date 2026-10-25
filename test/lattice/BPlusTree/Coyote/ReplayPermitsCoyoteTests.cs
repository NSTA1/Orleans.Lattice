using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>Explores production memory-ledger and priority-admission decisions.</summary>
[TestFixture]
[Category("Coyote")]
[NonParallelizable]
public sealed class ReplayPermitsCoyoteTests
{
    [Test]
    public void Memory_returns_and_priority_observations_preserve_the_gate_contract()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new PermitModel());
    }

    private sealed class PermitModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            const int ceiling = 4;
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, 0);
            using var gate = new SemaphoreSlim(ceiling, ceiling);
            var holders = 0;
            try
            {
                for (var step = 0; step < 48; step++)
                {
                    if (runtime.RandomBoolean() && gate.Wait(0))
                    {
                        holders++;
                    }
                    else if (holders > 0)
                    {
                        var before = BPlusLeafGrain.WithheldReplayPermitsForTest;
                        var pressure = runtime.RandomBoolean();
                        var withheld = pressure && BPlusLeafGrain.TryWithholdReplayPermitOnPressure(
                            LatticeMetrics.PermitAdaptationTriggerFault);
                        Specification.Assert(withheld == (pressure && before < ceiling - 1),
                            "[MemoryWithholding/FloorOne] subtract real capacity, but never the last permit");
                        if (!withheld)
                            gate.Release();
                        holders--;
                    }

                    if (runtime.RandomBoolean())
                    {
                        var before = BPlusLeafGrain.WithheldReplayPermitsForTest;
                        var restored = BPlusLeafGrain.TryRestoreWithheldReplayPermit();
                        Specification.Assert(restored == (before > 0),
                            "[RestoreOwnership] restore exactly an owned memory permit");
                        if (restored)
                            gate.Release();
                    }

                    var memory = BPlusLeafGrain.WithheldReplayPermitsForTest;
                    Specification.Assert(memory < ceiling && gate.CurrentCount + holders + memory == ceiling,
                        "[PermitConservation] physical free, held and withheld permits account for the ceiling");

                    var depth = 1 + (int)runtime.RandomInteger(4);
                    var queued = (int)runtime.RandomInteger(ceiling * depth + 2);
                    BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, queued);
                    var circulating = ceiling - memory;
                    var interactive = circulating * depth;
                    var bulk = Math.Max(1, interactive - circulating);
                    var bulkAdmitted = BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                        depth, LatticeReplayAdmissionClass.Bulk, out var bulkObserved, out var bulkBound);
                    var interactiveAdmitted = BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                        depth, LatticeReplayAdmissionClass.Interactive, out var interactiveObserved, out var interactiveBound);
                    Specification.Assert(bulkObserved == queued && interactiveObserved == queued
                        && bulkBound == bulk && interactiveBound == interactive,
                        "[ClassPriority] observations use circulating capacity and preserve the floored bulk margin");
                    Specification.Assert(bulkAdmitted == (queued < bulk) && interactiveAdmitted == (queued < interactive),
                        "[QueueBounded] classify the observed depth at the actual class boundary");
                }
            }
            finally
            {
                BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            }
        }
    }
}
