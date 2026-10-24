using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing.Coyote;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TagIndexReconcileGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    [Category("Coyote")]
    public void Cooperative_reminder_demand_and_subject_changes_preserve_sweep_progress(bool probeOnly)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new TagScheduleModel(probeOnly));
    }

    private sealed class TagScheduleModel(bool probeOnly) : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var harness = CreateSweepGrain(
                new LatticeTagIndexReconciliationOptions { ChunkSize = 1, ProbeOnly = probeOnly },
                configureSubjectTrees: factory =>
                {
                    StubSubjectTree(factory, "tree-a", [1]);
                    StubSubjectTree(factory, "tree-b", [1]);
                });
            foreach (var tree in new[] { "tree-a", "tree-b" })
                harness.IndexTree.Data["\0covered\0" + tree] = [1];
            var state = harness.State.State;
            Complete(harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus()));
            var demandSent = false;
            var subjectChanged = false;
            for (var opportunity = 0; opportunity < 8; opportunity++)
            {
                if (runtime.RandomBoolean())
                {
                    var cursor = state.NextProbeIndex;
                    var repair = state.NextRepairIndex;
                    Complete(harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus()));
                    Specification.Assert(state.NextProbeIndex == cursor && state.NextRepairIndex == repair,
                        "[SingleSweep] reminder arrival cannot rewind an active sweep");
                }
                if (!subjectChanged && runtime.RandomBoolean())
                {
                    subjectChanged = true;
                    StubSubjectTree(harness.GrainFactory, "tree-b", [2]);
                    harness.IndexTree.Data["tag\0tree-b\0gone"] = [1];
                    harness.IndexTree.Data["\0k\0tree-b\0gone\0tag"] = [1];
                }
                if (!demandSent && runtime.RandomBoolean())
                {
                    var wasActive = state.InProgress;
                    var sawTerminal = !wasActive;
                    harness.State.OnWriteState = snapshot =>
                    {
                        if (!snapshot.InProgress) sawTerminal = true;
                        Specification.Assert(sawTerminal || snapshot.Phase != TagIndexReconcilePhase.Probe
                            || snapshot.NextProbeIndex != 0,
                            "[SingleSweep] demand must finish the old sweep before initializing its own");
                    };
                    Complete(harness.Grain.RunSweepAsync());
                    harness.State.OnWriteState = null;
                    Specification.Assert(!state.InProgress, "[TriggerNotLost] synchronous demand must finish");
                    demandSent = true;
                }
                else
                {
                    var phase = state.Phase;
                    var cursor = state.NextProbeIndex;
                    var baselines = state.Baselines.ToDictionary();
                    Complete(harness.Grain.ProcessNextPhaseAsync());
                    if (phase == TagIndexReconcilePhase.Probe)
                    {
                        Specification.Assert(!state.InProgress || state.NextProbeIndex - cursor <= 1,
                            "[ChunkBounded] probe processes at most one tree per phase callback");
                        Specification.Assert(baselines.Count == state.Baselines.Count
                            && baselines.All(pair => state.Baselines[pair.Key].SequenceEqual(pair.Value)),
                            "[BaselineOnlyAfterRepair] probe must not certify repair");
                    }
                }
                Specification.Assert(!probeOnly || state.Baselines.Count == 0,
                    "[ProbeOnlyNeverRepairs] audit never banks a repair baseline");
            }

            // Finite environment tail: deliver the queued demand and enough real
            // phase callbacks, then another reminder after the final mutation.
            Complete(harness.Grain.RunSweepAsync());
            Complete(harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus()));
            for (var turn = 0; turn < 6 && state.InProgress; turn++)
                Complete(harness.Grain.ProcessNextPhaseAsync());
            Specification.Assert(!state.InProgress, "[SweepTerminates] finite covered set must drain");
            Specification.Assert(probeOnly || !harness.IndexTree.Data.ContainsKey("tag\0tree-b\0gone"),
                "[DivergenceEventuallyRepaired] final subject version must repair on the next sweep");
        }

        private static void Complete(Task task)
        {
            Specification.Assert(task.IsCompleted, "cooperative collaborator must complete without blocking Coyote");
            task.GetAwaiter().GetResult();
        }
    }
}
